namespace GeoResolver;

public sealed class GeoLocationService
{
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly ILogger<GeoLocationService> _logger;

    private const int Wgs84Srid = 4326;

    public GeoLocationService(NpgsqlDataSource npgsqlDataSource, ILogger<GeoLocationService> logger)
    {
        ArgumentNullException.ThrowIfNull(npgsqlDataSource);
        ArgumentNullException.ThrowIfNull(logger);
        
        _npgsqlDataSource = npgsqlDataSource;
        _logger = logger;
    }

    /// <summary>
    /// Resolves geo-location information for given coordinates
    /// Always returns a response with timezone data (required), country/region/city data is optional
    /// </summary>
    public async Task<GeoLocationResponse> ResolveAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        var countryTask = FindCountryByPointAsync(latitude, longitude, cancellationToken);
        var regionTask = FindRegionByPointAsync(latitude, longitude, cancellationToken);
        var cityTask = FindCityByPointAsync(latitude, longitude, cancellationToken);
        var timezoneTask = GetTimezoneOffsetAsync(latitude, longitude, cancellationToken);

        await Task.WhenAll(countryTask, regionTask, cityTask, timezoneTask);

        var country = await countryTask;
        var region = await regionTask;
        var city = await cityTask;
        var timezone = await timezoneTask;

        // Timezone is always required - use fallback calculation if not found in database
        var timezoneOffset = timezone ?? (0, 0);

        return new GeoLocationResponse
        {
            CountryIsoAlpha2Code = country?.IsoAlpha2Code,
            CountryIsoAlpha3Code = country?.IsoAlpha3Code,
            CountryNameLatin = country?.NameLatin,
            RegionIdentifier = region?.Identifier,
            RegionNameLatin = region?.NameLatin,
            CityIdentifier = city?.Identifier,
            CityNameLatin = city?.NameLatin,
            TimezoneRawOffsetSeconds = timezoneOffset.RawOffset,
            TimezoneDstOffsetSeconds = timezoneOffset.DstOffset
        };
    }

    private static Point CreatePoint(double latitude, double longitude)
    {
        return new Point(longitude, latitude) { SRID = Wgs84Srid };
    }

    private async Task<T?> FindEntityByPointAsync<T>(
        string sql,
        Func<NpgsqlDataReader, T> mapFunc,
        double latitude,
        double longitude,
        CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var point = CreatePoint(latitude, longitude);
        await using var cmd = new NpgsqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("point", point);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return mapFunc(reader);
        }

        return default;
    }

    private async Task<Country?> FindCountryByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        const string sql = @"
            SELECT id, iso_alpha2_code, iso_alpha3_code, name_latin, wikidataid, geometry
            FROM countries
            WHERE ST_Contains(geometry, @point)
            LIMIT 1;";

        return await FindEntityByPointAsync(sql, MapCountryFromReader, latitude, longitude, cancellationToken);
    }

    private async Task<Region?> FindRegionByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        const string sql = @"
            SELECT id, identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, wikidataid, name_local, geometry
            FROM regions
            WHERE ST_Contains(geometry, @point)
            LIMIT 1;";

        return await FindEntityByPointAsync(sql, MapRegionFromReader, latitude, longitude, cancellationToken);
    }

    private async Task<City?> FindCityByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        const string sql = @"
            SELECT id, identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, name_local, geometry
            FROM cities
            WHERE ST_Contains(geometry, @point)
            LIMIT 1;";

        return await FindEntityByPointAsync(sql, MapCityFromReader, latitude, longitude, cancellationToken);
    }

    private static Country MapCountryFromReader(NpgsqlDataReader reader)
    {
        return new Country
        {
            Id = reader.GetInt32(0),
            IsoAlpha2Code = reader.GetString(1),
            IsoAlpha3Code = reader.GetString(2),
            NameLatin = reader.GetString(3),
            WikidataId = reader.GetString(4),
            Geometry = (Geometry)reader.GetValue(5)
        };
    }

    private static Region MapRegionFromReader(NpgsqlDataReader reader)
    {
        return new Region
        {
            Id = reader.GetInt32(0),
            Identifier = reader.GetString(1),
            NameLatin = reader.GetString(2),
            CountryIsoAlpha2Code = reader.GetString(3),
            CountryIsoAlpha3Code = reader.GetString(4),
            WikidataId = reader.GetString(5),
            NameLocal = reader.GetString(6),
            Geometry = (Geometry)reader.GetValue(7)
        };
    }

    private static City MapCityFromReader(NpgsqlDataReader reader)
    {
        return new City
        {
            Id = reader.GetInt32(0),
            Identifier = reader.GetString(1),
            NameLatin = reader.GetString(2),
            CountryIsoAlpha2Code = reader.GetString(3),
            CountryIsoAlpha3Code = reader.GetString(4),
            RegionIdentifier = reader.IsDBNull(5) ? null : reader.GetString(5),
            NameLocal = reader.GetString(6),
            Geometry = (Geometry)reader.GetValue(7)
        };
    }

    private async Task<(int RawOffset, int DstOffset)?> GetTimezoneOffsetAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        const string sql = @"
            SELECT timezone_id
            FROM timezones
            WHERE ST_Contains(geometry, @point)
            LIMIT 1;";

        var timezoneId = await FindEntityByPointAsync(
            sql,
            reader => reader.GetString(0),
            latitude,
            longitude,
            cancellationToken);

        if (timezoneId != null)
        {
            _logger.LogDebug(
                "Found timezone ID '{TimezoneId}' in database for point ({Latitude}, {Longitude})",
                timezoneId, latitude, longitude);
            return CalculateTimezoneOffset(timezoneId, DateTimeOffset.UtcNow);
        }

        // Fallback: approximate timezone based on longitude if no timezone data in database
        // This is a simple approximation: each 15 degrees of longitude ≈ 1 hour
        var approximateOffsetHours = (int)Math.Round(longitude / 15.0);
        var approximateOffsetSeconds = approximateOffsetHours * 3600;
        return (approximateOffsetSeconds, 0);
    }

    private (int RawOffset, int DstOffset) CalculateTimezoneOffset(string timezoneId, DateTimeOffset utcNow)
    {
        try
        {
            // Normalize timezone ID: trim whitespace and handle common variations
            var normalizedId = timezoneId.Trim();
            if (string.IsNullOrWhiteSpace(normalizedId))
            {
                _logger.LogWarning("Empty or null timezone ID provided");
                return (0, 0);
            }

            _logger.LogDebug("Attempting to get timezone info for ID: '{TimezoneId}' (normalized from '{Original}')", 
                normalizedId, timezoneId);
            
            // NodaTime works completely independently of system timezone files
            // It has built-in IANA timezone database, perfect for chiseled Docker images
            DateTimeZone? timeZone;
            try
            {
                timeZone = DateTimeZoneProviders.Tzdb.GetZoneOrNull(normalizedId);
                if (timeZone == null)
                {
                    _logger.LogWarning("Timezone '{TimezoneId}' not found in NodaTime database", normalizedId);
                    return (0, 0);
                }
                
                _logger.LogDebug("Successfully retrieved timezone info for '{TimezoneId}'", normalizedId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting timezone '{TimezoneId}' from NodaTime", normalizedId);
                return (0, 0);
            }

            // Convert DateTimeOffset to NodaTime Instant
            var instant = Instant.FromDateTimeUtc(utcNow.UtcDateTime);
            
            // Get the zone interval (handles DST automatically)
            var zoneInterval = timeZone.GetZoneInterval(instant);
            
            // Calculate offsets
            // WallOffset is the total offset from UTC (includes DST)
            // StandardOffset is the base offset without DST
            var totalOffset = zoneInterval.WallOffset;
            var standardOffset = zoneInterval.StandardOffset;
            var dstOffset = totalOffset - standardOffset;

            // NodaTime Offset uses Seconds property, not TotalSeconds
            var rawOffsetSeconds = standardOffset.Seconds;
            var dstOffsetSeconds = dstOffset.Seconds;

            _logger.LogDebug("Calculated timezone offset for '{TimezoneId}': RawOffset={RawOffset}s, DstOffset={DstOffset}s, Total={Total}s", 
                normalizedId, rawOffsetSeconds, dstOffsetSeconds, totalOffset.Seconds);

            return (rawOffsetSeconds, dstOffsetSeconds);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error calculating timezone offset for '{TimezoneId}'", timezoneId);
            // Fallback: return UTC if timezone cannot be determined
            return (0, 0);
        }
    }
}
