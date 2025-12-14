using System.Globalization;
using System.Linq;
using System.Text.Json;
using GeoResolver.DataUpdater.Models;
using Microsoft.Extensions.Logging;
using Npgsql;
using NetTopologySuite.Geometries;
using NpgsqlTypes;

namespace GeoResolver.DataUpdater.Services;

public sealed class DatabaseWriterService : IDatabaseWriterService
{
    private readonly NpgsqlDataSource _npgsqlDataSource;
    private readonly ILogger<DatabaseWriterService>? _logger;
    private static readonly Dictionary<string, string> Alpha3ToAlpha2Cache = new(StringComparer.OrdinalIgnoreCase);

    public DatabaseWriterService(NpgsqlDataSource npgsqlDataSource, ILogger<DatabaseWriterService>? logger = null)
    {
        _npgsqlDataSource = npgsqlDataSource;
        _logger = logger;
    }

    private static string? Alpha3ToAlpha2(string alpha3Code)
    {
        if (string.IsNullOrWhiteSpace(alpha3Code) || alpha3Code.Length != 3)
            return null;

        // Check cache first
        if (Alpha3ToAlpha2Cache.TryGetValue(alpha3Code, out var cached))
            return cached;

        // Try to get region info using .NET's built-in support
        try
        {
            var regions = CultureInfo.GetCultures(CultureTypes.SpecificCultures)
                .Select(c => new RegionInfo(c.Name))
                .Where(r => r.ThreeLetterISORegionName.Equals(alpha3Code, StringComparison.OrdinalIgnoreCase));
            
            var region = regions.FirstOrDefault();
            if (region != null)
            {
                var alpha2 = region.TwoLetterISORegionName;
                Alpha3ToAlpha2Cache[alpha3Code] = alpha2;
                return alpha2;
            }
        }
        catch
        {
            // If RegionInfo fails, return null
        }

        return null;
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        // All database schema objects (tables, indexes, extensions) are created by SQL scripts
        // This method is kept for interface compatibility but does nothing
        // SQL scripts location:
        // - Docker: docker-entrypoint-initdb.d/*.sql
        // - Manual: scripts/manual-*.sql
        await Task.CompletedTask;
    }

    public async Task<CountryEntity?> FindCountryByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var point = $"POINT({longitude} {latitude})";
        await using var cmd = new NpgsqlCommand(@"
            SELECT id, iso_alpha2_code, name_latin, geometry
            FROM countries
            WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
            LIMIT 1;", connection);

        cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);
        
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new CountryEntity
            {
                Id = reader.GetInt32(0),
                IsoAlpha2Code = reader.GetString(1),
                NameLatin = reader.GetString(2),
                Geometry = (Geometry)reader.GetValue(3)
            };
        }

        return null;
    }

    public async Task<RegionEntity?> FindRegionByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var point = $"POINT({longitude} {latitude})";
        await using var cmd = new NpgsqlCommand(@"
            SELECT id, identifier, name_latin, country_iso_alpha2_code, geometry
            FROM regions
            WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
            LIMIT 1;", connection);

        cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);
        
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new RegionEntity
            {
                Id = reader.GetInt32(0),
                Identifier = reader.GetString(1),
                NameLatin = reader.GetString(2),
                CountryIsoAlpha2Code = reader.GetString(3),
                Geometry = (Geometry)reader.GetValue(4)
            };
        }

        return null;
    }

    public async Task<CityEntity?> FindCityByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var point = $"POINT({longitude} {latitude})";
        await using var cmd = new NpgsqlCommand(@"
            SELECT id, identifier, name_latin, country_iso_alpha2_code, region_identifier, geometry
            FROM cities
            WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
            LIMIT 1;", connection);

        cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);
        
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new CityEntity
            {
                Id = reader.GetInt32(0),
                Identifier = reader.GetString(1),
                NameLatin = reader.GetString(2),
                CountryIsoAlpha2Code = reader.GetString(3),
                RegionIdentifier = reader.IsDBNull(4) ? null : reader.GetString(4),
                Geometry = (Geometry)reader.GetValue(5)
            };
        }

        return null;
    }

    public async Task<(int RawOffset, int DstOffset)?> GetTimezoneOffsetAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var point = $"POINT({longitude} {latitude})";
        await using var cmd = new NpgsqlCommand(@"
            SELECT timezone_id
            FROM timezones
            WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
            LIMIT 1;", connection);

        cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);
        
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            var timezoneId = reader.GetString(0);
            return CalculateTimezoneOffset(timezoneId, DateTimeOffset.UtcNow);
        }

        // Fallback: approximate timezone based on longitude if no timezone data in database
        // This is a simple approximation: each 15 degrees of longitude ≈ 1 hour
        var approximateOffsetHours = (int)Math.Round(longitude / 15.0);
        var approximateOffsetSeconds = approximateOffsetHours * 3600;
        return (approximateOffsetSeconds, 0);
    }

    private static (int RawOffset, int DstOffset) CalculateTimezoneOffset(string timezoneId, DateTimeOffset utcNow)
    {
        try
        {
            var timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById(timezoneId);
            var localTime = TimeZoneInfo.ConvertTimeFromUtc(utcNow.UtcDateTime, timeZoneInfo);
            var baseOffset = timeZoneInfo.BaseUtcOffset;
            var dstOffset = TimeSpan.Zero;

            if (timeZoneInfo.SupportsDaylightSavingTime && timeZoneInfo.IsDaylightSavingTime(localTime))
            {
                dstOffset = timeZoneInfo.GetUtcOffset(localTime) - baseOffset;
            }

            return ((int)baseOffset.TotalSeconds, (int)dstOffset.TotalSeconds);
        }
        catch
        {
            // Fallback: try parsing IANA timezone or return UTC
            return (0, 0);
        }
    }

    public async Task<DateTimeOffset?> GetLastUpdateTimeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(
            "SELECT updated_at FROM last_update WHERE id = 1;", 
            connection);
        
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        if (result == null || result == DBNull.Value)
        {
            return null;
        }

        if (result is DateTime dateTime)
        {
            return new DateTimeOffset(dateTime, TimeSpan.Zero);
        }

        return null;
    }

    public async Task SetLastUpdateTimeAsync(DateTimeOffset updateTime, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand(
            @"INSERT INTO last_update (id, updated_at) 
              VALUES (1, @updateTime)
              ON CONFLICT (id) 
              DO UPDATE SET updated_at = @updateTime;", 
            connection);
        
        cmd.Parameters.AddWithValue("@updateTime", updateTime.UtcDateTime);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task ClearAllDataAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var cmd1 = new NpgsqlCommand("TRUNCATE TABLE countries CASCADE;", connection);
        await cmd1.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd2 = new NpgsqlCommand("TRUNCATE TABLE regions CASCADE;", connection);
        await cmd2.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd3 = new NpgsqlCommand("TRUNCATE TABLE cities CASCADE;", connection);
        await cmd3.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd4 = new NpgsqlCommand("TRUNCATE TABLE timezones CASCADE;", connection);
        await cmd4.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task ImportCountriesAsync(IEnumerable<CountryEntity> countries, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var country in countries)
        {
            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO countries (iso_alpha2_code, name_latin, geometry)
                VALUES (@isoCode, @name, ST_GeomFromWKB(@geometry, 4326))
                ON CONFLICT (iso_alpha2_code) DO UPDATE
                SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;", connection, transaction);

            var wkbWriter = new NetTopologySuite.IO.WKBWriter();
            var wkb = wkbWriter.Write(country.Geometry);

            cmd.Parameters.AddWithValue("isoCode", country.IsoAlpha2Code);
            cmd.Parameters.AddWithValue("name", country.NameLatin);
            cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task ImportCountriesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var jsonDoc = JsonDocument.Parse(geoJsonContent);
        var root = jsonDoc.RootElement;
        var features = root.GetProperty("features");

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        
        int processed = 0;
        int skipped = 0;
        var skippedReasons = new Dictionary<string, int>();

        foreach (var featureElement in features.EnumerateArray())
        {
            var properties = featureElement.GetProperty("properties");
            var geometryElement = featureElement.GetProperty("geometry");

            // Try to get ISO code from various possible fields
            // Different GeoJSON sources use different field names
            string? isoCode = null;
            
            // Try ISO_A2 (Natural Earth format)
            if (properties.TryGetProperty("ISO_A2", out var isoA2) && isoA2.ValueKind == JsonValueKind.String)
            {
                var isoValue = isoA2.GetString();
                // Natural Earth uses "-99" for missing values
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
                    isoCode = isoValue.ToUpperInvariant();
            }
            // Try ISO (some sources use just ISO)
            else if (properties.TryGetProperty("ISO", out var iso) && iso.ValueKind == JsonValueKind.String)
            {
                var isoValue = iso.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue.Length == 2)
                    isoCode = isoValue.ToUpperInvariant();
            }
            // Try iso_a2 (lowercase)
            else if (properties.TryGetProperty("iso_a2", out var isoA2Lower) && isoA2Lower.ValueKind == JsonValueKind.String)
            {
                var isoValue = isoA2Lower.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
                    isoCode = isoValue.ToUpperInvariant();
            }
            // Try iso_a2_eh
            else if (properties.TryGetProperty("iso_a2_eh", out var isoA2Eh) && isoA2Eh.ValueKind == JsonValueKind.String)
            {
                var isoValue = isoA2Eh.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
                    isoCode = isoValue.ToUpperInvariant();
            }
            // Try ISO_A2_EH (uppercase variant)
            else if (properties.TryGetProperty("ISO_A2_EH", out var isoA2EhUpper) && isoA2EhUpper.ValueKind == JsonValueKind.String)
            {
                var isoValue = isoA2EhUpper.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
                    isoCode = isoValue.ToUpperInvariant();
            }
            // Try id field - some sources use 3-letter ISO codes (alpha-3) in id
            // We need to map them to 2-letter codes (alpha-2)
            if (string.IsNullOrWhiteSpace(isoCode) && featureElement.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String)
            {
                var idValue = id.GetString();
                if (!string.IsNullOrWhiteSpace(idValue))
                {
                    // If it's 2 characters, use directly
                    if (idValue.Length == 2)
                    {
                        isoCode = idValue.ToUpperInvariant();
                    }
                    // If it's 3 characters (alpha-3), map to alpha-2 using RegionInfo
                    else if (idValue.Length == 3)
                    {
                        isoCode = Alpha3ToAlpha2(idValue);
                    }
                }
            }

            // Natural Earth uses NAME and NAME_LONG fields
            var nameLatin = "Unknown";
            if (properties.TryGetProperty("NAME", out var name) && name.ValueKind == JsonValueKind.String)
                nameLatin = name.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("NAME_LONG", out var nameLong) && nameLong.ValueKind == JsonValueKind.String)
                nameLatin = nameLong.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("name", out var nameLower) && nameLower.ValueKind == JsonValueKind.String)
                nameLatin = nameLower.GetString() ?? "Unknown";

            if (string.IsNullOrWhiteSpace(isoCode) || isoCode.Length != 2)
            {
                skipped++;
                var reason = "Missing or invalid ISO code";
                skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;
                
                // Log first few skipped features for debugging
                if (skipped <= 10)
                {
                    var allProperties = new System.Text.StringBuilder();
                    foreach (var prop in properties.EnumerateObject())
                    {
                        allProperties.Append($"{prop.Name}={prop.Value}, ");
                    }
                    _logger?.LogDebug("Skipped country (missing/invalid ISO code): {Properties}", allProperties);
                }
                continue;
            }

            var geometryJson = geometryElement.GetRawText();

            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO countries (iso_alpha2_code, name_latin, geometry)
                VALUES (@isoCode, @name, ST_GeomFromGeoJSON(@geometryJson))
                ON CONFLICT (iso_alpha2_code) DO UPDATE
                SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;", connection, transaction);

            cmd.Parameters.AddWithValue("isoCode", isoCode);
            cmd.Parameters.AddWithValue("name", nameLatin);
            cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);
            
            try
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                processed++;
            }
            catch (Exception ex)
            {
                // Skip invalid geometries
                skipped++;
                var reason = $"Database error: {ex.GetType().Name}";
                skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;
            }
        }

        await transaction.CommitAsync(cancellationToken);
        
        // Log summary
        _logger?.LogInformation("Countries import: processed={Processed}, skipped={Skipped}", processed, skipped);
        foreach (var reason in skippedReasons)
        {
            _logger?.LogInformation("  Skipped reason '{Reason}': {Count}", reason.Key, reason.Value);
        }
    }

    public async Task ImportRegionsAsync(IEnumerable<RegionEntity> regions, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var region in regions)
        {
            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, geometry)
                VALUES (@identifier, @name, @countryCode, ST_GeomFromWKB(@geometry, 4326))
                ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;", connection, transaction);

            var wkbWriter = new NetTopologySuite.IO.WKBWriter();
            var wkb = wkbWriter.Write(region.Geometry);

            cmd.Parameters.AddWithValue("identifier", region.Identifier);
            cmd.Parameters.AddWithValue("name", region.NameLatin);
            cmd.Parameters.AddWithValue("countryCode", region.CountryIsoAlpha2Code);
            cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task ImportRegionsFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var jsonDoc = JsonDocument.Parse(geoJsonContent);
        var root = jsonDoc.RootElement;
        var features = root.GetProperty("features");

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        int processed = 0;
        int skipped = 0;

        foreach (var featureElement in features.EnumerateArray())
        {
            var properties = featureElement.GetProperty("properties");
            var geometryElement = featureElement.GetProperty("geometry");

            // Natural Earth Admin 1 uses fields: name, name_en, admin, adm0_a3, iso_a2, postal, etc.
            // Get country ISO code (alpha-2)
            string? countryIsoCode = null;
            
            // Try ISO_A2 (uppercase - Natural Earth format)
            if (properties.TryGetProperty("ISO_A2", out var isoA2Upper) && isoA2Upper.ValueKind == JsonValueKind.String)
            {
                var isoValue = isoA2Upper.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
                    countryIsoCode = isoValue.ToUpperInvariant();
            }
            // Try iso_a2 (lowercase - after ogr2ogr conversion)
            else if (properties.TryGetProperty("iso_a2", out var isoA2) && isoA2.ValueKind == JsonValueKind.String)
            {
                var isoValue = isoA2.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
                    countryIsoCode = isoValue.ToUpperInvariant();
            }
            // Try ADM0_ISO (uppercase)
            else if (properties.TryGetProperty("ADM0_ISO", out var adm0IsoUpper) && adm0IsoUpper.ValueKind == JsonValueKind.String)
            {
                var isoValue = adm0IsoUpper.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue.Length == 2)
                    countryIsoCode = isoValue.ToUpperInvariant();
            }
            // Try adm0_iso (lowercase - after ogr2ogr conversion)
            else if (properties.TryGetProperty("adm0_iso", out var adm0Iso) && adm0Iso.ValueKind == JsonValueKind.String)
            {
                var isoValue = adm0Iso.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue.Length == 2)
                    countryIsoCode = isoValue.ToUpperInvariant();
            }
            // Try ADM0_A3 (uppercase) and map to alpha-2
            else if (properties.TryGetProperty("ADM0_A3", out var adm0A3Upper) && adm0A3Upper.ValueKind == JsonValueKind.String)
            {
                var alpha3Value = adm0A3Upper.GetString();
                if (!string.IsNullOrWhiteSpace(alpha3Value))
                    countryIsoCode = Alpha3ToAlpha2(alpha3Value);
            }
            // Try adm0_a3 (lowercase - after ogr2ogr conversion) and map to alpha-2
            else if (properties.TryGetProperty("adm0_a3", out var adm0A3) && adm0A3.ValueKind == JsonValueKind.String)
            {
                var alpha3Value = adm0A3.GetString();
                if (!string.IsNullOrWhiteSpace(alpha3Value))
                    countryIsoCode = Alpha3ToAlpha2(alpha3Value);
            }

            if (string.IsNullOrWhiteSpace(countryIsoCode) || countryIsoCode.Length != 2)
            {
                skipped++;
                // Log first few skipped regions for debugging
                if (skipped <= 10)
                {
                    var regionName = properties.TryGetProperty("name", out var nameProp) && nameProp.ValueKind == JsonValueKind.String 
                        ? nameProp.GetString() 
                        : properties.TryGetProperty("name_en", out var nameEnProp) && nameEnProp.ValueKind == JsonValueKind.String
                            ? nameEnProp.GetString()
                            : "Unknown";
                    _logger?.LogDebug("Skipped region '{RegionName}' - missing/invalid country ISO code", regionName);
                }
                continue;
            }

            // Get region name
            var nameLatin = "Unknown";
            if (properties.TryGetProperty("name_en", out var nameEn) && nameEn.ValueKind == JsonValueKind.String)
                nameLatin = nameEn.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                nameLatin = name.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("NAME", out var nameUpper) && nameUpper.ValueKind == JsonValueKind.String)
                nameLatin = nameUpper.GetString() ?? "Unknown";

            if (string.IsNullOrWhiteSpace(nameLatin) || nameLatin == "Unknown")
            {
                skipped++;
                continue;
            }

            // Generate identifier - prefer postal code, then use name + country code
            // Natural Earth Admin 1 may have postal codes or use other identifiers
            string identifier;
            
            // Try POSTAL (uppercase) first
            if (properties.TryGetProperty("POSTAL", out var postalUpper) && postalUpper.ValueKind == JsonValueKind.String)
            {
                var postalValue = postalUpper.GetString();
                if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
                    identifier = postalValue.ToUpperInvariant();
                else
                    identifier = $"{nameLatin}_{countryIsoCode}";
            }
            // Try postal (lowercase - after ogr2ogr conversion)
            else if (properties.TryGetProperty("postal", out var postal) && postal.ValueKind == JsonValueKind.String)
            {
                var postalValue = postal.GetString();
                if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
                    identifier = postalValue.ToUpperInvariant();
                else
                    identifier = $"{nameLatin}_{countryIsoCode}";
            }
            // Try POSTAL_CODE (uppercase)
            else if (properties.TryGetProperty("POSTAL_CODE", out var postalCodeUpper) && postalCodeUpper.ValueKind == JsonValueKind.String)
            {
                var postalValue = postalCodeUpper.GetString();
                if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
                    identifier = postalValue.ToUpperInvariant();
                else
                    identifier = $"{nameLatin}_{countryIsoCode}";
            }
            // Try postal_code (lowercase)
            else if (properties.TryGetProperty("postal_code", out var postalCode) && postalCode.ValueKind == JsonValueKind.String)
            {
                var postalValue = postalCode.GetString();
                if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
                    identifier = postalValue.ToUpperInvariant();
                else
                    identifier = $"{nameLatin}_{countryIsoCode}";
            }
            else
            {
                // Use combination of name and country code as identifier
                identifier = $"{nameLatin}_{countryIsoCode}";
            }

            // Normalize identifier (remove special characters, spaces, make safe for database)
            // Remove or replace characters that might cause issues in identifiers
            identifier = System.Text.RegularExpressions.Regex.Replace(identifier, @"[^a-zA-Z0-9_]", "_");
            // Collapse multiple underscores into one
            identifier = System.Text.RegularExpressions.Regex.Replace(identifier, @"_+", "_");
            // Remove leading/trailing underscores
            identifier = identifier.Trim('_');

            var geometryJson = geometryElement.GetRawText();

            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, geometry)
                VALUES (@identifier, @name, @countryCode, ST_GeomFromGeoJSON(@geometryJson))
                ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;", connection, transaction);

            cmd.Parameters.AddWithValue("identifier", identifier);
            cmd.Parameters.AddWithValue("name", nameLatin);
            cmd.Parameters.AddWithValue("countryCode", countryIsoCode);
            cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);
            
            try
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                processed++;
            }
            catch
            {
                // Skip invalid geometries
                skipped++;
            }
        }

        await transaction.CommitAsync(cancellationToken);
        
        // Log summary
        _logger?.LogInformation("Regions import: processed={Processed}, skipped={Skipped}", processed, skipped);
        
        // Query to check regions per country (for debugging)
        try
        {
            await using var checkCmd = new NpgsqlCommand(
                @"SELECT country_iso_alpha2_code, COUNT(*) as region_count 
                  FROM regions 
                  GROUP BY country_iso_alpha2_code 
                  ORDER BY region_count DESC 
                  LIMIT 10;", 
            connection);
        
            await using var reader = await checkCmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var countryCode = reader.GetString(0);
                var count = reader.GetInt64(1);
                _logger?.LogInformation("  Country {CountryCode}: {Count} regions", countryCode, count);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to query region counts");
        }
    }

    public async Task ImportCitiesAsync(IEnumerable<CityEntity> cities, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var city in cities)
        {
            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, region_identifier, geometry)
                VALUES (@identifier, @name, @countryCode, @regionIdentifier, ST_GeomFromWKB(@geometry, 4326))
                ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;", connection, transaction);

            var wkbWriter = new NetTopologySuite.IO.WKBWriter();
            var wkb = wkbWriter.Write(city.Geometry);

            cmd.Parameters.AddWithValue("identifier", city.Identifier);
            cmd.Parameters.AddWithValue("name", city.NameLatin);
            cmd.Parameters.AddWithValue("countryCode", city.CountryIsoAlpha2Code);
            cmd.Parameters.AddWithValue("regionIdentifier", city.RegionIdentifier ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task ImportCitiesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        var jsonDoc = JsonDocument.Parse(geoJsonContent);
        var root = jsonDoc.RootElement;
        var features = root.GetProperty("features");

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        int processed = 0;
        int skipped = 0;

        foreach (var featureElement in features.EnumerateArray())
        {
            var properties = featureElement.GetProperty("properties");
            var geometryElement = featureElement.GetProperty("geometry");

            // Natural Earth Populated Places uses fields: name, nameascii, namealt, adm0name, adm0_a3, adm0cap, iso_a2, etc.
            // Get country ISO code (alpha-2)
            string? countryIsoCode = null;
            
            // Try iso_a2 (country ISO alpha-2)
            if (properties.TryGetProperty("iso_a2", out var isoA2) && isoA2.ValueKind == JsonValueKind.String)
            {
                var isoValue = isoA2.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
                    countryIsoCode = isoValue.ToUpperInvariant();
            }
            // Try adm0_iso
            else if (properties.TryGetProperty("adm0_iso", out var adm0Iso) && adm0Iso.ValueKind == JsonValueKind.String)
            {
                var isoValue = adm0Iso.GetString();
                if (!string.IsNullOrWhiteSpace(isoValue) && isoValue.Length == 2)
                    countryIsoCode = isoValue.ToUpperInvariant();
            }
            // Try adm0_a3 and map to alpha-2
            else if (properties.TryGetProperty("adm0_a3", out var adm0A3) && adm0A3.ValueKind == JsonValueKind.String)
            {
                var alpha3Value = adm0A3.GetString();
                if (!string.IsNullOrWhiteSpace(alpha3Value))
                    countryIsoCode = Alpha3ToAlpha2(alpha3Value);
            }

            if (string.IsNullOrWhiteSpace(countryIsoCode) || countryIsoCode.Length != 2)
            {
                skipped++;
                continue;
            }

            // Get city name
            var nameLatin = "Unknown";
            if (properties.TryGetProperty("nameascii", out var nameAscii) && nameAscii.ValueKind == JsonValueKind.String)
                nameLatin = nameAscii.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
                nameLatin = name.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("NAME", out var nameUpper) && nameUpper.ValueKind == JsonValueKind.String)
                nameLatin = nameUpper.GetString() ?? "Unknown";

            if (string.IsNullOrWhiteSpace(nameLatin) || nameLatin == "Unknown")
            {
                skipped++;
                continue;
            }

            // Get region identifier (admin level 1) - optional
            string? regionIdentifier = null;
            if (properties.TryGetProperty("adm1name", out var adm1Name) && adm1Name.ValueKind == JsonValueKind.String)
            {
                var adm1Value = adm1Name.GetString();
                if (!string.IsNullOrWhiteSpace(adm1Value))
                {
                    regionIdentifier = System.Text.RegularExpressions.Regex.Replace(adm1Value, @"[^a-zA-Z0-9_]", "_");
                    regionIdentifier = System.Text.RegularExpressions.Regex.Replace(regionIdentifier, @"_+", "_");
                    regionIdentifier = regionIdentifier.Trim('_');
                }
            }

            // Generate city identifier - use nameascii + country code, or geonames_id if available
            string identifier;
            if (properties.TryGetProperty("geonameid", out var geonameId) && geonameId.ValueKind == JsonValueKind.Number)
            {
                identifier = geonameId.GetInt64().ToString();
            }
            else if (properties.TryGetProperty("GN_ID", out var gnId) && gnId.ValueKind == JsonValueKind.Number)
            {
                identifier = gnId.GetInt64().ToString();
            }
            else
            {
                // Use combination of name and country code as identifier
                identifier = $"{nameLatin}_{countryIsoCode}";
            }

            // Normalize identifier
            identifier = System.Text.RegularExpressions.Regex.Replace(identifier, @"[^a-zA-Z0-9_]", "_");
            identifier = System.Text.RegularExpressions.Regex.Replace(identifier, @"_+", "_");
            identifier = identifier.Trim('_');

            // For cities, geometry is typically a Point
            var geometryJson = geometryElement.GetRawText();

            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, region_identifier, geometry)
                VALUES (@identifier, @name, @countryCode, @regionIdentifier, ST_GeomFromGeoJSON(@geometryJson))
                ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;", connection, transaction);

            cmd.Parameters.AddWithValue("identifier", identifier);
            cmd.Parameters.AddWithValue("name", nameLatin);
            cmd.Parameters.AddWithValue("countryCode", countryIsoCode);
            cmd.Parameters.AddWithValue("regionIdentifier", regionIdentifier ?? (object)DBNull.Value);
            cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);
            
            try
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
                processed++;
            }
            catch
            {
                // Skip invalid geometries
                skipped++;
            }
        }

        await transaction.CommitAsync(cancellationToken);
    }

    public async Task ImportTimezonesAsync(IEnumerable<TimezoneEntity> timezones, CancellationToken cancellationToken = default)
    {
        await using var connection = _npgsqlDataSource.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var timezone in timezones)
        {
            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO timezones (timezone_id, geometry)
                VALUES (@timezoneId, ST_GeomFromWKB(@geometry, 4326))
                ON CONFLICT (timezone_id) DO UPDATE
                SET geometry = EXCLUDED.geometry;", connection, transaction);

            var wkbWriter = new NetTopologySuite.IO.WKBWriter();
            var wkb = wkbWriter.Write(timezone.Geometry);

            cmd.Parameters.AddWithValue("timezoneId", timezone.TimezoneId);
            cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
}

}
