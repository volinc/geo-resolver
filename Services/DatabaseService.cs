using System.Text.Json;
using GeoResolver.Models;
using Npgsql;
using NetTopologySuite.Geometries;
using NpgsqlTypes;

namespace GeoResolver.Services;

public sealed class DatabaseService : IDatabaseService
{
    private readonly NpgsqlDataSource _npgsqlDataSource;

    public DatabaseService(NpgsqlDataSource npgsqlDataSource)
    {
        _npgsqlDataSource = npgsqlDataSource;
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

        // Simple mapping from ISO 3166-1 alpha-3 to alpha-2 for common countries
        // This is a partial mapping - in production you'd want a complete table
        var alpha3ToAlpha2 = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "AFG", "AF" }, { "AGO", "AO" }, { "ALB", "AL" }, { "ARE", "AE" }, { "ARG", "AR" },
            { "ARM", "AM" }, { "ATA", "AQ" }, { "ATF", "TF" }, { "AUS", "AU" }, { "AUT", "AT" },
            { "AZE", "AZ" }, { "BDI", "BI" }, { "BEL", "BE" }, { "BEN", "BJ" }, { "BFA", "BF" },
            { "BGD", "BD" }, { "BGR", "BG" }, { "BHS", "BS" }, { "BIH", "BA" }, { "BLR", "BY" },
            { "BLZ", "BZ" }, { "BOL", "BO" }, { "BRA", "BR" }, { "BRN", "BN" }, { "BTN", "BT" },
            { "BWA", "BW" }, { "CAF", "CF" }, { "CAN", "CA" }, { "CHE", "CH" }, { "CHL", "CL" },
            { "CHN", "CN" }, { "CIV", "CI" }, { "CMR", "CM" }, { "COD", "CD" }, { "COG", "CG" },
            { "COL", "CO" }, { "COM", "KM" }, { "CPV", "CV" }, { "CRI", "CR" }, { "CUB", "CU" },
            { "CYP", "CY" }, { "CZE", "CZ" }, { "DEU", "DE" }, { "DJI", "DJ" }, { "DNK", "DK" },
            { "DOM", "DO" }, { "DZA", "DZ" }, { "ECU", "EC" }, { "EGY", "EG" }, { "ERI", "ER" },
            { "ESP", "ES" }, { "EST", "EE" }, { "ETH", "ET" }, { "FIN", "FI" }, { "FJI", "FJ" },
            { "FRA", "FR" }, { "FSM", "FM" }, { "GAB", "GA" }, { "GBR", "GB" }, { "GEO", "GE" },
            { "GHA", "GH" }, { "GIN", "GN" }, { "GMB", "GM" }, { "GNB", "GW" }, { "GNQ", "GQ" },
            { "GRC", "GR" }, { "GRD", "GD" }, { "GTM", "GT" }, { "GUY", "GY" }, { "HND", "HN" },
            { "HRV", "HR" }, { "HTI", "HT" }, { "HUN", "HU" }, { "IDN", "ID" }, { "IND", "IN" },
            { "IRL", "IE" }, { "IRN", "IR" }, { "IRQ", "IQ" }, { "ISL", "IS" }, { "ISR", "IL" },
            { "ITA", "IT" }, { "JAM", "JM" }, { "JOR", "JO" }, { "JPN", "JP" }, { "KAZ", "KZ" },
            { "KEN", "KE" }, { "KGZ", "KG" }, { "KHM", "KH" }, { "KIR", "KI" }, { "KNA", "KN" },
            { "KOR", "KR" }, { "KWT", "KW" }, { "LAO", "LA" }, { "LBN", "LB" }, { "LBR", "LR" },
            { "LBY", "LY" }, { "LCA", "LC" }, { "LIE", "LI" }, { "LKA", "LK" }, { "LSO", "LS" },
            { "LTU", "LT" }, { "LUX", "LU" }, { "LVA", "LV" }, { "MAR", "MA" }, { "MCO", "MC" },
            { "MDA", "MD" }, { "MDG", "MG" }, { "MDV", "MV" }, { "MEX", "MX" }, { "MHL", "MH" },
            { "MKD", "MK" }, { "MLI", "ML" }, { "MLT", "MT" }, { "MMR", "MM" }, { "MNE", "ME" },
            { "MNG", "MN" }, { "MOZ", "MZ" }, { "MRT", "MR" }, { "MUS", "MU" }, { "MWI", "MW" },
            { "MYS", "MY" }, { "NAM", "NA" }, { "NER", "NE" }, { "NGA", "NG" }, { "NIC", "NI" },
            { "NLD", "NL" }, { "NOR", "NO" }, { "NPL", "NP" }, { "NRU", "NR" }, { "NZL", "NZ" },
            { "OMN", "OM" }, { "PAK", "PK" }, { "PAN", "PA" }, { "PER", "PE" }, { "PHL", "PH" },
            { "PLW", "PW" }, { "PNG", "PG" }, { "POL", "PL" }, { "PRK", "KP" }, { "PRT", "PT" },
            { "PRY", "PY" }, { "QAT", "QA" }, { "ROU", "RO" }, { "RUS", "RU" }, { "RWA", "RW" },
            { "SAU", "SA" }, { "SDN", "SD" }, { "SEN", "SN" }, { "SGP", "SG" }, { "SLB", "SB" },
            { "SLE", "SL" }, { "SLV", "SV" }, { "SOM", "SO" }, { "SRB", "RS" }, { "SSD", "SS" },
            { "STP", "ST" }, { "SUR", "SR" }, { "SVK", "SK" }, { "SVN", "SI" }, { "SWE", "SE" },
            { "SWZ", "SZ" }, { "SYC", "SC" }, { "SYR", "SY" }, { "TCD", "TD" }, { "TGO", "TG" },
            { "THA", "TH" }, { "TJK", "TJ" }, { "TKM", "TM" }, { "TLS", "TL" }, { "TON", "TO" },
            { "TTO", "TT" }, { "TUN", "TN" }, { "TUR", "TR" }, { "TUV", "TV" }, { "TZA", "TZ" },
            { "UGA", "UG" }, { "UKR", "UA" }, { "URY", "UY" }, { "USA", "US" }, { "UZB", "UZ" },
            { "VAT", "VA" }, { "VCT", "VC" }, { "VEN", "VE" }, { "VNM", "VN" }, { "VUT", "VU" },
            { "WSM", "WS" }, { "YEM", "YE" }, { "ZAF", "ZA" }, { "ZMB", "ZM" }, { "ZWE", "ZW" }
        };

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

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
                    // If it's 3 characters (alpha-3), map to alpha-2 using our mapping table
                    else if (idValue.Length == 3 && alpha3ToAlpha2.TryGetValue(idValue, out var mappedCode))
                    {
                        isoCode = mappedCode;
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
                continue;

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
            }
            catch
            {
                // Skip invalid geometries
            }
        }

        await transaction.CommitAsync(cancellationToken);
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

