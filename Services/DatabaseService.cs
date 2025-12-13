using System.Data;
using System.Text.Json;
using GeoResolver.Models;
using Npgsql;
using NetTopologySuite.Geometries;
using NpgsqlTypes;

namespace GeoResolver.Services;

public class DatabaseService : IDatabaseService
{
    private readonly string _connectionString;
    private const string LockTableName = "app_locks";

    public DatabaseService(IConfiguration configuration)
    {
        var host = configuration["Database:Host"] ?? "localhost";
        var port = configuration["Database:Port"] ?? "5432";
        var database = configuration["Database:Name"] ?? "georesolver";
        var username = configuration["Database:Username"] ?? "georesolver";
        var password = configuration["Database:Password"] ?? throw new InvalidOperationException("Database password must be configured");

        _connectionString = $"Host={host};Port={port};Database={database};Username={username};Password={password};";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Ensure PostGIS extension exists
        await using var cmd1 = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS postgis;", connection);
        await cmd1.ExecuteNonQueryAsync(cancellationToken);

        // Create locks table for distributed locking
        await using var cmd2 = new NpgsqlCommand($@"
            CREATE TABLE IF NOT EXISTS {LockTableName} (
                lock_name VARCHAR(255) PRIMARY KEY,
                acquired_at TIMESTAMP WITH TIME ZONE NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL
            );", connection);
        await cmd2.ExecuteNonQueryAsync(cancellationToken);

        // Create countries table
        await using var cmd3 = new NpgsqlCommand(@"
            CREATE TABLE IF NOT EXISTS countries (
                id SERIAL PRIMARY KEY,
                iso_alpha2_code VARCHAR(2) NOT NULL UNIQUE,
                name_latin VARCHAR(255) NOT NULL,
                geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_countries_geometry ON countries USING GIST (geometry);", connection);
        await cmd3.ExecuteNonQueryAsync(cancellationToken);

        // Create regions table
        await using var cmd4 = new NpgsqlCommand(@"
            CREATE TABLE IF NOT EXISTS regions (
                id SERIAL PRIMARY KEY,
                identifier VARCHAR(100) NOT NULL,
                name_latin VARCHAR(255) NOT NULL,
                country_iso_alpha2_code VARCHAR(2) NOT NULL,
                geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
                UNIQUE(identifier, country_iso_alpha2_code)
            );
            CREATE INDEX IF NOT EXISTS idx_regions_geometry ON regions USING GIST (geometry);
            CREATE INDEX IF NOT EXISTS idx_regions_country ON regions (country_iso_alpha2_code);", connection);
        await cmd4.ExecuteNonQueryAsync(cancellationToken);

        // Create cities table
        await using var cmd5 = new NpgsqlCommand(@"
            CREATE TABLE IF NOT EXISTS cities (
                id SERIAL PRIMARY KEY,
                identifier VARCHAR(100) NOT NULL,
                name_latin VARCHAR(255) NOT NULL,
                country_iso_alpha2_code VARCHAR(2) NOT NULL,
                region_identifier VARCHAR(100),
                geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
                UNIQUE(identifier, country_iso_alpha2_code)
            );
            CREATE INDEX IF NOT EXISTS idx_cities_geometry ON cities USING GIST (geometry);
            CREATE INDEX IF NOT EXISTS idx_cities_country ON cities (country_iso_alpha2_code);
            CREATE INDEX IF NOT EXISTS idx_cities_region ON cities (region_identifier);", connection);
        await cmd5.ExecuteNonQueryAsync(cancellationToken);

        // Create timezones table
        await using var cmd6 = new NpgsqlCommand(@"
            CREATE TABLE IF NOT EXISTS timezones (
                id SERIAL PRIMARY KEY,
                timezone_id VARCHAR(100) NOT NULL UNIQUE,
                geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_timezones_geometry ON timezones USING GIST (geometry);", connection);
        await cmd6.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<CountryEntity?> FindCountryByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
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
        await using var connection = new NpgsqlConnection(_connectionString);
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
        await using var connection = new NpgsqlConnection(_connectionString);
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
        await using var connection = new NpgsqlConnection(_connectionString);
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

    public async Task<bool> TryAcquireLockAsync(string lockName, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        // Clean expired locks
        await using var cmd1 = new NpgsqlCommand($@"
            DELETE FROM {LockTableName}
            WHERE expires_at < NOW();", connection);
        await cmd1.ExecuteNonQueryAsync(cancellationToken);

        var expiresAt = DateTimeOffset.UtcNow.Add(timeout);
        await using var cmd2 = new NpgsqlCommand($@"
            INSERT INTO {LockTableName} (lock_name, acquired_at, expires_at)
            VALUES (@lockName, NOW(), @expiresAt)
            ON CONFLICT (lock_name) DO UPDATE
            SET acquired_at = NOW(), expires_at = @expiresAt
            WHERE {LockTableName}.expires_at < NOW();", connection);

        cmd2.Parameters.AddWithValue("lockName", lockName);
        cmd2.Parameters.AddWithValue("expiresAt", expiresAt);
        
        var rowsAffected = await cmd2.ExecuteNonQueryAsync(cancellationToken);
        return rowsAffected > 0;
    }

    public async Task ReleaseLockAsync(string lockName, CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var cmd = new NpgsqlCommand($@"
            DELETE FROM {LockTableName}
            WHERE lock_name = @lockName;", connection);
        cmd.Parameters.AddWithValue("lockName", lockName);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task ClearAllDataAsync(CancellationToken cancellationToken = default)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
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
        await using var connection = new NpgsqlConnection(_connectionString);
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
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        var jsonDoc = JsonDocument.Parse(geoJsonContent);
        var root = jsonDoc.RootElement;
        var features = root.GetProperty("features");

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        foreach (var featureElement in features.EnumerateArray())
        {
            var properties = featureElement.GetProperty("properties");
            var geometryElement = featureElement.GetProperty("geometry");

            // Try to get ISO code
            string? isoCode = null;
            if (properties.TryGetProperty("ISO_A2", out var isoA2))
                isoCode = isoA2.GetString()?.ToUpperInvariant();
            else if (properties.TryGetProperty("ISO", out var iso))
                isoCode = iso.GetString()?.ToUpperInvariant();
            else if (properties.TryGetProperty("iso_a2", out var isoA2Lower))
                isoCode = isoA2Lower.GetString()?.ToUpperInvariant();
            else if (properties.TryGetProperty("iso_a2_eh", out var isoA2Eh))
                isoCode = isoA2Eh.GetString()?.ToUpperInvariant();

            var nameLatin = "Unknown";
            if (properties.TryGetProperty("NAME", out var name))
                nameLatin = name.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("NAME_LONG", out var nameLong))
                nameLatin = nameLong.GetString() ?? "Unknown";
            else if (properties.TryGetProperty("name", out var nameLower))
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
        await using var connection = new NpgsqlConnection(_connectionString);
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
        await using var connection = new NpgsqlConnection(_connectionString);
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
        await using var connection = new NpgsqlConnection(_connectionString);
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

