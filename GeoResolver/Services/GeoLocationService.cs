using GeoResolver.Models;
using Npgsql;
using NetTopologySuite.Geometries;
using NpgsqlTypes;

namespace GeoResolver.Services;

public sealed class GeoLocationService : IGeoLocationService
{
    private readonly NpgsqlDataSource _npgsqlDataSource;

    public GeoLocationService(NpgsqlDataSource npgsqlDataSource)
    {
        _npgsqlDataSource = npgsqlDataSource;
    }

    public async Task<GeoLocationResponse?> ResolveAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        var countryTask = FindCountryByPointAsync(latitude, longitude, cancellationToken);
        var regionTask = FindRegionByPointAsync(latitude, longitude, cancellationToken);
        var cityTask = FindCityByPointAsync(latitude, longitude, cancellationToken);
        var timezoneTask = GetTimezoneOffsetAsync(latitude, longitude, cancellationToken);

        await Task.WhenAll(countryTask, regionTask, cityTask, timezoneTask);

        var country = await countryTask;
        if (country == null)
        {
            return null;
        }

        var region = await regionTask;
        var city = await cityTask;
        var timezone = await timezoneTask;

        return new GeoLocationResponse
        {
            CountryIsoAlpha2Code = country.IsoAlpha2Code,
            CountryNameLatin = country.NameLatin,
            RegionIdentifier = region?.Identifier,
            RegionNameLatin = region?.NameLatin,
            CityIdentifier = city?.Identifier,
            CityNameLatin = city?.NameLatin,
            TimezoneRawOffsetSeconds = timezone?.RawOffset ?? 0,
            TimezoneDstOffsetSeconds = timezone?.DstOffset ?? 0
        };
    }

    private async Task<CountryEntity?> FindCountryByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
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

    private async Task<RegionEntity?> FindRegionByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
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

    private async Task<CityEntity?> FindCityByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
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

    private async Task<(int RawOffset, int DstOffset)?> GetTimezoneOffsetAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
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
}
