using GeoResolver.Models;

namespace GeoResolver.Services;

public interface IDatabaseService
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
    Task<CountryEntity?> FindCountryByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default);
    Task<RegionEntity?> FindRegionByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default);
    Task<CityEntity?> FindCityByPointAsync(double latitude, double longitude, CancellationToken cancellationToken = default);
    Task<(int RawOffset, int DstOffset)?> GetTimezoneOffsetAsync(double latitude, double longitude, CancellationToken cancellationToken = default);
    Task<bool> TryAcquireLockAsync(string lockName, TimeSpan timeout, CancellationToken cancellationToken = default);
    Task ReleaseLockAsync(string lockName, CancellationToken cancellationToken = default);
    Task ClearAllDataAsync(CancellationToken cancellationToken = default);
    Task ImportCountriesAsync(IEnumerable<CountryEntity> countries, CancellationToken cancellationToken = default);
    Task ImportCountriesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
    Task ImportRegionsAsync(IEnumerable<RegionEntity> regions, CancellationToken cancellationToken = default);
    Task ImportCitiesAsync(IEnumerable<CityEntity> cities, CancellationToken cancellationToken = default);
    Task ImportTimezonesAsync(IEnumerable<TimezoneEntity> timezones, CancellationToken cancellationToken = default);
}

