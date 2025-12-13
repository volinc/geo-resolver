using GeoResolver.DataUpdater.Models;

namespace GeoResolver.DataUpdater.Services;

public interface IDatabaseWriterService
{
    Task InitializeAsync(CancellationToken cancellationToken = default);
    Task SetLastUpdateTimeAsync(DateTimeOffset updateTime, CancellationToken cancellationToken = default);
    Task ClearAllDataAsync(CancellationToken cancellationToken = default);
    Task ImportCountriesAsync(IEnumerable<CountryEntity> countries, CancellationToken cancellationToken = default);
    Task ImportCountriesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
    Task ImportRegionsAsync(IEnumerable<RegionEntity> regions, CancellationToken cancellationToken = default);
    Task ImportRegionsFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
    Task ImportCitiesAsync(IEnumerable<CityEntity> cities, CancellationToken cancellationToken = default);
    Task ImportCitiesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
    Task ImportTimezonesAsync(IEnumerable<TimezoneEntity> timezones, CancellationToken cancellationToken = default);
}

