using GeoResolver.Models;

namespace GeoResolver.DataUpdater.Services;

public interface IDatabaseWriterService
{
	Task SetLastUpdateTimeAsync(DateTimeOffset updateTime, CancellationToken cancellationToken = default);
	Task ClearAllDataAsync(CancellationToken cancellationToken = default);
	Task ImportCountriesAsync(IEnumerable<Country> countries, CancellationToken cancellationToken = default);
	Task ImportCountriesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
	Task ImportRegionsAsync(IEnumerable<Region> regions, CancellationToken cancellationToken = default);
	Task ImportRegionsFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
	Task ImportCitiesAsync(IEnumerable<City> cities, CancellationToken cancellationToken = default);
	Task ImportCitiesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
	Task ImportTimezonesAsync(IEnumerable<Timezone> timezones, CancellationToken cancellationToken = default);
	Task ImportTimezonesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default);
}