namespace GeoResolver.DataUpdater.Services.Shapefile;

public interface INaturalEarthShapefileLoader
{
	/// <summary>
	///     Downloads Natural Earth Admin 0 (Countries) Shapefile ZIP and processes it
	/// </summary>
	Task LoadCountriesAsync(CancellationToken cancellationToken = default);

	/// <summary>
	///     Downloads Natural Earth Admin 1 (States/Provinces) Shapefile ZIP and processes it
	/// </summary>
	Task LoadRegionsAsync(CancellationToken cancellationToken = default);

	/// <summary>
	///     Downloads Natural Earth Populated Places Shapefile ZIP and processes it
	/// </summary>
	Task LoadCitiesAsync(CancellationToken cancellationToken = default);

	/// <summary>
	///     Downloads and loads timezone boundaries from timezone-boundary-builder
	///     Source: https://github.com/evansiroky/timezone-boundary-builder/releases
	/// </summary>
	Task LoadTimezonesAsync(CancellationToken cancellationToken = default);
}