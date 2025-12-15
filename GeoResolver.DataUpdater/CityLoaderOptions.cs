namespace GeoResolver.DataUpdater;

/// <summary>
///     Options for configuring which countries' city polygons
///     should be loaded from OSM/Geofabrik.
/// </summary>
public sealed class CityLoaderOptions
{
	// List of countries (2-letter ISO codes) for which city polygons
	// should be loaded from OSM/Geofabrik. Example:
	// "Countries": [ "DE", "FR", "RU" ]
	public string[] Countries { get; init; } = [];
}