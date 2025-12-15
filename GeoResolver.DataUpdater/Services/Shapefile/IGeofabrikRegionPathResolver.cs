namespace GeoResolver.DataUpdater.Services.Shapefile;

public interface IGeofabrikRegionPathResolver
{
	/// <summary>
	///     Gets the list of Geofabrik region paths for the specified country.
	///     Returns relative paths (e.g., "europe/germany" or "russia/central-fed-district")
	///     that should be appended to "https://download.geofabrik.de/" and suffixed with "-latest-free.shp.zip".
	/// </summary>
	/// <param name="countryIsoAlpha2Code">2-letter ISO country code (e.g., "DE", "RU", "FR")</param>
	/// <returns>List of region paths. For most countries, returns a single path. For countries like Russia, returns multiple regional paths.</returns>
	IReadOnlyList<string> GetRegionPaths(string countryIsoAlpha2Code);
}