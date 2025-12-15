namespace GeoResolver.DataUpdater.Services.Shapefile;

/// <summary>
///     Resolves Geofabrik region paths (URL paths) for a given country ISO alpha-2 code.
///     For many countries, Geofabrik provides a single country-level shapefile.
///     For some large countries (e.g., Russia), only regional shapefiles exist.
/// </summary>
public sealed class GeofabrikRegionPathResolver
{
	/// <summary>
	///     Gets the list of Geofabrik region paths for the specified country.
	///     Returns relative paths (e.g., "europe/germany" or "russia/central-fed-district")
	///     that should be appended to "https://download.geofabrik.de/" and suffixed with "-latest-free.shp.zip".
	/// </summary>
	/// <param name="countryIsoAlpha2Code">2-letter ISO country code (e.g., "DE", "RU", "FR")</param>
	/// <returns>List of region paths. For most countries, returns a single path. For countries like Russia, returns multiple regional paths.</returns>
	public IReadOnlyList<string> GetRegionPaths(string countryIsoAlpha2Code)
	{
		if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code) || countryIsoAlpha2Code.Length != 2)
			throw new ArgumentException("Country ISO alpha-2 code must be 2-letter non-empty string",
				nameof(countryIsoAlpha2Code));

		var code = countryIsoAlpha2Code.ToUpperInvariant();

		// Special case: Russia – only regional shapefiles, no single country-wide shapefile
		if (code == "RU")
		{
			// Список федеральных округов России на Geofabrik.
			// Каждый из этих путей даёт свой *-latest-free.shp.zip.
			return
			[
				"russia/central-fed-district",
				"russia/northwestern-fed-district",
				"russia/siberian-fed-district",
				"russia/ural-fed-district",
				"russia/far-eastern-fed-district",
				"russia/volga-fed-district",
				"russia/south-fed-district",
				"russia/north-caucasus-fed-district"
			];
		}

		// Default: single country-level shapefile
		var path = code switch
		{
			"DE" => "europe/germany",
			"FR" => "europe/france",
			"IT" => "europe/italy",
			"ES" => "europe/spain",
			"PT" => "europe/portugal",
			"PL" => "europe/poland",
			"NL" => "europe/netherlands",
			"BE" => "europe/belgium",
			"LU" => "europe/luxembourg",
			"AT" => "europe/austria",
			"CH" => "europe/switzerland",
			"CZ" => "europe/czech-republic",
			"SK" => "europe/slovakia",
			"HU" => "europe/hungary",
			"SI" => "europe/slovenia",
			"HR" => "europe/croatia",
			"RS" => "europe/serbia",
			"BA" => "europe/bosnia-herzegovina",
			"ME" => "europe/montenegro",
			"AL" => "europe/albania",
			"MK" => "europe/macedonia",
			"GR" => "europe/greece",
			"BG" => "europe/bulgaria",
			"RO" => "europe/romania",
			// Ukraine, Belarus – country-level shapefiles exist
			"UA" => "europe/ukraine",
			"BY" => "europe/belarus",
			// Fallback: try using "europe/{lowercase-name}" is not possible without mapping, so default to continent-level
			_ => "europe"
		};

		return [path];
	}
}
