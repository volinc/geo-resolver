namespace GeoResolver.DataUpdater.Services;

/// <summary>
///     Constants for GeoJSON (and related source) property names.
///     Using named constants instead of string literals improves readability and reduces typo risks.
/// </summary>
internal static class GeoJsonFieldNames
{
	public const string IsoA2 = "ISO_A2";
	public const string IsoA2Lower = "iso_a2";
	public const string IsoA2Eh = "ISO_A2_EH";
	public const string IsoA2EhLower = "iso_a2_eh";

	public const string IsoA3 = "ISO_A3";
	public const string IsoA3Lower = "iso_a3";
	public const string Adm0A3 = "ADM0_A3";
	public const string Adm0A3Lower = "adm0_a3";

	public const string Iso = "ISO";
	public const string Adm0Iso = "ADM0_ISO";
	public const string Adm0IsoLower = "adm0_iso";

	// OSM-style country codes
	public const string OsmIso3166Alpha2 = "ISO3166-1:alpha2";
	public const string OsmIso3166Alpha3 = "ISO3166-1:alpha3";
	public const string OsmIso3166 = "ISO3166-1";
	public const string OsmAddrCountry = "addr:country";

	// Names
	public const string Name = "name";
	public const string NameLocal = "name_local";
	public const string NameLong = "NAME_LONG";
	public const string NameAscii = "nameascii";

	// Region identifiers
	public const string Iso31662 = "ISO_3166_2";
	public const string Iso31662Lower = "iso_3166_2";
	public const string Adm1Code = "ADM1_CODE";
	public const string Adm1CodeLower = "adm1_code";
	public const string Postal = "POSTAL";
	public const string PostalLower = "postal";
	public const string PostalCode = "POSTAL_CODE";
	public const string PostalCodeLower = "postal_code";

	// City identifiers
	public const string OsmId = "osm_id";

	// Wikidata-related field names frequently seen in GeoJSON sources
	public static readonly string[] WikidataFieldNames =
	{
		"wikidata",
		"WIKIDATA",
		"wikidataid",
		"WIKIDATAID",
		"wikidata_id",
		"WIKIDATA_ID"
	};
}

