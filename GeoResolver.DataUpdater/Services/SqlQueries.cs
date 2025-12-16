namespace GeoResolver.DataUpdater.Services;

/// <summary>
///     Central place for SQL query text used by data updater.
///     Keeping SQL here improves readability and makes refactoring queries easier.
/// </summary>
internal static class SqlQueries
{
	public const string UpsertLastUpdate = @"
INSERT INTO last_update (id, updated_at) 
VALUES (1, @updateTime)
ON CONFLICT (id) 
DO UPDATE SET updated_at = @updateTime;";

	public const string TruncateCountries = "TRUNCATE TABLE countries CASCADE;";
	public const string TruncateRegions = "TRUNCATE TABLE regions CASCADE;";
	public const string TruncateCities = "TRUNCATE TABLE cities CASCADE;";
	public const string TruncateTimezones = "TRUNCATE TABLE timezones CASCADE;";

	public const string InsertCountryFromModel = @"
INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, wikidataid, geometry)
VALUES (@isoAlpha2Code, @isoAlpha3Code, @name, @wikidataid, ST_GeomFromWKB(@geometry, 4326))
ON CONFLICT (iso_alpha2_code) DO UPDATE
    SET iso_alpha3_code = EXCLUDED.iso_alpha3_code,
        name_latin       = EXCLUDED.name_latin,
        wikidataid       = EXCLUDED.wikidataid,
        geometry         = EXCLUDED.geometry;";

	public const string InsertCountryFromGeoJson = @"
INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, wikidataid, geometry)
VALUES (@isoAlpha2Code, @isoAlpha3Code, @name, @wikidataid, ST_GeomFromGeoJSON(@geometryJson))
ON CONFLICT (iso_alpha2_code) DO UPDATE
    SET iso_alpha3_code = EXCLUDED.iso_alpha3_code,
        name_latin       = EXCLUDED.name_latin, 
        wikidataid       = EXCLUDED.wikidataid,
        geometry         = EXCLUDED.geometry;";

	public const string UpdateCountryByAlpha3OnConflict = @"
UPDATE countries
SET iso_alpha2_code = COALESCE(@isoAlpha2Code, countries.iso_alpha2_code),
    name_latin      = @name,
    geometry        = ST_GeomFromGeoJSON(@geometryJson)
WHERE iso_alpha3_code = @isoAlpha3Code;";

	public const string LookupCountryAlpha3ByAlpha2 = @"
SELECT iso_alpha3_code
FROM countries
WHERE iso_alpha2_code = @alpha2Code
LIMIT 1;";

	public const string InsertTimezoneFromModel = @"
INSERT INTO timezones (timezone_id, geometry)
VALUES (@timezoneId, ST_GeomFromWKB(@geometry, 4326))
ON CONFLICT (timezone_id) DO UPDATE
    SET geometry = EXCLUDED.geometry;";

	public const string InsertTimezoneFromGeoJson = @"
INSERT INTO timezones (timezone_id, geometry)
VALUES (@timezoneId, ST_GeomFromGeoJSON(@geometryJson))
ON CONFLICT (timezone_id) DO UPDATE
    SET geometry = EXCLUDED.geometry;";

	public const string FindCountryByPoint = @"
SELECT id, iso_alpha2_code, iso_alpha3_code, name_latin, wikidataid, geometry
FROM countries
WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
LIMIT 1;";

	public const string FindRegionByPoint = @"
SELECT id, identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, wikidataid, name_local, geometry
FROM regions
WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
LIMIT 1;";

	public const string FindCityByPoint = @"
SELECT id, identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, name_local, geometry
FROM cities
WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
LIMIT 1;";

	public const string FindTimezoneByPoint = @"
SELECT timezone_id
FROM timezones
WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
LIMIT 1;";
}

