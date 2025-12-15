using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using GeoResolver.Models;
using ICU4N.Text;
using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql;
using NpgsqlTypes;

namespace GeoResolver.DataUpdater.Services;

public sealed class DatabaseWriterService : IDatabaseWriterService
{
	private readonly ILogger<DatabaseWriterService>? _logger;
	private readonly NpgsqlDataSource _npgsqlDataSource;

	public DatabaseWriterService(NpgsqlDataSource npgsqlDataSource, ILogger<DatabaseWriterService>? logger = null)
	{
		_npgsqlDataSource = npgsqlDataSource;
		_logger = logger;
	}

	public async Task SetLastUpdateTimeAsync(DateTimeOffset updateTime, CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		await using var cmd = new NpgsqlCommand(
			@"INSERT INTO last_update (id, updated_at) 
              VALUES (1, @updateTime)
              ON CONFLICT (id) 
              DO UPDATE SET updated_at = @updateTime;",
			connection);

		cmd.Parameters.AddWithValue("@updateTime", updateTime.UtcDateTime);
		await cmd.ExecuteNonQueryAsync(cancellationToken);
	}

	public async Task ClearAllDataAsync(CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		await using var cmd1 = new NpgsqlCommand("TRUNCATE TABLE countries CASCADE;", connection);
		await cmd1.ExecuteNonQueryAsync(cancellationToken);

		await using var cmd2 = new NpgsqlCommand("TRUNCATE TABLE regions CASCADE;", connection);
		await cmd2.ExecuteNonQueryAsync(cancellationToken);

		await using var cmd3 = new NpgsqlCommand("TRUNCATE TABLE cities CASCADE;", connection);
		await cmd3.ExecuteNonQueryAsync(cancellationToken);

		await using var cmd4 = new NpgsqlCommand("TRUNCATE TABLE timezones CASCADE;", connection);
		await cmd4.ExecuteNonQueryAsync(cancellationToken);
	}

	public async Task ImportCountriesAsync(IEnumerable<Country> countries,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

		foreach (var country in countries)
		{
			// Build query based on which codes are available
			string insertQuery;
			if (!string.IsNullOrWhiteSpace(country.IsoAlpha2Code) && !string.IsNullOrWhiteSpace(country.IsoAlpha3Code))
				insertQuery = @"
                    INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, geometry)
                    VALUES (@isoAlpha2Code, @isoAlpha3Code, @name, ST_GeomFromWKB(@geometry, 4326))
                ON CONFLICT (iso_alpha2_code) DO UPDATE
                    SET iso_alpha3_code = COALESCE(EXCLUDED.iso_alpha3_code, countries.iso_alpha3_code),
                        name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;";
			else if (!string.IsNullOrWhiteSpace(country.IsoAlpha2Code))
				insertQuery = @"
                    INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, geometry)
                    VALUES (@isoAlpha2Code, NULL, @name, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (iso_alpha2_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;";
			else if (!string.IsNullOrWhiteSpace(country.IsoAlpha3Code))
				insertQuery = @"
                    INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, geometry)
                    VALUES (NULL, @isoAlpha3Code, @name, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;";
			else
				throw new InvalidOperationException(
					$"Country '{country.NameLatin}' must have at least one ISO code (alpha-2 or alpha-3)");

			await using var cmd = new NpgsqlCommand(insertQuery, connection, transaction);

			var wkbWriter = new WKBWriter();
			var wkb = wkbWriter.Write(country.Geometry);

			if (!string.IsNullOrWhiteSpace(country.IsoAlpha2Code))
				cmd.Parameters.AddWithValue("isoAlpha2Code", country.IsoAlpha2Code);
			if (!string.IsNullOrWhiteSpace(country.IsoAlpha3Code))
				cmd.Parameters.AddWithValue("isoAlpha3Code", country.IsoAlpha3Code);
			cmd.Parameters.AddWithValue("name", country.NameLatin);
			cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
			await cmd.ExecuteNonQueryAsync(cancellationToken);
		}

		await transaction.CommitAsync(cancellationToken);
	}

	public async Task ImportCountriesFromGeoJsonAsync(string geoJsonContent,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		var jsonDoc = JsonDocument.Parse(geoJsonContent);
		var root = jsonDoc.RootElement;
		var features = root.GetProperty("features");

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

		var processed = 0;
		var skipped = 0;
		var skippedReasons = new Dictionary<string, int>();

		foreach (var featureElement in features.EnumerateArray())
		{
			var properties = featureElement.GetProperty("properties");
			var geometryElement = featureElement.GetProperty("geometry");

			// Try to get ISO codes (both alpha-2 and alpha-3) from various possible fields
			// Different GeoJSON sources use different field names
			string? isoAlpha2Code = null;
			string? isoAlpha3Code = null;

			// Try ISO_A2 (Natural Earth format) - alpha-2 code
			if (properties.TryGetProperty("ISO_A2", out var isoA2) && isoA2.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA2.GetString();
				// Natural Earth uses "-99" for missing values
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
					isoAlpha2Code = isoValue.ToUpperInvariant();
			}
			// Try ISO (some sources use just ISO)
			else if (properties.TryGetProperty("ISO", out var iso) && iso.ValueKind == JsonValueKind.String)
			{
				var isoValue = iso.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue))
				{
					if (isoValue.Length == 2)
						isoAlpha2Code = isoValue.ToUpperInvariant();
					else if (isoValue.Length == 3)
						isoAlpha3Code = isoValue.ToUpperInvariant();
				}
			}
			// Try iso_a2 (lowercase)
			else if (properties.TryGetProperty("iso_a2", out var isoA2Lower) &&
			         isoA2Lower.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA2Lower.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
					isoAlpha2Code = isoValue.ToUpperInvariant();
			}
			// Try iso_a2_eh
			else if (properties.TryGetProperty("iso_a2_eh", out var isoA2Eh) &&
			         isoA2Eh.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA2Eh.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
					isoAlpha2Code = isoValue.ToUpperInvariant();
			}
			// Try ISO_A2_EH (uppercase variant)
			else if (properties.TryGetProperty("ISO_A2_EH", out var isoA2EhUpper) &&
			         isoA2EhUpper.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA2EhUpper.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
					isoAlpha2Code = isoValue.ToUpperInvariant();
			}

			// Try ISO_A3 (Natural Earth format) - alpha-3 code
			if (properties.TryGetProperty("ISO_A3", out var isoA3) && isoA3.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA3.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 3)
					isoAlpha3Code = isoValue.ToUpperInvariant();
			}
			// Try iso_a3 (lowercase)
			else if (properties.TryGetProperty("iso_a3", out var isoA3Lower) &&
			         isoA3Lower.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA3Lower.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 3)
					isoAlpha3Code = isoValue.ToUpperInvariant();
			}
			// Try ADM0_A3 (some sources use this for country alpha-3)
			else if (properties.TryGetProperty("ADM0_A3", out var adm0A3) && adm0A3.ValueKind == JsonValueKind.String)
			{
				var isoValue = adm0A3.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 3)
					isoAlpha3Code = isoValue.ToUpperInvariant();
			}
			// Try adm0_a3 (lowercase)
			else if (properties.TryGetProperty("adm0_a3", out var adm0A3Lower) &&
			         adm0A3Lower.ValueKind == JsonValueKind.String)
			{
				var isoValue = adm0A3Lower.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 3)
					isoAlpha3Code = isoValue.ToUpperInvariant();
			}

			// Try id field - some sources use ISO codes in id
			if (featureElement.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String)
			{
				var idValue = id.GetString();
				if (!string.IsNullOrWhiteSpace(idValue))
				{
					if (idValue.Length == 2 && string.IsNullOrWhiteSpace(isoAlpha2Code))
						isoAlpha2Code = idValue.ToUpperInvariant();
					else if (idValue.Length == 3 && string.IsNullOrWhiteSpace(isoAlpha3Code))
						isoAlpha3Code = idValue.ToUpperInvariant();
				}
			}
			// If we have alpha-2 but not alpha-3, try to convert (reverse lookup would be complex, skip for now)

			// Natural Earth uses NAME and NAME_LONG fields
			var nameLatin = "Unknown";
			if (properties.TryGetProperty("NAME", out var name) && name.ValueKind == JsonValueKind.String)
				nameLatin = name.GetString() ?? "Unknown";
			else if (properties.TryGetProperty("NAME_LONG", out var nameLong) &&
			         nameLong.ValueKind == JsonValueKind.String)
				nameLatin = nameLong.GetString() ?? "Unknown";
			else if (properties.TryGetProperty("name", out var nameLower) &&
			         nameLower.ValueKind == JsonValueKind.String)
				nameLatin = nameLower.GetString() ?? "Unknown";

			// At least one ISO code must be present
			if (string.IsNullOrWhiteSpace(isoAlpha2Code) && string.IsNullOrWhiteSpace(isoAlpha3Code))
			{
				skipped++;
				var reason = "Missing or invalid ISO code";
				skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;

				// Log first few skipped features for debugging
				if (skipped <= 10)
				{
					var allProperties = new StringBuilder();
					foreach (var prop in properties.EnumerateObject())
						allProperties.Append($"{prop.Name}={prop.Value}, ");
					_logger?.LogDebug("Skipped country (missing/invalid ISO code): {Properties}", allProperties);
				}

				continue;
			}

			var geometryJson = geometryElement.GetRawText();

			try
			{
				// Build the INSERT query dynamically based on which codes we have
				// If we have both codes, try insert with ON CONFLICT on alpha-2 first
				// If that fails due to alpha-3 conflict, handle it separately
				if (!string.IsNullOrWhiteSpace(isoAlpha2Code) && !string.IsNullOrWhiteSpace(isoAlpha3Code))
				{
					// Both codes available - try insert with conflict on alpha-2
					await using var cmd = new NpgsqlCommand(@"
                        INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, geometry)
                        VALUES (@isoAlpha2Code, @isoAlpha3Code, @name, ST_GeomFromGeoJSON(@geometryJson))
                        ON CONFLICT (iso_alpha2_code) DO UPDATE
                        SET iso_alpha3_code = COALESCE(EXCLUDED.iso_alpha3_code, countries.iso_alpha3_code),
                            name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;", connection, transaction);

					cmd.Parameters.AddWithValue("isoAlpha2Code", isoAlpha2Code);
					cmd.Parameters.AddWithValue("isoAlpha3Code", isoAlpha3Code);
					cmd.Parameters.AddWithValue("name", nameLatin);
					cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);

					await cmd.ExecuteNonQueryAsync(cancellationToken);
					processed++;
				}
				else if (!string.IsNullOrWhiteSpace(isoAlpha2Code))
				{
					// Only alpha-2
					await using var cmd = new NpgsqlCommand(@"
                        INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, geometry)
                        VALUES (@isoAlpha2Code, NULL, @name, ST_GeomFromGeoJSON(@geometryJson))
                        ON CONFLICT (iso_alpha2_code) DO UPDATE
                        SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;", connection, transaction);

					cmd.Parameters.AddWithValue("isoAlpha2Code", isoAlpha2Code);
					cmd.Parameters.AddWithValue("name", nameLatin);
					cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);

					await cmd.ExecuteNonQueryAsync(cancellationToken);
					processed++;
				}
				else
				{
					// Only alpha-3
					await using var cmd = new NpgsqlCommand(@"
                        INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, geometry)
                        VALUES (NULL, @isoAlpha3Code, @name, ST_GeomFromGeoJSON(@geometryJson))
                        ON CONFLICT (iso_alpha3_code) DO UPDATE
                        SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;", connection, transaction);

					cmd.Parameters.AddWithValue("isoAlpha3Code", isoAlpha3Code!); // Not null in this branch
					cmd.Parameters.AddWithValue("name", nameLatin);
					cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);

					await cmd.ExecuteNonQueryAsync(cancellationToken);
					processed++;
				}
			}
			catch (PostgresException pgEx) when (pgEx.SqlState == "23505" &&
			                                     pgEx.ConstraintName?.Contains("iso_alpha3") == true)
			{
				// Conflict on alpha-3 when we tried to insert with both codes
				// Update existing record by alpha-3
				if (!string.IsNullOrWhiteSpace(isoAlpha3Code))
				{
					await using var updateCmd = new NpgsqlCommand(@"
                        UPDATE countries
                        SET iso_alpha2_code = COALESCE(@isoAlpha2Code, countries.iso_alpha2_code),
                            name_latin = @name, geometry = ST_GeomFromGeoJSON(@geometryJson)
                        WHERE iso_alpha3_code = @isoAlpha3Code;", connection, transaction);

					if (!string.IsNullOrWhiteSpace(isoAlpha2Code))
						updateCmd.Parameters.AddWithValue("isoAlpha2Code", isoAlpha2Code);
					updateCmd.Parameters.AddWithValue("isoAlpha3Code", isoAlpha3Code);
					updateCmd.Parameters.AddWithValue("name", nameLatin);
					updateCmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);

					await updateCmd.ExecuteNonQueryAsync(cancellationToken);
					processed++;
				}
				else
				{
					skipped++;
					var reason = $"Database error: {pgEx.GetType().Name}";
					skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;
				}
			}
			catch (Exception ex)
			{
				// Skip invalid geometries or other errors
				skipped++;
				var reason = $"Database error: {ex.GetType().Name}";
				skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;
			}
		}

		await transaction.CommitAsync(cancellationToken);

		// Log summary
		_logger?.LogInformation("Countries import: processed={Processed}, skipped={Skipped}", processed, skipped);
		foreach (var reason in skippedReasons)
			_logger?.LogInformation("  Skipped reason '{Reason}': {Count}", reason.Key, reason.Value);
	}

	public async Task ImportRegionsAsync(IEnumerable<Region> regions, CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

		foreach (var region in regions)
		{
			// If we have alpha-2 but not alpha-3, try to get alpha-3 from countries table
			var countryIsoAlpha2Code = region.CountryIsoAlpha2Code;
			var countryIsoAlpha3Code = region.CountryIsoAlpha3Code;

			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
			{
				try
				{
					await using var lookupCmd = new NpgsqlCommand(
						"SELECT iso_alpha3_code FROM countries WHERE iso_alpha2_code = @alpha2Code LIMIT 1;",
						connection, transaction);
					lookupCmd.Parameters.AddWithValue("alpha2Code", countryIsoAlpha2Code);
					var alpha3Result = await lookupCmd.ExecuteScalarAsync(cancellationToken);
					if (alpha3Result != null && alpha3Result != DBNull.Value)
					{
						countryIsoAlpha3Code = alpha3Result.ToString();
						_logger?.LogDebug("Resolved alpha-3 code {Alpha3} for alpha-2 code {Alpha2} from countries table",
							countryIsoAlpha3Code, countryIsoAlpha2Code);
					}
				}
				catch (Exception ex)
				{
					// Log but don't fail - we can still insert with just alpha-2
					_logger?.LogDebug(ex,
						"Failed to lookup alpha-3 code for alpha-2 {Alpha2} from countries table",
						countryIsoAlpha2Code);
				}
			}

			// Build query based on which codes are available
			// Use partial unique indexes to handle NULL values properly
			string insertQuery;
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code) &&
			    !string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				insertQuery = @"
                    INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, @countryAlpha3Code, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                    SET country_iso_alpha3_code = COALESCE(EXCLUDED.country_iso_alpha3_code, regions.country_iso_alpha3_code),
                        name_latin = EXCLUDED.name_latin, 
                        geometry = EXCLUDED.geometry;";
			else if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				insertQuery = @"
                    INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, NULL, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;";
			else if (!string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				insertQuery = @"
                    INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry)
                    VALUES (@identifier, @name, NULL, @countryAlpha3Code, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (identifier, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;";
			else
				throw new InvalidOperationException(
					$"Region '{region.NameLatin}' must have at least one country ISO code (alpha-2 or alpha-3)");

			await using var cmd = new NpgsqlCommand(insertQuery, connection, transaction);

			var wkbWriter = new WKBWriter();
			var wkb = wkbWriter.Write(region.Geometry);

			cmd.Parameters.AddWithValue("identifier", region.Identifier);
			cmd.Parameters.AddWithValue("name", region.NameLatin);
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				cmd.Parameters.AddWithValue("countryAlpha2Code", countryIsoAlpha2Code);
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				cmd.Parameters.AddWithValue("countryAlpha3Code", countryIsoAlpha3Code);
			cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
			await cmd.ExecuteNonQueryAsync(cancellationToken);
		}

		await transaction.CommitAsync(cancellationToken);
	}

	public async Task ImportRegionsFromGeoJsonAsync(string geoJsonContent,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		var jsonDoc = JsonDocument.Parse(geoJsonContent);
		var root = jsonDoc.RootElement;
		var features = root.GetProperty("features");

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
		var processed = 0;
		var skipped = 0;
		var skippedReasons = new Dictionary<string, int>();
		var onlyAlpha3Count = 0;

		foreach (var featureElement in features.EnumerateArray())
		{
			var properties = featureElement.GetProperty("properties");
			var geometryElement = featureElement.GetProperty("geometry");

			// Natural Earth Admin 1 uses fields: name, name_en, admin, adm0_a3, iso_a2, postal, etc.
			// Get country ISO codes (both alpha-2 and alpha-3)
			string? countryIsoAlpha2Code = null;
			string? countryIsoAlpha3Code = null;

			// Try ISO_A2 (uppercase - Natural Earth format)
			if (properties.TryGetProperty("ISO_A2", out var isoA2Upper) && isoA2Upper.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA2Upper.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
					countryIsoAlpha2Code = isoValue.ToUpperInvariant();
			}
			// Try iso_a2 (lowercase - after ogr2ogr conversion)
			else if (properties.TryGetProperty("iso_a2", out var isoA2) && isoA2.ValueKind == JsonValueKind.String)
			{
				var isoValue = isoA2.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
					countryIsoAlpha2Code = isoValue.ToUpperInvariant();
			}
			// Try ADM0_ISO (uppercase) - usually alpha-2
			else if (properties.TryGetProperty("ADM0_ISO", out var adm0IsoUpper) &&
			         adm0IsoUpper.ValueKind == JsonValueKind.String)
			{
				var isoValue = adm0IsoUpper.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99")
				{
					if (isoValue.Length == 2)
						countryIsoAlpha2Code = isoValue.ToUpperInvariant();
					else if (isoValue.Length == 3)
						countryIsoAlpha3Code = isoValue.ToUpperInvariant();
				}
			}
			// Try adm0_iso (lowercase - after ogr2ogr conversion)
			else if (properties.TryGetProperty("adm0_iso", out var adm0Iso) &&
			         adm0Iso.ValueKind == JsonValueKind.String)
			{
				var isoValue = adm0Iso.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99")
				{
					if (isoValue.Length == 2)
						countryIsoAlpha2Code = isoValue.ToUpperInvariant();
					else if (isoValue.Length == 3)
						countryIsoAlpha3Code = isoValue.ToUpperInvariant();
				}
			}

			// Try ADM0_A3 (uppercase) - alpha-3 code
			if (properties.TryGetProperty("ADM0_A3", out var adm0A3Upper) &&
			    adm0A3Upper.ValueKind == JsonValueKind.String)
			{
				var alpha3Value = adm0A3Upper.GetString();
				if (!string.IsNullOrWhiteSpace(alpha3Value) && alpha3Value != "-99" && alpha3Value.Length == 3)
					countryIsoAlpha3Code = alpha3Value.ToUpperInvariant();
			}
			// Try adm0_a3 (lowercase - after ogr2ogr conversion)
			else if (properties.TryGetProperty("adm0_a3", out var adm0A3) && adm0A3.ValueKind == JsonValueKind.String)
			{
				var alpha3Value = adm0A3.GetString();
				if (!string.IsNullOrWhiteSpace(alpha3Value) && alpha3Value != "-99" && alpha3Value.Length == 3)
					countryIsoAlpha3Code = alpha3Value.ToUpperInvariant();
			}

			// At least one ISO code must be present
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
			{
				skipped++;
				// Log first few skipped regions for debugging
				if (skipped <= 10)
				{
					var regionName = properties.TryGetProperty("name", out var nameProp) &&
					                 nameProp.ValueKind == JsonValueKind.String
						? nameProp.GetString()
						: properties.TryGetProperty("name_en", out var nameEnProp) &&
						  nameEnProp.ValueKind == JsonValueKind.String
							? nameEnProp.GetString()
							: "Unknown";
					_logger?.LogDebug("Skipped region '{RegionName}' - missing/invalid country ISO code", regionName);
				}

				continue;
			}

			// If we have alpha-2 but not alpha-3, try to get alpha-3 from countries table
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
			{
				try
				{
					await using var lookupCmd = new NpgsqlCommand(
						"SELECT iso_alpha3_code FROM countries WHERE iso_alpha2_code = @alpha2Code LIMIT 1;",
						connection, transaction);
					lookupCmd.Parameters.AddWithValue("alpha2Code", countryIsoAlpha2Code);
					var alpha3Result = await lookupCmd.ExecuteScalarAsync(cancellationToken);
					if (alpha3Result != null && alpha3Result != DBNull.Value)
					{
						countryIsoAlpha3Code = alpha3Result.ToString();
						_logger?.LogDebug("Resolved alpha-3 code {Alpha3} for alpha-2 code {Alpha2} from countries table",
							countryIsoAlpha3Code, countryIsoAlpha2Code);
					}
				}
				catch (Exception ex)
				{
					// Log but don't fail - we can still insert with just alpha-2
					_logger?.LogDebug(ex,
						"Failed to lookup alpha-3 code for alpha-2 {Alpha2} from countries table",
						countryIsoAlpha2Code);
				}
			}

			// Get region name
			var nameLatin = "Unknown";
			if (properties.TryGetProperty("name_en", out var nameEn) && nameEn.ValueKind == JsonValueKind.String)
				nameLatin = nameEn.GetString() ?? "Unknown";
			else if (properties.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
				nameLatin = name.GetString() ?? "Unknown";
			else if (properties.TryGetProperty("NAME", out var nameUpper) &&
			         nameUpper.ValueKind == JsonValueKind.String)
				nameLatin = nameUpper.GetString() ?? "Unknown";

			if (string.IsNullOrWhiteSpace(nameLatin) || nameLatin == "Unknown")
			{
				skipped++;
				continue;
			}

			// Generate identifier - prefer postal code, then use name + country code
			// Natural Earth Admin 1 may have postal codes or use other identifiers
			string identifier;

			// Try POSTAL (uppercase) first
			if (properties.TryGetProperty("POSTAL", out var postalUpper) &&
			    postalUpper.ValueKind == JsonValueKind.String)
			{
				var postalValue = postalUpper.GetString();
				if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
					identifier = postalValue.ToUpperInvariant();
				else
					identifier = $"{nameLatin}_{countryIsoAlpha2Code ?? countryIsoAlpha3Code}";
			}
			// Try postal (lowercase - after ogr2ogr conversion)
			else if (properties.TryGetProperty("postal", out var postal) && postal.ValueKind == JsonValueKind.String)
			{
				var postalValue = postal.GetString();
				if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
					identifier = postalValue.ToUpperInvariant();
				else
					identifier = $"{nameLatin}_{countryIsoAlpha2Code ?? countryIsoAlpha3Code}";
			}
			// Try POSTAL_CODE (uppercase)
			else if (properties.TryGetProperty("POSTAL_CODE", out var postalCodeUpper) &&
			         postalCodeUpper.ValueKind == JsonValueKind.String)
			{
				var postalValue = postalCodeUpper.GetString();
				if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
					identifier = postalValue.ToUpperInvariant();
				else
					identifier = $"{nameLatin}_{countryIsoAlpha2Code ?? countryIsoAlpha3Code}";
			}
			// Try postal_code (lowercase)
			else if (properties.TryGetProperty("postal_code", out var postalCode) &&
			         postalCode.ValueKind == JsonValueKind.String)
			{
				var postalValue = postalCode.GetString();
				if (!string.IsNullOrWhiteSpace(postalValue) && postalValue != "-99")
					identifier = postalValue.ToUpperInvariant();
				else
					identifier = $"{nameLatin}_{countryIsoAlpha2Code ?? countryIsoAlpha3Code}";
			}
			else
			{
				// Use combination of name and country code as identifier
				identifier = $"{nameLatin}_{countryIsoAlpha2Code ?? countryIsoAlpha3Code}";
			}

			// Normalize identifier (remove special characters, spaces, make safe for database)
			// Remove or replace characters that might cause issues in identifiers
			identifier = Regex.Replace(identifier, @"[^a-zA-Z0-9_]", "_");
			// Collapse multiple underscores into one
			identifier = Regex.Replace(identifier, @"_+", "_");
			// Remove leading/trailing underscores
			identifier = identifier.Trim('_');

			var geometryJson = geometryElement.GetRawText();

			// Build the INSERT query dynamically based on which codes we have
			// Use partial unique indexes to handle NULL values properly
			string insertQuery;
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && !string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
			{
				// Both codes available - use alpha-2 index (both should work, but alpha-2 is primary)
				insertQuery = @"
                    INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, @countryAlpha3Code, ST_GeomFromGeoJSON(@geometryJson))
                ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                    SET country_iso_alpha3_code = COALESCE(EXCLUDED.country_iso_alpha3_code, regions.country_iso_alpha3_code),
                        name_latin = EXCLUDED.name_latin, 
                        geometry = EXCLUDED.geometry;";
			}
			else if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
			{
				// Only alpha-2 (or alpha-3 was looked up from countries table)
				if (!string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				{
					// We have both codes now (alpha-3 was looked up)
					insertQuery = @"
                    INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, @countryAlpha3Code, ST_GeomFromGeoJSON(@geometryJson))
                    ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                    SET country_iso_alpha3_code = COALESCE(EXCLUDED.country_iso_alpha3_code, regions.country_iso_alpha3_code),
                        name_latin = EXCLUDED.name_latin, 
                        geometry = EXCLUDED.geometry;";
				}
				else
				{
					// Still only alpha-2 (lookup failed or not found)
					insertQuery = @"
                    INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, NULL, ST_GeomFromGeoJSON(@geometryJson))
                    ON CONFLICT (identifier, country_iso_alpha2_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;";
				}
			}
			else
			{
				// Only alpha-3
				onlyAlpha3Count++;
				if (onlyAlpha3Count <= 5)
					_logger?.LogInformation(
						"Inserting region with only alpha-3 code: identifier={Identifier}, alpha3={Alpha3}", identifier,
						countryIsoAlpha3Code);
				insertQuery = @"
                    INSERT INTO regions (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry)
                    VALUES (@identifier, @name, NULL, @countryAlpha3Code, ST_GeomFromGeoJSON(@geometryJson))
                    ON CONFLICT (identifier, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, geometry = EXCLUDED.geometry;";
			}

			await using var cmd = new NpgsqlCommand(insertQuery, connection, transaction);

			cmd.Parameters.AddWithValue("identifier", identifier);
			cmd.Parameters.AddWithValue("name", nameLatin);
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				cmd.Parameters.AddWithValue("countryAlpha2Code", countryIsoAlpha2Code);
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				cmd.Parameters.AddWithValue("countryAlpha3Code", countryIsoAlpha3Code);
			cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);

			try
			{
				await cmd.ExecuteNonQueryAsync(cancellationToken);
				processed++;
			}
			catch (PostgresException pgEx)
			{
				// Log database errors (constraint violations, etc.)
				skipped++;
				var reason = $"Database error: {pgEx.SqlState} - {pgEx.Message}";
				if (!skippedReasons.ContainsKey(reason))
					skippedReasons[reason] = 0;
				skippedReasons[reason]++;

				// Log first few errors for debugging
				if (skipped <= 10)
					_logger?.LogWarning(
						"Failed to insert region '{Identifier}' (country codes: alpha2={Alpha2}, alpha3={Alpha3}): {Error}",
						identifier, countryIsoAlpha2Code ?? "NULL", countryIsoAlpha3Code ?? "NULL", pgEx.Message);
			}
			catch (Exception ex)
			{
				// Skip invalid geometries or other errors
				skipped++;
				var reason = $"Error: {ex.GetType().Name}";
				if (!skippedReasons.ContainsKey(reason))
					skippedReasons[reason] = 0;
				skippedReasons[reason]++;

				if (skipped <= 10)
					_logger?.LogWarning(ex,
						"Failed to insert region '{Identifier}' (country codes: alpha2={Alpha2}, alpha3={Alpha3})",
						identifier, countryIsoAlpha2Code ?? "NULL", countryIsoAlpha3Code ?? "NULL");
			}
		}

		await transaction.CommitAsync(cancellationToken);

		// Log summary
		_logger?.LogInformation("Regions import: processed={Processed}, skipped={Skipped}", processed, skipped);
		_logger?.LogInformation("  Regions with only alpha-3 code (NULL in alpha-2): {OnlyAlpha3}", onlyAlpha3Count);
		foreach (var reason in skippedReasons)
			_logger?.LogInformation("  Skipped reason '{Reason}': {Count}", reason.Key, reason.Value);

		// Query to check regions per country (for debugging)
		try
		{
			await using var checkCmd = new NpgsqlCommand(
				@"SELECT COALESCE(country_iso_alpha2_code, country_iso_alpha3_code) as country_code, COUNT(*) as region_count 
                  FROM regions 
                  GROUP BY country_iso_alpha2_code, country_iso_alpha3_code
                  ORDER BY region_count DESC 
                  LIMIT 10;",
				connection);

			await using var reader = await checkCmd.ExecuteReaderAsync(cancellationToken);
			while (await reader.ReadAsync(cancellationToken))
			{
				var countryCode = reader.IsDBNull(0) ? "NULL" : reader.GetString(0);
				var count = reader.GetInt64(1);
				_logger?.LogInformation("  Country {CountryCode}: {Count} regions", countryCode, count);
			}
		}
		catch (Exception ex)
		{
			_logger?.LogWarning(ex, "Failed to query region counts");
		}
	}

	public async Task ImportCitiesAsync(IEnumerable<City> cities, CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

		foreach (var city in cities)
		{
			// Build query based on which codes are available
			string insertQuery;
			if (!string.IsNullOrWhiteSpace(city.CountryIsoAlpha2Code) &&
			    !string.IsNullOrWhiteSpace(city.CountryIsoAlpha3Code))
				insertQuery = @"
                    INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, @countryAlpha3Code, @regionIdentifier, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (identifier, country_iso_alpha2_code, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;";
			else if (!string.IsNullOrWhiteSpace(city.CountryIsoAlpha2Code))
				insertQuery = @"
                    INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, NULL, @regionIdentifier, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (identifier, country_iso_alpha2_code, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;";
			else if (!string.IsNullOrWhiteSpace(city.CountryIsoAlpha3Code))
				insertQuery = @"
                    INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, geometry)
                    VALUES (@identifier, @name, NULL, @countryAlpha3Code, @regionIdentifier, ST_GeomFromWKB(@geometry, 4326))
                    ON CONFLICT (identifier, country_iso_alpha2_code, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;";
			else
				throw new InvalidOperationException(
					$"City '{city.NameLatin}' must have at least one country ISO code (alpha-2 or alpha-3)");

			await using var cmd = new NpgsqlCommand(insertQuery, connection, transaction);

			var wkbWriter = new WKBWriter();
			var wkb = wkbWriter.Write(city.Geometry);

			cmd.Parameters.AddWithValue("identifier", city.Identifier);
			cmd.Parameters.AddWithValue("name", city.NameLatin);
			if (!string.IsNullOrWhiteSpace(city.CountryIsoAlpha2Code))
				cmd.Parameters.AddWithValue("countryAlpha2Code", city.CountryIsoAlpha2Code);
			if (!string.IsNullOrWhiteSpace(city.CountryIsoAlpha3Code))
				cmd.Parameters.AddWithValue("countryAlpha3Code", city.CountryIsoAlpha3Code);
			cmd.Parameters.AddWithValue("regionIdentifier", city.RegionIdentifier ?? (object) DBNull.Value);
			cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
			await cmd.ExecuteNonQueryAsync(cancellationToken);
		}

		await transaction.CommitAsync(cancellationToken);
	}

	public async Task ImportCitiesFromGeoJsonAsync(string geoJsonContent, CancellationToken cancellationToken = default)
	{
		await ImportCitiesFromGeoJsonAsync(geoJsonContent, null, cancellationToken);
	}

	/// <summary>
	///     Imports city geometries from GeoJSON into the <c>cities</c> table.
	///     Allows specifying a default country ISO alpha-2 code which will be used
	///     when the source data does not contain an explicit country code.
	/// </summary>
	public async Task ImportCitiesFromGeoJsonAsync(string geoJsonContent, string? defaultCountryIsoAlpha2Code,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		var jsonDoc = JsonDocument.Parse(geoJsonContent);
		var root = jsonDoc.RootElement;
		var features = root.GetProperty("features");

		// Count total features for progress logging
		var totalFeatures = features.GetArrayLength();
		_logger?.LogInformation("Starting import of {TotalCount} city features from GeoJSON", totalFeatures);

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
		var processed = 0;
		var skipped = 0;
		var skippedReasons = new Dictionary<string, int>();
		var lastLogTime = DateTime.UtcNow;
		const int logIntervalSeconds = 10; // Log progress every 10 seconds
		const int logIntervalCount = 100; // Or every 100 cities

		foreach (var featureElement in features.EnumerateArray())
		{
			var properties = featureElement.GetProperty("properties");
			var geometryElement = featureElement.GetProperty("geometry");

			// Support both Natural Earth and OSM formats
			// Natural Earth: ISO_A2, iso_a2, ADM0_ISO, adm0_iso
			// OSM: ISO3166-1:alpha2, ISO3166-1:alpha3, ISO3166-1, addr:country
			// Get country ISO codes (both alpha-2 and alpha-3)
			string? countryIsoAlpha2Code = null;
			string? countryIsoAlpha3Code = null;

			// Try OSM format first: ISO3166-1:alpha2
			if (properties.TryGetProperty("ISO3166-1:alpha2", out var osmIsoA2) &&
			    osmIsoA2.ValueKind == JsonValueKind.String)
			{
				var isoValue = osmIsoA2.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue.Length == 2)
					countryIsoAlpha2Code = isoValue.ToUpperInvariant();
			}

			// Try OSM format: ISO3166-1:alpha3
			if (properties.TryGetProperty("ISO3166-1:alpha3", out var osmIsoA3) &&
			    osmIsoA3.ValueKind == JsonValueKind.String)
			{
				var isoValue = osmIsoA3.GetString();
				if (!string.IsNullOrWhiteSpace(isoValue) && isoValue.Length == 3)
					countryIsoAlpha3Code = isoValue.ToUpperInvariant();
			}

			// Try OSM format: ISO3166-1 (can be either alpha-2 or alpha-3)
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				if (properties.TryGetProperty("ISO3166-1", out var osmIso) && osmIso.ValueKind == JsonValueKind.String)
				{
					var isoValue = osmIso.GetString();
					if (!string.IsNullOrWhiteSpace(isoValue))
					{
						if (isoValue.Length == 2)
							countryIsoAlpha2Code = isoValue.ToUpperInvariant();
						else if (isoValue.Length == 3)
							countryIsoAlpha3Code = isoValue.ToUpperInvariant();
					}
				}

			// Try OSM format: addr:country (usually alpha-2)
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				if (properties.TryGetProperty("addr:country", out var addrCountry) &&
				    addrCountry.ValueKind == JsonValueKind.String)
				{
					var isoValue = addrCountry.GetString();
					if (!string.IsNullOrWhiteSpace(isoValue) && isoValue.Length == 2)
						countryIsoAlpha2Code = isoValue.ToUpperInvariant();
				}

			// Try Natural Earth format: ISO_A2 (uppercase)
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				if (properties.TryGetProperty("ISO_A2", out var isoA2Upper) &&
				    isoA2Upper.ValueKind == JsonValueKind.String)
				{
					var isoValue = isoA2Upper.GetString();
					if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
						countryIsoAlpha2Code = isoValue.ToUpperInvariant();
				}

			// Try iso_a2 (lowercase - after ogr2ogr conversion)
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				if (properties.TryGetProperty("iso_a2", out var isoA2) && isoA2.ValueKind == JsonValueKind.String)
				{
					var isoValue = isoA2.GetString();
					if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99" && isoValue.Length == 2)
						countryIsoAlpha2Code = isoValue.ToUpperInvariant();
				}

			// Try ADM0_ISO (uppercase) - usually alpha-2, but could be alpha-3
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				if (properties.TryGetProperty("ADM0_ISO", out var adm0IsoUpper) &&
				    adm0IsoUpper.ValueKind == JsonValueKind.String)
				{
					var isoValue = adm0IsoUpper.GetString();
					if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99")
					{
						if (isoValue.Length == 2)
							countryIsoAlpha2Code = isoValue.ToUpperInvariant();
						else if (isoValue.Length == 3)
							countryIsoAlpha3Code = isoValue.ToUpperInvariant();
					}
				}

			// Try adm0_iso (lowercase - after ogr2ogr conversion)
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				if (properties.TryGetProperty("adm0_iso", out var adm0Iso) && adm0Iso.ValueKind == JsonValueKind.String)
				{
					var isoValue = adm0Iso.GetString();
					if (!string.IsNullOrWhiteSpace(isoValue) && isoValue != "-99")
					{
						if (isoValue.Length == 2)
							countryIsoAlpha2Code = isoValue.ToUpperInvariant();
						else if (isoValue.Length == 3)
							countryIsoAlpha3Code = isoValue.ToUpperInvariant();
					}
				}

			// Try ADM0_A3 (uppercase) - alpha-3 code
			if (properties.TryGetProperty("ADM0_A3", out var adm0A3Upper) &&
			    adm0A3Upper.ValueKind == JsonValueKind.String)
			{
				var alpha3Value = adm0A3Upper.GetString();
				if (!string.IsNullOrWhiteSpace(alpha3Value) && alpha3Value != "-99" && alpha3Value.Length == 3)
					countryIsoAlpha3Code = alpha3Value.ToUpperInvariant();
			}
			// Try adm0_a3 (lowercase - after ogr2ogr conversion)
			else if (properties.TryGetProperty("adm0_a3", out var adm0A3) && adm0A3.ValueKind == JsonValueKind.String)
			{
				var alpha3Value = adm0A3.GetString();
				if (!string.IsNullOrWhiteSpace(alpha3Value) && alpha3Value != "-99" && alpha3Value.Length == 3)
					countryIsoAlpha3Code = alpha3Value.ToUpperInvariant();
			}

			// At least one ISO code must be present
			if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
			{
				if (!string.IsNullOrWhiteSpace(defaultCountryIsoAlpha2Code))
				{
					countryIsoAlpha2Code = defaultCountryIsoAlpha2Code.ToUpperInvariant();
				}
				else
				{
					skipped++;
					var reason = "Missing or invalid country ISO code";
					skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;

					// Log first few skipped cities for debugging
					if (skipped <= 10)
					{
						var cityName = properties.TryGetProperty("name", out var nameProp) &&
						               nameProp.ValueKind == JsonValueKind.String
							? nameProp.GetString()
							: properties.TryGetProperty("nameascii", out var nameAsciiProp) &&
							  nameAsciiProp.ValueKind == JsonValueKind.String
								? nameAsciiProp.GetString()
								: "Unknown";
						_logger?.LogDebug("Skipped city '{CityName}' - missing/invalid country ISO code", cityName);
					}

					continue;
				}
			}

			// Get city name - prioritize ASCII/English names to avoid Cyrillic and other non-Latin characters
			// OSM/Geofabrik shapefile fields: nameascii (ASCII transliteration), name_en (English), name (original, can be any language)
			var nameLatin = "Unknown";
			if (properties.TryGetProperty("NAMEASCII", out var nameAsciiUpper) &&
			    nameAsciiUpper.ValueKind == JsonValueKind.String)
			{
				var value = nameAsciiUpper.GetString();
				if (!string.IsNullOrWhiteSpace(value))
					nameLatin = value;
			}

			if ((nameLatin == "Unknown" || string.IsNullOrWhiteSpace(nameLatin)) &&
			    properties.TryGetProperty("nameascii", out var nameAscii) &&
			    nameAscii.ValueKind == JsonValueKind.String)
			{
				var value = nameAscii.GetString();
				if (!string.IsNullOrWhiteSpace(value))
					nameLatin = value;
			}

			// Fallback to English name if ASCII not available
			if ((nameLatin == "Unknown" || string.IsNullOrWhiteSpace(nameLatin)) &&
			    properties.TryGetProperty("name_en", out var nameEn) &&
			    nameEn.ValueKind == JsonValueKind.String)
			{
				var value = nameEn.GetString();
				if (!string.IsNullOrWhiteSpace(value))
					nameLatin = value;
			}

			// Last resort: use original name, but only if it contains Latin characters
			// This avoids importing Cyrillic, Arabic, Chinese, etc. names into name_latin field
			if ((nameLatin == "Unknown" || string.IsNullOrWhiteSpace(nameLatin)) &&
			    properties.TryGetProperty("NAME", out var nameUpper) &&
			    nameUpper.ValueKind == JsonValueKind.String)
			{
				var value = nameUpper.GetString();
				if (!string.IsNullOrWhiteSpace(value) && ContainsLatinCharacters(value))
					nameLatin = value;
			}

			if ((nameLatin == "Unknown" || string.IsNullOrWhiteSpace(nameLatin)) &&
			    properties.TryGetProperty("name", out var name) && name.ValueKind == JsonValueKind.String)
			{
				var value = name.GetString();
				if (!string.IsNullOrWhiteSpace(value) && ContainsLatinCharacters(value))
					nameLatin = value;
			}

			// If no Latin name found, use original name (will be transliterated in post-processing)
			// This ensures we import all cities from shapefile, even if they don't have Latin names yet
			if ((nameLatin == "Unknown" || string.IsNullOrWhiteSpace(nameLatin)) &&
			    properties.TryGetProperty("name", out var originalName) && originalName.ValueKind == JsonValueKind.String)
			{
				var originalValue = originalName.GetString();
				if (!string.IsNullOrWhiteSpace(originalValue))
					nameLatin = originalValue; // Use original name, will be transliterated later
			}

			// Skip only if absolutely no name is available
			if (string.IsNullOrWhiteSpace(nameLatin) || nameLatin == "Unknown")
			{
				skipped++;
				var reason = "Missing city name";
				skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;
				continue;
			}

			// Get geometry JSON
			var geometryJson = geometryElement.GetRawText();

			// Get region identifier (admin level 1) - optional
			// Only get from OSM/Natural Earth properties (spatial join will be done in post-processing)
			// Support both Natural Earth and OSM formats for region identifier
			// OSM: is_in:state, addr:state, is_in:province
			// Natural Earth: ADM1NAME, adm1name
			string? regionIdentifier = null;
			if (properties.TryGetProperty("is_in:state", out var isInState) &&
			    isInState.ValueKind == JsonValueKind.String)
			{
				var regionValue = isInState.GetString();
				if (!string.IsNullOrWhiteSpace(regionValue))
				{
					regionIdentifier = Regex.Replace(regionValue, @"[^a-zA-Z0-9_]", "_");
					regionIdentifier = Regex.Replace(regionIdentifier, @"_+", "_");
					regionIdentifier = regionIdentifier.Trim('_');
				}
			}
			else if (properties.TryGetProperty("addr:state", out var addrState) &&
			         addrState.ValueKind == JsonValueKind.String)
			{
				var regionValue = addrState.GetString();
				if (!string.IsNullOrWhiteSpace(regionValue))
				{
					regionIdentifier = Regex.Replace(regionValue, @"[^a-zA-Z0-9_]", "_");
					regionIdentifier = Regex.Replace(regionIdentifier, @"_+", "_");
					regionIdentifier = regionIdentifier.Trim('_');
				}
			}
			else if (properties.TryGetProperty("is_in:province", out var isInProvince) &&
			         isInProvince.ValueKind == JsonValueKind.String)
			{
				var regionValue = isInProvince.GetString();
				if (!string.IsNullOrWhiteSpace(regionValue))
				{
					regionIdentifier = Regex.Replace(regionValue, @"[^a-zA-Z0-9_]", "_");
					regionIdentifier = Regex.Replace(regionIdentifier, @"_+", "_");
					regionIdentifier = regionIdentifier.Trim('_');
				}
			}
			else if (properties.TryGetProperty("ADM1NAME", out var adm1NameUpper) &&
			         adm1NameUpper.ValueKind == JsonValueKind.String)
			{
				var adm1Value = adm1NameUpper.GetString();
				if (!string.IsNullOrWhiteSpace(adm1Value))
				{
					regionIdentifier = Regex.Replace(adm1Value, @"[^a-zA-Z0-9_]", "_");
					regionIdentifier = Regex.Replace(regionIdentifier, @"_+", "_");
					regionIdentifier = regionIdentifier.Trim('_');
				}
			}
			else if (properties.TryGetProperty("adm1name", out var adm1Name) &&
			         adm1Name.ValueKind == JsonValueKind.String)
			{
				var adm1Value = adm1Name.GetString();
				if (!string.IsNullOrWhiteSpace(adm1Value))
				{
					regionIdentifier = Regex.Replace(adm1Value, @"[^a-zA-Z0-9_]", "_");
					regionIdentifier = Regex.Replace(regionIdentifier, @"_+", "_");
					regionIdentifier = regionIdentifier.Trim('_');
				}
			}
			// Note: Spatial join for region_identifier is done in post-processing

			// Generate city identifier - use nameascii + country code, or geonames_id if available
			string identifier;
			if (properties.TryGetProperty("GEONAMEID", out var geonameIdUpper) &&
			    geonameIdUpper.ValueKind == JsonValueKind.Number)
				identifier = geonameIdUpper.GetInt64().ToString();
			else if (properties.TryGetProperty("geonameid", out var geonameId) &&
			         geonameId.ValueKind == JsonValueKind.Number)
				identifier = geonameId.GetInt64().ToString();
			else if (properties.TryGetProperty("GN_ID", out var gnId) && gnId.ValueKind == JsonValueKind.Number)
				identifier = gnId.GetInt64().ToString();
			else
				// Use combination of name and country code as identifier
				identifier = $"{nameLatin}_{countryIsoAlpha2Code ?? countryIsoAlpha3Code}";

			// Normalize identifier
			identifier = Regex.Replace(identifier, @"[^a-zA-Z0-9_]", "_");
			identifier = Regex.Replace(identifier, @"_+", "_");
			identifier = identifier.Trim('_');

			// Build the INSERT query dynamically based on which codes we have
			string insertQuery;
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code) && !string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				// Both codes available
				insertQuery = @"
                    INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, @countryAlpha3Code, @regionIdentifier, ST_GeomFromGeoJSON(@geometryJson))
                    ON CONFLICT (identifier, country_iso_alpha2_code, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;";
			else if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				// Only alpha-2
				insertQuery = @"
                    INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, geometry)
                    VALUES (@identifier, @name, @countryAlpha2Code, NULL, @regionIdentifier, ST_GeomFromGeoJSON(@geometryJson))
                    ON CONFLICT (identifier, country_iso_alpha2_code, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;";
			else
				// Only alpha-3
				insertQuery = @"
                    INSERT INTO cities (identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, geometry)
                    VALUES (@identifier, @name, NULL, @countryAlpha3Code, @regionIdentifier, ST_GeomFromGeoJSON(@geometryJson))
                    ON CONFLICT (identifier, country_iso_alpha2_code, country_iso_alpha3_code) DO UPDATE
                    SET name_latin = EXCLUDED.name_latin, region_identifier = EXCLUDED.region_identifier, geometry = EXCLUDED.geometry;";

			await using var cmd = new NpgsqlCommand(insertQuery, connection, transaction);

			cmd.Parameters.AddWithValue("identifier", identifier);
			cmd.Parameters.AddWithValue("name", nameLatin);
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha2Code))
				cmd.Parameters.AddWithValue("countryAlpha2Code", countryIsoAlpha2Code);
			if (!string.IsNullOrWhiteSpace(countryIsoAlpha3Code))
				cmd.Parameters.AddWithValue("countryAlpha3Code", countryIsoAlpha3Code);
			cmd.Parameters.AddWithValue("regionIdentifier", regionIdentifier ?? (object) DBNull.Value);
			cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);

			try
			{
				await cmd.ExecuteNonQueryAsync(cancellationToken);
				processed++;
				
				// Log progress periodically
				var now = DateTime.UtcNow;
				if (processed % logIntervalCount == 0 || (now - lastLogTime).TotalSeconds >= logIntervalSeconds)
				{
					var progressPercent = totalFeatures > 0 ? (processed * 100.0 / totalFeatures) : 0;
					_logger?.LogInformation(
						"City import progress: {Processed}/{Total} ({ProgressPercent:F1}%), skipped: {Skipped}",
						processed, totalFeatures, progressPercent, skipped);
					lastLogTime = now;
				}
			}
			catch (Exception ex)
			{
				// Skip invalid geometries
				skipped++;
				var reason = $"Database error: {ex.GetType().Name}";
				skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;
			}
		}

		await transaction.CommitAsync(cancellationToken);

		// Log summary
		_logger?.LogInformation("Cities import: processed={Processed}, skipped={Skipped}", processed, skipped);
		foreach (var reason in skippedReasons)
			_logger?.LogInformation("  Skipped reason '{Reason}': {Count}", reason.Key, reason.Value);

		// Post-processing: fill missing region_identifier and transliterate names
		if (processed > 0)
		{
			_logger?.LogInformation("Starting post-processing: updating region_identifier and transliterating names");
			await PostProcessCitiesAsync(cancellationToken);
		}
	}

	public async Task ImportTimezonesAsync(IEnumerable<Timezone> timezones,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

		foreach (var timezone in timezones)
		{
			await using var cmd = new NpgsqlCommand(@"
                INSERT INTO timezones (timezone_id, geometry)
                VALUES (@timezoneId, ST_GeomFromWKB(@geometry, 4326))
                ON CONFLICT (timezone_id) DO UPDATE
                SET geometry = EXCLUDED.geometry;", connection, transaction);

			var wkbWriter = new WKBWriter();
			var wkb = wkbWriter.Write(timezone.Geometry);

			cmd.Parameters.AddWithValue("timezoneId", timezone.TimezoneId);
			cmd.Parameters.AddWithValue("geometry", NpgsqlDbType.Bytea, wkb);
			await cmd.ExecuteNonQueryAsync(cancellationToken);
		}

		await transaction.CommitAsync(cancellationToken);
	}

	public async Task ImportTimezonesFromGeoJsonAsync(string geoJsonContent,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		var jsonDoc = JsonDocument.Parse(geoJsonContent);
		var root = jsonDoc.RootElement;
		var features = root.GetProperty("features");

		await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
		var processed = 0;
		var skipped = 0;
		var skippedReasons = new Dictionary<string, int>();

		foreach (var featureElement in features.EnumerateArray())
		{
			var properties = featureElement.GetProperty("properties");
			var geometryElement = featureElement.GetProperty("geometry");

			// Timezone Boundary Builder uses "tzid" property for IANA timezone ID
			string? timezoneId = null;
			if (properties.TryGetProperty("tzid", out var tzid) && tzid.ValueKind == JsonValueKind.String)
				timezoneId = tzid.GetString();
			else if (properties.TryGetProperty("TZID", out var tzidUpper) &&
			         tzidUpper.ValueKind == JsonValueKind.String)
				timezoneId = tzidUpper.GetString();
			else if (properties.TryGetProperty("timezone", out var timezone) &&
			         timezone.ValueKind == JsonValueKind.String)
				timezoneId = timezone.GetString();
			else if (properties.TryGetProperty("TIMEZONE", out var timezoneUpper) &&
			         timezoneUpper.ValueKind == JsonValueKind.String) timezoneId = timezoneUpper.GetString();

			if (string.IsNullOrWhiteSpace(timezoneId))
			{
				skipped++;
				var reason = "Missing or invalid timezone ID";
				skippedReasons[reason] = skippedReasons.GetValueOrDefault(reason, 0) + 1;
				continue;
			}

			var geometryJson = geometryElement.GetRawText();

			try
			{
				await using var cmd = new NpgsqlCommand(@"
                    INSERT INTO timezones (timezone_id, geometry)
                    VALUES (@timezoneId, ST_GeomFromGeoJSON(@geometryJson))
                    ON CONFLICT (timezone_id) DO UPDATE
                    SET geometry = EXCLUDED.geometry;", connection, transaction);

				cmd.Parameters.AddWithValue("timezoneId", timezoneId);
				cmd.Parameters.AddWithValue("geometryJson", NpgsqlDbType.Jsonb, geometryJson);

				await cmd.ExecuteNonQueryAsync(cancellationToken);
				processed++;
			}
			catch (PostgresException pgEx)
			{
				skipped++;
				var reason = $"Database error: {pgEx.SqlState} - {pgEx.Message}";
				if (!skippedReasons.ContainsKey(reason))
					skippedReasons[reason] = 0;
				skippedReasons[reason]++;

				if (skipped <= 10)
					_logger?.LogWarning("Failed to insert timezone '{TimezoneId}': {Error}", timezoneId, pgEx.Message);
			}
			catch (Exception ex)
			{
				skipped++;
				var reason = $"Error: {ex.GetType().Name}";
				if (!skippedReasons.ContainsKey(reason))
					skippedReasons[reason] = 0;
				skippedReasons[reason]++;

				if (skipped <= 10) _logger?.LogWarning(ex, "Failed to insert timezone '{TimezoneId}'", timezoneId);
			}
		}

		await transaction.CommitAsync(cancellationToken);

		_logger?.LogInformation("Timezones import: processed={Processed}, skipped={Skipped}", processed, skipped);
		foreach (var reason in skippedReasons)
			_logger?.LogInformation("  Skipped reason '{Reason}': {Count}", reason.Key, reason.Value);
	}

	public async Task<Country?> FindCountryByPointAsync(double latitude, double longitude,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		var point = $"POINT({longitude} {latitude})";
		await using var cmd = new NpgsqlCommand(@"
            SELECT id, iso_alpha2_code, iso_alpha3_code, name_latin, geometry
            FROM countries
            WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
            LIMIT 1;", connection);

		cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);

		await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
			return new Country
			{
				Id = reader.GetInt32(0),
				IsoAlpha2Code = reader.IsDBNull(1) ? null : reader.GetString(1),
				IsoAlpha3Code = reader.IsDBNull(2) ? null : reader.GetString(2),
				NameLatin = reader.GetString(3),
				Geometry = (Geometry) reader.GetValue(4)
			};

		return null;
	}

	public async Task<Region?> FindRegionByPointAsync(double latitude, double longitude,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		var point = $"POINT({longitude} {latitude})";
		await using var cmd = new NpgsqlCommand(@"
            SELECT id, identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, geometry
            FROM regions
            WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
            LIMIT 1;", connection);

		cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);

		await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
			return new Region
			{
				Id = reader.GetInt32(0),
				Identifier = reader.GetString(1),
				NameLatin = reader.GetString(2),
				CountryIsoAlpha2Code = reader.IsDBNull(3) ? null : reader.GetString(3),
				CountryIsoAlpha3Code = reader.IsDBNull(4) ? null : reader.GetString(4),
				Geometry = (Geometry) reader.GetValue(5)
			};

		return null;
	}

	public async Task<City?> FindCityByPointAsync(double latitude, double longitude,
		CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		var point = $"POINT({longitude} {latitude})";
		// Cities use MULTIPOLYGON geometries (city boundaries), so we use ST_Contains to check if point is within the boundary
		await using var cmd = new NpgsqlCommand(@"
            SELECT id, identifier, name_latin, country_iso_alpha2_code, country_iso_alpha3_code, region_identifier, geometry
            FROM cities
            WHERE ST_Contains(geometry, ST_GeomFromText(@point, 4326))
            LIMIT 1;", connection);

		cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);

		await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
			return new City
			{
				Id = reader.GetInt32(0),
				Identifier = reader.GetString(1),
				NameLatin = reader.GetString(2),
				CountryIsoAlpha2Code = reader.IsDBNull(3) ? null : reader.GetString(3),
				CountryIsoAlpha3Code = reader.IsDBNull(4) ? null : reader.GetString(4),
				RegionIdentifier = reader.IsDBNull(5) ? null : reader.GetString(5),
				Geometry = (Geometry) reader.GetValue(6)
			};

		return null;
	}

	public async Task<(int RawOffset, int DstOffset)?> GetTimezoneOffsetAsync(double latitude, double longitude,
		CancellationToken cancellationToken = default)
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
		var approximateOffsetHours = (int) Math.Round(longitude / 15.0);
		var approximateOffsetSeconds = approximateOffsetHours * 3600;
		return (approximateOffsetSeconds, 0);
	}

	private static bool ContainsLatinCharacters(string text)
	{
		// Check if string contains at least one Latin character (A-Z, a-z)
		// This helps filter out names in Cyrillic, Arabic, Chinese, etc.
		return text.Any(c => (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'));
	}

	private static string? TransliterateToLatin(string text)
	{
		if (string.IsNullOrWhiteSpace(text))
			return null;

		try
		{
			// Try common transliteration rules
			// ICU supports various transliteration IDs like "Cyrillic-Latin", "Any-Latin", etc.
			// "Any-Latin; Latin-ASCII" attempts to transliterate any script to Latin, then to ASCII
			var transliterator = Transliterator.GetInstance("Any-Latin; Latin-ASCII");
			return transliterator.Transliterate(text);
		}
		catch
		{
			// If ICU transliteration fails, return null
			return null;
		}
	}

	/// <summary>
	///     Post-processes imported cities: fills missing region_identifier using spatial queries
	///     and transliterates non-Latin names to Latin.
	/// </summary>
	private async Task PostProcessCitiesAsync(CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		_logger?.LogInformation("Post-processing cities: updating region_identifier and transliterating names");

		// Step 1: Update region_identifier for cities where it's NULL using spatial queries
		var regionUpdateCount = 0;
		try
		{
			// Update cities with NULL region_identifier by finding matching regions via spatial join
			// Use DISTINCT ON to handle cases where multiple regions might contain the same city
			// We'll update each city only once with the first matching region
			await using var regionUpdateCmd = new NpgsqlCommand(@"
                UPDATE cities c
                SET region_identifier = sub.identifier
                FROM (
                  SELECT DISTINCT ON (c.id) c.id, r.identifier
                  FROM cities c
                  INNER JOIN regions r ON (
                    (c.country_iso_alpha2_code IS NOT NULL AND r.country_iso_alpha2_code = c.country_iso_alpha2_code)
                    OR (c.country_iso_alpha3_code IS NOT NULL AND r.country_iso_alpha3_code = c.country_iso_alpha3_code)
                  )
                  WHERE c.region_identifier IS NULL
                    AND ST_Contains(r.geometry, ST_Centroid(c.geometry))
                  ORDER BY c.id, r.id
                ) sub
                WHERE c.id = sub.id;", connection);
			regionUpdateCmd.CommandTimeout = 300; // 5 minutes timeout for bulk update
			regionUpdateCount = await regionUpdateCmd.ExecuteNonQueryAsync(cancellationToken);
			_logger?.LogInformation("Updated region_identifier for {Count} cities using spatial queries", regionUpdateCount);
		}
		catch (Exception ex)
		{
			_logger?.LogWarning(ex, "Failed to update region_identifier for some cities");
		}

		// Step 2: Transliterate non-Latin names to Latin
		var transliterationCount = 0;
		try
		{
			// Get cities with non-Latin names (or names that need transliteration)
			await using var selectCmd = new NpgsqlCommand(@"
                SELECT id, name_latin, country_iso_alpha2_code, country_iso_alpha3_code
                FROM cities
                WHERE name_latin IS NOT NULL
                  AND NOT (name_latin ~ '^[A-Za-z0-9\s\-\.\']+$') -- Contains non-Latin characters
                LIMIT 10000;", connection); // Process in batches to avoid memory issues
			selectCmd.CommandTimeout = 60;

			await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);
			var citiesToUpdate = new List<(int Id, string OriginalName, string? CountryAlpha2, string? CountryAlpha3)>();

			while (await reader.ReadAsync(cancellationToken))
			{
				var id = reader.GetInt32(0);
				var originalName = reader.GetString(1);
				var countryAlpha2 = reader.IsDBNull(2) ? null : reader.GetString(2);
				var countryAlpha3 = reader.IsDBNull(3) ? null : reader.GetString(3);
				citiesToUpdate.Add((id, originalName, countryAlpha2, countryAlpha3));
			}

			_logger?.LogInformation("Found {Count} cities with non-Latin names to transliterate", citiesToUpdate.Count);

			// Update cities with transliterated names
			if (citiesToUpdate.Count > 0)
			{
				await using var updateTransaction = await connection.BeginTransactionAsync(cancellationToken);
				try
				{
					foreach (var (id, originalName, countryAlpha2, countryAlpha3) in citiesToUpdate)
					{
						var transliterated = TransliterateToLatin(originalName);
						if (!string.IsNullOrWhiteSpace(transliterated) && ContainsLatinCharacters(transliterated))
						{
							await using var updateCmd = new NpgsqlCommand(@"
                                UPDATE cities
                                SET name_latin = @transliteratedName
                                WHERE id = @cityId;", connection, updateTransaction);
							updateCmd.Parameters.AddWithValue("transliteratedName", transliterated);
							updateCmd.Parameters.AddWithValue("cityId", id);
							await updateCmd.ExecuteNonQueryAsync(cancellationToken);
							transliterationCount++;

							if (transliterationCount % 100 == 0)
							{
								_logger?.LogInformation("Transliterated {Count} city names...", transliterationCount);
							}
						}
					}

					await updateTransaction.CommitAsync(cancellationToken);
					_logger?.LogInformation("Transliterated {Count} city names to Latin", transliterationCount);
				}
				catch (Exception ex)
				{
					await updateTransaction.RollbackAsync(cancellationToken);
					_logger?.LogError(ex, "Failed to transliterate city names, transaction rolled back");
				}
			}
		}
		catch (Exception ex)
		{
			_logger?.LogWarning(ex, "Failed to transliterate some city names");
		}

		_logger?.LogInformation("Post-processing completed: updated {RegionCount} region_identifiers, transliterated {TransliterationCount} names",
			regionUpdateCount, transliterationCount);
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
				dstOffset = timeZoneInfo.GetUtcOffset(localTime) - baseOffset;

			return ((int) baseOffset.TotalSeconds, (int) dstOffset.TotalSeconds);
		}
		catch
		{
			// Fallback: try parsing IANA timezone or return UTC
			return (0, 0);
		}
	}

	public async Task<DateTimeOffset?> GetLastUpdateTimeAsync(CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		await using var cmd = new NpgsqlCommand(
			"SELECT updated_at FROM last_update WHERE id = 1;",
			connection);

		var result = await cmd.ExecuteScalarAsync(cancellationToken);
		if (result == null || result == DBNull.Value) return null;

		if (result is DateTime dateTime) return new DateTimeOffset(dateTime, TimeSpan.Zero);

		return null;
	}
}