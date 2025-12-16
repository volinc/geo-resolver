using Microsoft.Extensions.Logging;
using Npgsql;

namespace GeoResolver.DataUpdater.Services;

/// <summary>
///     Post-processes imported cities: fills missing region_identifier using spatial queries
///     and transliterates non-Latin names to Latin.
/// </summary>
public sealed class CityPostProcessor : ICityPostProcessor
{
	private readonly ITransliterationService _transliterationService;
	private readonly ILogger<CityPostProcessor>? _logger;
	private readonly NpgsqlDataSource _npgsqlDataSource;

	public CityPostProcessor(NpgsqlDataSource npgsqlDataSource, ITransliterationService transliterationService,
		ILogger<CityPostProcessor>? logger = null)
	{
		_npgsqlDataSource = npgsqlDataSource;
		_transliterationService = transliterationService;
		_logger = logger;
	}

	/// <summary>
	///     Post-processes imported cities: fills missing region_identifier using spatial queries
	///     and transliterates non-Latin names to Latin.
	/// </summary>
	public async Task PostProcessCitiesAsync(CancellationToken cancellationToken = default)
	{
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		_logger?.LogInformation("Post-processing cities: updating region_identifier and transliterating names");

		// Step 1: Update region_identifier for cities where it's NULL using spatial queries
		var regionUpdateCount = 0;
		try
		{
			// Stage 1: Use ST_Contains with centroid (strictest match)
			await using var regionUpdateCmd1 = new NpgsqlCommand(@"
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
			regionUpdateCmd1.CommandTimeout = 300;
			var count1 = await regionUpdateCmd1.ExecuteNonQueryAsync(cancellationToken);
			regionUpdateCount += count1;
			_logger?.LogInformation("Stage 1: Updated region_identifier for {Count} cities using ST_Contains (centroid)", count1);
			
			// Stage 2: Use ST_Intersects with city geometry (more lenient, for cities on borders)
			await using var regionUpdateCmd2 = new NpgsqlCommand(@"
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
                    AND ST_Intersects(r.geometry, c.geometry)
                  ORDER BY c.id, r.id
                ) sub
                WHERE c.id = sub.id;", connection);
			regionUpdateCmd2.CommandTimeout = 300;
			var count2 = await regionUpdateCmd2.ExecuteNonQueryAsync(cancellationToken);
			regionUpdateCount += count2;
			_logger?.LogInformation("Stage 2: Updated region_identifier for {Count} cities using ST_Intersects (geometry)", count2);
			
			_logger?.LogInformation("Total updated region_identifier for {Count} cities using spatial queries", regionUpdateCount);
			
			// Check for cities that found a region but from a different country (should not happen, but verify)
			await using var wrongCountryCmd = new NpgsqlCommand(@"
                SELECT c.id, c.name_latin, c.country_iso_alpha2_code, c.country_iso_alpha3_code, c.region_identifier,
                       r.country_iso_alpha2_code, r.country_iso_alpha3_code
                FROM cities c
                INNER JOIN regions r ON c.region_identifier = r.identifier
                WHERE c.region_identifier IS NOT NULL
                  AND (
                    (c.country_iso_alpha2_code IS NOT NULL AND r.country_iso_alpha2_code IS NOT NULL AND c.country_iso_alpha2_code != r.country_iso_alpha2_code)
                    OR (c.country_iso_alpha3_code IS NOT NULL AND r.country_iso_alpha3_code IS NOT NULL AND c.country_iso_alpha3_code != r.country_iso_alpha3_code)
                    OR (c.country_iso_alpha2_code IS NOT NULL AND r.country_iso_alpha2_code IS NULL AND r.country_iso_alpha3_code IS NOT NULL AND c.country_iso_alpha2_code != (SELECT iso_alpha2_code FROM countries WHERE iso_alpha3_code = r.country_iso_alpha3_code LIMIT 1))
                    OR (c.country_iso_alpha3_code IS NOT NULL AND r.country_iso_alpha3_code IS NULL AND r.country_iso_alpha2_code IS NOT NULL AND c.country_iso_alpha3_code != (SELECT iso_alpha3_code FROM countries WHERE iso_alpha2_code = r.country_iso_alpha2_code LIMIT 1))
                  )
                LIMIT 20;", connection);
			await using var wrongCountryReader = await wrongCountryCmd.ExecuteReaderAsync(cancellationToken);
			var wrongCountryCities = new List<string>();
			while (await wrongCountryReader.ReadAsync(cancellationToken))
			{
				var id = wrongCountryReader.GetInt32(0);
				var name = wrongCountryReader.GetString(1);
				var cityAlpha2 = wrongCountryReader.IsDBNull(2) ? null : wrongCountryReader.GetString(2);
				var cityAlpha3 = wrongCountryReader.IsDBNull(3) ? null : wrongCountryReader.GetString(3);
				var regionId = wrongCountryReader.GetString(4);
				var regionAlpha2 = wrongCountryReader.IsDBNull(5) ? null : wrongCountryReader.GetString(5);
				var regionAlpha3 = wrongCountryReader.IsDBNull(6) ? null : wrongCountryReader.GetString(6);
				wrongCountryCities.Add($"ID {id}: {name} (city country: {cityAlpha2 ?? cityAlpha3 ?? "NULL"}, region: {regionId}, region country: {regionAlpha2 ?? regionAlpha3 ?? "NULL"})");
			}
			if (wrongCountryCities.Count > 0)
			{
				_logger?.LogWarning("Found {Count} cities with region from different country (will be removed):", wrongCountryCities.Count);
				foreach (var city in wrongCountryCities)
					_logger?.LogWarning("  - {City}", city);
				
				// Delete cities with regions from different countries
				await using var deleteWrongCountryCmd = new NpgsqlCommand(@"
                    DELETE FROM cities c
                    USING regions r
                    WHERE c.region_identifier = r.identifier
                      AND (
                        (c.country_iso_alpha2_code IS NOT NULL AND r.country_iso_alpha2_code IS NOT NULL AND c.country_iso_alpha2_code != r.country_iso_alpha2_code)
                        OR (c.country_iso_alpha3_code IS NOT NULL AND r.country_iso_alpha3_code IS NOT NULL AND c.country_iso_alpha3_code != r.country_iso_alpha3_code)
                      );", connection);
				deleteWrongCountryCmd.CommandTimeout = 60;
				var deletedWrongCountryCount = await deleteWrongCountryCmd.ExecuteNonQueryAsync(cancellationToken);
				_logger?.LogInformation("Deleted {Count} cities with regions from different countries", deletedWrongCountryCount);
			}
			
			// Stage 3: Find cities that still don't have a region in their assigned country
			// These cities are outside their assigned country's borders and should be removed
			await using var unassignedCmd = new NpgsqlCommand(@"
                SELECT c.id, c.name_latin, c.country_iso_alpha2_code, c.country_iso_alpha3_code
                FROM cities c
                WHERE c.region_identifier IS NULL
                  AND (c.country_iso_alpha2_code IS NOT NULL OR c.country_iso_alpha3_code IS NOT NULL);", connection);
			await using var reader = await unassignedCmd.ExecuteReaderAsync(cancellationToken);
			var unassignedCities = new List<(int Id, string Name, string? CountryAlpha2, string? CountryAlpha3)>();
			while (await reader.ReadAsync(cancellationToken))
			{
				var id = reader.GetInt32(0);
				var name = reader.GetString(1);
				var alpha2 = reader.IsDBNull(2) ? null : reader.GetString(2);
				var alpha3 = reader.IsDBNull(3) ? null : reader.GetString(3);
				unassignedCities.Add((id, name, alpha2, alpha3));
			}
			
			if (unassignedCities.Count > 0)
			{
				_logger?.LogWarning("Stage 3: Found {Count} cities without region_identifier in their assigned country. These are outside country borders and will be removed:", unassignedCities.Count);
				foreach (var city in unassignedCities)
					_logger?.LogWarning("  - ID {Id}: {Name} (assigned country: {Country})", city.Id, city.Name, city.CountryAlpha2 ?? city.CountryAlpha3 ?? "NULL");
				
				// Delete cities that couldn't be assigned to any region in their assigned country
				// This means they are outside their assigned country's borders
				await using var deleteCmd = new NpgsqlCommand(@"
                    DELETE FROM cities
                    WHERE region_identifier IS NULL
                      AND (country_iso_alpha2_code IS NOT NULL OR country_iso_alpha3_code IS NOT NULL);", connection);
				deleteCmd.CommandTimeout = 60;
				var deletedCount = await deleteCmd.ExecuteNonQueryAsync(cancellationToken);
				_logger?.LogInformation("Stage 3: Deleted {Count} cities outside their assigned country's borders", deletedCount);
				
				// Verify deletion - check if any cities with NULL region_identifier still exist
				await using var verifyCmd = new NpgsqlCommand(@"
                    SELECT COUNT(*) 
                    FROM cities 
                    WHERE region_identifier IS NULL
                      AND (country_iso_alpha2_code IS NOT NULL OR country_iso_alpha3_code IS NOT NULL);", connection);
				var remainingCount = await verifyCmd.ExecuteScalarAsync(cancellationToken);
				if (remainingCount != null && Convert.ToInt64(remainingCount) > 0)
				{
					_logger?.LogWarning("Warning: {Count} cities still remain without region_identifier after deletion attempt", remainingCount);
					
					// Log remaining cities for debugging
					await using var remainingCmd = new NpgsqlCommand(@"
                        SELECT id, name_latin, country_iso_alpha2_code, country_iso_alpha3_code
                        FROM cities
                        WHERE region_identifier IS NULL
                          AND (country_iso_alpha2_code IS NOT NULL OR country_iso_alpha3_code IS NOT NULL)
                        LIMIT 10;", connection);
					await using var remainingReader = await remainingCmd.ExecuteReaderAsync(cancellationToken);
					while (await remainingReader.ReadAsync(cancellationToken))
					{
						var id = remainingReader.GetInt32(0);
						var name = remainingReader.GetString(1);
						var alpha2 = remainingReader.IsDBNull(2) ? null : remainingReader.GetString(2);
						var alpha3 = remainingReader.IsDBNull(3) ? null : remainingReader.GetString(3);
						_logger?.LogWarning("  Remaining city: ID {Id}, {Name} (country: {Country})", id, name, alpha2 ?? alpha3 ?? "NULL");
					}
				}
				
				// Also check for cities without country codes but with NULL region_identifier
				await using var noCountryCmd = new NpgsqlCommand(@"
                    SELECT COUNT(*) 
                    FROM cities 
                    WHERE region_identifier IS NULL
                      AND country_iso_alpha2_code IS NULL
                      AND country_iso_alpha3_code IS NULL;", connection);
				var noCountryCount = await noCountryCmd.ExecuteScalarAsync(cancellationToken);
				if (noCountryCount != null && Convert.ToInt64(noCountryCount) > 0)
				{
					_logger?.LogWarning("Found {Count} cities without region_identifier and without country codes (these will not be deleted)", noCountryCount);
					
					// Log a few examples
					await using var noCountryExamplesCmd = new NpgsqlCommand(@"
                        SELECT id, name_latin
                        FROM cities
                        WHERE region_identifier IS NULL
                          AND country_iso_alpha2_code IS NULL
                          AND country_iso_alpha3_code IS NULL
                        LIMIT 5;", connection);
					await using var noCountryExamplesReader = await noCountryExamplesCmd.ExecuteReaderAsync(cancellationToken);
					while (await noCountryExamplesReader.ReadAsync(cancellationToken))
					{
						var id = noCountryExamplesReader.GetInt32(0);
						var name = noCountryExamplesReader.GetString(1);
						_logger?.LogWarning("  City without country: ID {Id}, {Name}", id, name);
					}
				}
			}
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
			// Check for names that contain characters outside basic ASCII Latin range
			// Simplified regex: check for any character that is NOT in [A-Za-z0-9 space hyphen dot]
			var citiesToUpdate = new List<(int Id, string OriginalName, string? CountryAlpha2, string? CountryAlpha3)>();

			// Read all data first, then close the reader before starting transaction
			await using (var selectCmd = new NpgsqlCommand(@"
                SELECT id, name_latin, country_iso_alpha2_code, country_iso_alpha3_code
                FROM cities
                WHERE name_latin IS NOT NULL
                  AND name_latin ~ '[^A-Za-z0-9 -.]' -- Contains non-Latin characters (simplified pattern)
                LIMIT 10000;", connection)) // Process in batches to avoid memory issues
			{
				selectCmd.CommandTimeout = 60;
				await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);

				while (await reader.ReadAsync(cancellationToken))
				{
					var id = reader.GetInt32(0);
					var originalName = reader.GetString(1);
					var countryAlpha2 = reader.IsDBNull(2) ? null : reader.GetString(2);
					var countryAlpha3 = reader.IsDBNull(3) ? null : reader.GetString(3);
					citiesToUpdate.Add((id, originalName, countryAlpha2, countryAlpha3));
				}
			} // Reader and command are closed here

			_logger?.LogInformation("Found {Count} cities with non-Latin names to transliterate", citiesToUpdate.Count);

			// Update cities with transliterated names
			if (citiesToUpdate.Count > 0)
			{
				await using var updateTransaction = await connection.BeginTransactionAsync(cancellationToken);
				try
				{
					foreach (var (id, originalName, countryAlpha2, countryAlpha3) in citiesToUpdate)
					{
						var transliterated = _transliterationService.TransliterateToLatin(originalName);
						if (!string.IsNullOrWhiteSpace(transliterated))
						{
							// Update with transliterated name - it should already be in Latin after transliteration
							await using var updateCmd = new NpgsqlCommand(@"
                                UPDATE cities
                                SET name_latin = @transliteratedName
                                WHERE id = @cityId;", connection, updateTransaction);
							updateCmd.Parameters.AddWithValue("transliteratedName", transliterated);
							updateCmd.Parameters.AddWithValue("cityId", id);
							await updateCmd.ExecuteNonQueryAsync(cancellationToken);
							transliterationCount++;

							if (transliterationCount % 1000 == 0)
							{
								_logger?.LogInformation("Transliterated {Count} city names...", transliterationCount);
							}
							
							// Log first few transliterations for debugging
							if (transliterationCount <= 5)
							{
								_logger?.LogInformation("Transliterated city: '{Original}' -> '{Transliterated}'", 
									originalName, transliterated);
							}
						}
						else
						{
							// Log if transliteration failed
							if (transliterationCount < 5)
							{
								_logger?.LogWarning("Transliteration failed for city ID {Id}, name: '{Name}'", id, originalName);
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

	/// <summary>
	///     Post-processes imported regions: transliterates non-Latin names to Latin.
	/// </summary>
	public async Task PostProcessRegionsAsync(CancellationToken cancellationToken = default)
	{
		_logger?.LogInformation("Starting post-processing of regions: transliterating names");
		
		await using var connection = _npgsqlDataSource.CreateConnection();
		await connection.OpenAsync(cancellationToken);

		_logger?.LogInformation("Post-processing regions: transliterating names");

		var transliterationCount = 0;
		try
		{
			// Get regions with non-Latin names (or names that need transliteration)
			// Check for names that contain characters outside basic ASCII Latin range
			var regionsToUpdate = new List<(int Id, string OriginalName)>();

			// Read all data first, then close the reader before starting transaction
			await using (var selectCmd = new NpgsqlCommand(@"
                SELECT id, name_latin
                FROM regions
                WHERE name_latin IS NOT NULL
                  AND name_latin ~ '[^A-Za-z0-9 -.]' -- Contains non-Latin characters (simplified pattern)
                LIMIT 10000;", connection)) // Process in batches to avoid memory issues
			{
				selectCmd.CommandTimeout = 60;
				await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);

				while (await reader.ReadAsync(cancellationToken))
				{
					var id = reader.GetInt32(0);
					var originalName = reader.GetString(1);
					regionsToUpdate.Add((id, originalName));
				}
			} // Reader and command are closed here

			_logger?.LogInformation("Found {Count} regions with non-Latin names to transliterate", regionsToUpdate.Count);

			// Update regions with transliterated names
			if (regionsToUpdate.Count > 0)
			{
				await using var updateTransaction = await connection.BeginTransactionAsync(cancellationToken);
				try
				{
					foreach (var (id, originalName) in regionsToUpdate)
					{
						var transliterated = _transliterationService.TransliterateToLatin(originalName);
						if (!string.IsNullOrWhiteSpace(transliterated))
						{
							// Update with transliterated name - it should already be in Latin after transliteration
							await using var updateCmd = new NpgsqlCommand(@"
                                UPDATE regions
                                SET name_latin = @transliteratedName
                                WHERE id = @regionId;", connection, updateTransaction);
							updateCmd.Parameters.AddWithValue("transliteratedName", transliterated);
							updateCmd.Parameters.AddWithValue("regionId", id);
							await updateCmd.ExecuteNonQueryAsync(cancellationToken);
							transliterationCount++;

							if (transliterationCount % 100 == 0)
							{
								_logger?.LogInformation("Transliterated {Count} region names...", transliterationCount);
							}
							
							// Log first few transliterations for debugging
							if (transliterationCount <= 5)
							{
								_logger?.LogInformation("Transliterated region: '{Original}' -> '{Transliterated}'", 
									originalName, transliterated);
							}
						}
						else
						{
							// Log if transliteration failed
							if (transliterationCount < 5)
							{
								_logger?.LogWarning("Transliteration failed for region ID {Id}, name: '{Name}'", id, originalName);
							}
						}
					}

					await updateTransaction.CommitAsync(cancellationToken);
					_logger?.LogInformation("Transliterated {Count} region names to Latin", transliterationCount);
				}
				catch (Exception ex)
				{
					await updateTransaction.RollbackAsync(cancellationToken);
					_logger?.LogError(ex, "Failed to transliterate region names, transaction rolled back");
				}
			}
		}
		catch (Exception ex)
		{
			_logger?.LogWarning(ex, "Failed to transliterate some region names");
		}

		_logger?.LogInformation("Region post-processing completed: transliterated {TransliterationCount} names", transliterationCount);
	}
}
