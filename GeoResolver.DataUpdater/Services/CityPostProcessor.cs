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
			
			// Stage 3: Delete cities that still don't have a region after stages 1 and 2
			// These cities are outside their assigned country's borders and should be removed
			var unassignedCities = new List<(int Id, string Name, string? CountryAlpha2, string? CountryAlpha3)>();
			
			// Read all data first, then close the reader before starting transaction
			await using (var unassignedCmd = new NpgsqlCommand(@"
                SELECT c.id, c.name_latin, c.country_iso_alpha2_code, c.country_iso_alpha3_code, c.region_identifier
                FROM cities c
                WHERE c.region_identifier IS NULL
                  AND (c.country_iso_alpha2_code IS NOT NULL OR c.country_iso_alpha3_code IS NOT NULL);", connection))
			{
				await using var reader = await unassignedCmd.ExecuteReaderAsync(cancellationToken);
				while (await reader.ReadAsync(cancellationToken))
				{
					var id = reader.GetInt32(0);
					var name = reader.GetString(1);
					var alpha2 = reader.IsDBNull(2) ? null : reader.GetString(2);
					var alpha3 = reader.IsDBNull(3) ? null : reader.GetString(3);
					var regionId = reader.IsDBNull(4) ? null : reader.GetString(4);
					unassignedCities.Add((id, name, alpha2, alpha3));
					
					// Log Vovchansk specifically for debugging
					if (name.Contains("Vovchans", StringComparison.OrdinalIgnoreCase) || 
					    name.Contains("Вовчанск", StringComparison.OrdinalIgnoreCase))
					{
						_logger?.LogWarning("Found Vovchansk in unassigned cities: ID {Id}, Name: {Name}, Country: {Country}, Region: {Region}", 
							id, name, alpha2 ?? alpha3 ?? "NULL", regionId ?? "NULL");
					}
				}
			} // Reader and command are closed here
			
			if (unassignedCities.Count > 0)
			{
				_logger?.LogWarning("Stage 3: Found {Count} cities without region_identifier after stages 1 and 2. These are outside country borders and will be removed:", unassignedCities.Count);
				foreach (var city in unassignedCities)
					_logger?.LogWarning("  - ID {Id}: {Name} (assigned country: {Country})", city.Id, city.Name, city.CountryAlpha2 ?? city.CountryAlpha3 ?? "NULL");
				
				// Delete cities that couldn't be assigned to any region in their assigned country
				// This means they are outside their assigned country's borders
				// Use explicit transaction to ensure deletion
				await using var deleteTransaction = await connection.BeginTransactionAsync(cancellationToken);
				try
				{
					await using var deleteCmd = new NpgsqlCommand(@"
                        DELETE FROM cities
                        WHERE region_identifier IS NULL
                          AND (country_iso_alpha2_code IS NOT NULL OR country_iso_alpha3_code IS NOT NULL);", connection, deleteTransaction);
					deleteCmd.CommandTimeout = 60;
					var deletedCount = await deleteCmd.ExecuteNonQueryAsync(cancellationToken);
					await deleteTransaction.CommitAsync(cancellationToken);
					_logger?.LogInformation("Stage 3: Deleted {Count} cities outside their assigned country's borders", deletedCount);
					
					// Verify Vovchansk was deleted
					foreach (var city in unassignedCities)
					{
						if (city.Name.Contains("Vovchans", StringComparison.OrdinalIgnoreCase) || 
						    city.Name.Contains("Вовчанск", StringComparison.OrdinalIgnoreCase))
						{
							await using var verifyCmd = new NpgsqlCommand(@"
                                SELECT id, name_latin, region_identifier, country_iso_alpha2_code, country_iso_alpha3_code
                                FROM cities
                                WHERE id = @cityId;", connection);
							verifyCmd.Parameters.AddWithValue("cityId", city.Id);
							await using var verifyReader = await verifyCmd.ExecuteReaderAsync(cancellationToken);
							if (await verifyReader.ReadAsync(cancellationToken))
							{
								var stillExists = verifyReader.GetInt32(0);
								var name = verifyReader.GetString(1);
								var regionId = verifyReader.IsDBNull(2) ? null : verifyReader.GetString(2);
								var alpha2 = verifyReader.IsDBNull(3) ? null : verifyReader.GetString(3);
								var alpha3 = verifyReader.IsDBNull(4) ? null : verifyReader.GetString(4);
								_logger?.LogError("ERROR: Vovchansk (ID {Id}, Name: {Name}) still exists after deletion! Region: {Region}, Country: {Country}", 
									stillExists, name, regionId ?? "NULL", alpha2 ?? alpha3 ?? "NULL");
							}
							else
							{
								_logger?.LogInformation("Vovchansk (ID {Id}) was successfully deleted", city.Id);
							}
						}
					}
				}
				catch (Exception ex)
				{
					await deleteTransaction.RollbackAsync(cancellationToken);
					_logger?.LogError(ex, "Failed to delete cities without region_identifier, transaction rolled back");
					throw;
				}
			}
			else
			{
				_logger?.LogInformation("Stage 3: No cities found without region_identifier (all cities have regions assigned)");
			}
			
			// Also check for cities without country codes but with NULL region_identifier - these should also be deleted
			await using var noCountryCmd = new NpgsqlCommand(@"
                SELECT id, name_latin, region_identifier
                FROM cities
                WHERE region_identifier IS NULL
                  AND country_iso_alpha2_code IS NULL
                  AND country_iso_alpha3_code IS NULL;", connection);
			await using var noCountryReader = await noCountryCmd.ExecuteReaderAsync(cancellationToken);
			var noCountryCities = new List<(int Id, string Name)>();
			while (await noCountryReader.ReadAsync(cancellationToken))
			{
				var id = noCountryReader.GetInt32(0);
				var name = noCountryReader.GetString(1);
				var regionId = noCountryReader.IsDBNull(2) ? null : noCountryReader.GetString(2);
				noCountryCities.Add((id, name));
				
				// Log Vovchansk specifically
				if (name.Contains("Vovchans", StringComparison.OrdinalIgnoreCase) || 
				    name.Contains("Вовчанск", StringComparison.OrdinalIgnoreCase))
				{
					_logger?.LogWarning("Found Vovchansk without country codes: ID {Id}, Name: {Name}, Region: {Region}", 
						id, name, regionId ?? "NULL");
				}
			}
			
			if (noCountryCities.Count > 0)
			{
				_logger?.LogWarning("Found {Count} cities without region_identifier and without country codes (will be removed):", noCountryCities.Count);
				foreach (var city in noCountryCities)
					_logger?.LogWarning("  - ID {Id}: {Name}", city.Id, city.Name);
				
				// Delete cities without country codes and without region
				await using var deleteNoCountryCmd = new NpgsqlCommand(@"
                    DELETE FROM cities
                    WHERE region_identifier IS NULL
                      AND country_iso_alpha2_code IS NULL
                      AND country_iso_alpha3_code IS NULL;", connection);
				deleteNoCountryCmd.CommandTimeout = 60;
				var deletedNoCountryCount = await deleteNoCountryCmd.ExecuteNonQueryAsync(cancellationToken);
				_logger?.LogInformation("Deleted {Count} cities without region_identifier and without country codes", deletedNoCountryCount);
			}
			
			// Final check: search for Vovchansk by name to see if it still exists
			await using var finalCheckCmd = new NpgsqlCommand(@"
                SELECT id, name_latin, region_identifier, country_iso_alpha2_code, country_iso_alpha3_code
                FROM cities
                WHERE name_latin ILIKE '%vovchans%' OR name_latin ILIKE '%вовчанск%';", connection);
			await using var finalCheckReader = await finalCheckCmd.ExecuteReaderAsync(cancellationToken);
			while (await finalCheckReader.ReadAsync(cancellationToken))
			{
				var id = finalCheckReader.GetInt32(0);
				var name = finalCheckReader.GetString(1);
				var regionId = finalCheckReader.IsDBNull(2) ? null : finalCheckReader.GetString(2);
				var alpha2 = finalCheckReader.IsDBNull(3) ? null : finalCheckReader.GetString(3);
				var alpha3 = finalCheckReader.IsDBNull(4) ? null : finalCheckReader.GetString(4);
				_logger?.LogError("Vovchansk still exists in database after deletion attempts: ID {Id}, Name: {Name}, Region: {Region}, Country: {Country}", 
					id, name, regionId ?? "NULL", alpha2 ?? alpha3 ?? "NULL");
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
