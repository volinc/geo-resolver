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
