using System.ComponentModel;
using System.Diagnostics;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace GeoResolver.DataUpdater.Services.Shapefile;

/// <summary>
///     Loads and processes OSM-based city polygon data from Geofabrik Shapefiles.
///     For each configured country (2-letter ISO code) downloads the corresponding
///     regional/country shapefile ZIP, extracts the city polygon layer and imports
///     it into the <c>cities</c> table.
/// </summary>
public sealed class OsmCityShapefileLoader
{
	private readonly IDatabaseWriterService _databaseWriterService;
	private readonly NpgsqlDataSource _dataSource;
	private readonly GeofabrikRegionPathResolver _geofabrikRegionPathResolver;
	private readonly IHttpClientFactory _httpClientFactory;
	private readonly ILogger<OsmCityShapefileLoader> _logger;
	private readonly IOptions<CityLoaderOptions> _options;

	public OsmCityShapefileLoader(
		ILogger<OsmCityShapefileLoader> logger,
		IHttpClientFactory httpClientFactory,
		IDatabaseWriterService databaseWriterService,
		IOptions<CityLoaderOptions> options,
		NpgsqlDataSource dataSource,
		GeofabrikRegionPathResolver geofabrikRegionPathResolver)
	{
		_logger = logger;
		_httpClientFactory = httpClientFactory;
		_databaseWriterService = databaseWriterService;
		_options = options;
		_dataSource = dataSource;
		_geofabrikRegionPathResolver = geofabrikRegionPathResolver;
	}

	/// <summary>
	///     Loads cities for all countries listed in configuration section "CityLoader:Countries"
	///     (array of 2-letter ISO country codes, e.g. ["DE", "FR", "RU"]).
	/// </summary>
	public async Task LoadAllConfiguredCountriesAsync(CancellationToken cancellationToken = default)
	{
		var configuredCodes = _options.Value.Countries;

		string[] countryCodes;
		if (configuredCodes.Length == 0)
		{
			_logger.LogInformation(
				"CityLoader:Countries is empty or not configured – will use all countries from 'countries' table");
			countryCodes = await GetAllCountryIso2FromDatabaseAsync(cancellationToken);
		}
		else
		{
			countryCodes = configuredCodes;
		}

		_logger.LogInformation("Starting OSM city loading for {Count} countries: {Countries}",
			countryCodes.Length, string.Join(", ", countryCodes));

		foreach (var rawCode in countryCodes)
		{
			var code = rawCode.Trim().ToUpperInvariant();
			if (string.IsNullOrWhiteSpace(code) || code.Length != 2)
			{
				_logger.LogWarning("Skipping invalid country code in CityLoader:Countries: '{Code}'", rawCode);
				continue;
			}

			try
			{
				await LoadCitiesForCountryAsync(code, cancellationToken);
			}
			catch (Exception ex)
			{
				// Не прерываем общий процесс по одной стране, но логируем ошибку.
				_logger.LogError(ex, "Failed to load cities for country {CountryIso2}", code);
			}
		}

		_logger.LogInformation("Finished OSM city loading for all configured countries");
	}

	/// <summary>
	///     Loads cities for a single country (2-letter ISO code) from the appropriate Geofabrik shapefile.
	/// </summary>
	private async Task LoadCitiesForCountryAsync(string countryIsoAlpha2Code,
		CancellationToken cancellationToken = default)
	{
		if (string.IsNullOrWhiteSpace(countryIsoAlpha2Code) || countryIsoAlpha2Code.Length != 2)
			throw new ArgumentException("Country ISO alpha-2 code must be 2-letter non-empty string",
				nameof(countryIsoAlpha2Code));

		countryIsoAlpha2Code = countryIsoAlpha2Code.ToUpperInvariant();

		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("=== [Cities] Starting to load cities for country {CountryIso2} ===",
			countryIsoAlpha2Code);

		var regionPaths = _geofabrikRegionPathResolver.GetRegionPaths(countryIsoAlpha2Code);
		if (regionPaths.Count == 0)
		{
			_logger.LogWarning("No Geofabrik region paths configured for country {CountryIso2} – skipping", countryIsoAlpha2Code);
			return;
		}

		var urls = regionPaths
			.Select(p => $"https://download.geofabrik.de/{p}-latest-free.shp.zip")
			.ToArray();

		if (urls.Length == 1)
			_logger.LogInformation("Using Geofabrik URL for {CountryIso2}: {Url}", countryIsoAlpha2Code, urls[0]);
		else
			_logger.LogInformation("Using {Count} Geofabrik regional URLs for {CountryIso2}: {Urls}",
				urls.Length, countryIsoAlpha2Code, string.Join(", ", urls));

		Exception? lastException;

		try
		{
			using var httpClient = _httpClientFactory.CreateClient();
			httpClient.Timeout = TimeSpan.FromMinutes(20);

			// Если один URL – сохраняем прежнюю последовательную логику
			if (urls.Length == 1)
			{
				var success =
					await DownloadAndProcessCityZipAsync(urls[0], countryIsoAlpha2Code, httpClient, cancellationToken);
				if (!success)
					throw new InvalidOperationException(
						$"Failed to download or process Geofabrik shapefile for {countryIsoAlpha2Code} from {urls[0]}");
			}
			else
			{
				// Для стран без единого shapefile (например, RU) обрабатываем региональные ZIP'ы параллельно
				var tasks = urls.Select(url =>
					DownloadAndProcessCityZipAsync(url, countryIsoAlpha2Code, httpClient, cancellationToken));

				var results = await Task.WhenAll(tasks);
				if (!results.Any(r => r))
					throw new InvalidOperationException(
						$"Failed to download or process any Geofabrik regional shapefiles for {countryIsoAlpha2Code}");
			}

			overallStopwatch.Stop();
			_logger.LogInformation(
				"=== [Cities] Successfully loaded cities for {CountryIso2} in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes) ===",
				countryIsoAlpha2Code, overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
			return;
		}
		catch (HttpRequestException ex)
		{
			lastException = ex;
			_logger.LogError(ex,
				"HTTP error while downloading Geofabrik shapefile(s) for {CountryIso2}. Urls: {Urls}",
				countryIsoAlpha2Code, string.Join(", ", urls));
		}
		catch (Exception ex)
		{
			lastException = ex;
			_logger.LogError(ex, "Unexpected error while loading cities for {CountryIso2}", countryIsoAlpha2Code);
		}

		overallStopwatch.Stop();
		throw new InvalidOperationException(
			$"Failed to load OSM/Geofabrik cities for country {countryIsoAlpha2Code}", lastException);
	}

	private async Task<bool> DownloadAndProcessCityZipAsync(string url, string countryIsoAlpha2Code,
		HttpClient httpClient, CancellationToken cancellationToken)
	{
		try
		{
			// Get cache directory
			var cacheDir = Path.Combine(Directory.GetCurrentDirectory(), "cache", "geofabrik");
			Directory.CreateDirectory(cacheDir);

			// Generate cache file name from URL
			var urlHash = ComputeSha256Hash(url);
			var cacheFile = Path.Combine(cacheDir, $"{urlHash}.zip");
			var cacheMetaFile = Path.Combine(cacheDir, $"{urlHash}.meta");

			Stream? zipStream = null;
			var downloadStopwatch = Stopwatch.StartNew();
			var needsDownload = true;

			// Check if cached file exists and is up-to-date
			if (File.Exists(cacheFile) && File.Exists(cacheMetaFile))
			{
				var cachedEtag = await File.ReadAllTextAsync(cacheMetaFile, cancellationToken);
				if (!string.IsNullOrWhiteSpace(cachedEtag))
				{
					// Check if file on server has changed using conditional request
					using var headRequest = new HttpRequestMessage(HttpMethod.Head, url);
					headRequest.Headers.IfNoneMatch.Add(new System.Net.Http.Headers.EntityTagHeaderValue($"\"{cachedEtag.Trim()}\""));
					
					try
					{
						var headResponse = await httpClient.SendAsync(headRequest, cancellationToken);
						if (headResponse.StatusCode == System.Net.HttpStatusCode.NotModified)
						{
							_logger.LogInformation(
								"Using cached shapefile for {CountryIso2} from {Url} (ETag: {ETag})",
								countryIsoAlpha2Code, url, cachedEtag.Trim());
							zipStream = File.OpenRead(cacheFile);
							needsDownload = false;
							downloadStopwatch.Stop();
						}
					}
					catch (Exception ex)
					{
						_logger.LogWarning(ex, "Failed to check ETag for {Url}, will download", url);
					}
				}
			}

			// Download if needed
			if (needsDownload)
			{
				var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

				if (!response.IsSuccessStatusCode)
				{
					_logger.LogError("Failed to download Geofabrik shapefile for {CountryIso2} from {Url}: {StatusCode}",
						countryIsoAlpha2Code, url, response.StatusCode);
					return false;
				}

				// Save ETag for future cache checks
				var etag = response.Headers.ETag?.Tag;
				if (!string.IsNullOrWhiteSpace(etag))
				{
					await File.WriteAllTextAsync(cacheMetaFile, etag, cancellationToken);
				}

				// Download to cache file
				await using var responseStream = await response.Content.ReadAsStreamAsync(cancellationToken);
				await using var fileStream = File.Create(cacheFile);
				await responseStream.CopyToAsync(fileStream, cancellationToken);
				
				downloadStopwatch.Stop();
				_logger.LogInformation(
					"Downloaded shapefile for {CountryIso2} from {Url} in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s) and cached to {CacheFile}",
					countryIsoAlpha2Code, url, downloadStopwatch.ElapsedMilliseconds,
					downloadStopwatch.Elapsed.TotalSeconds, cacheFile);
				
				zipStream = File.OpenRead(cacheFile);
			}

			if (zipStream != null)
			{
				await using (zipStream)
				{
					await ProcessCityShapefileZipAsync(zipStream, countryIsoAlpha2Code, cancellationToken);
				}
			}
			
			return true;
		}
		catch (InvalidDataException ex)
		{
			// Частый случай: URL не возвращает корректный ZIP (например, HTML-страница вместо файла)
			_logger.LogError(ex,
				"Invalid ZIP data when processing Geofabrik URL {Url} for country {CountryIso2}. " +
				"Make sure this URL points to a *.shp.zip file.", url, countryIsoAlpha2Code);
			return false;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error downloading or processing Geofabrik shapefile from {Url} for {CountryIso2}",
				url, countryIsoAlpha2Code);
			return false;
		}
	}

	private static string ComputeSha256Hash(string input)
	{
		var bytes = Encoding.UTF8.GetBytes(input);
		var hashBytes = SHA256.HashData(bytes);
		return Convert.ToHexString(hashBytes).ToLowerInvariant();
	}

	private async Task ProcessCityShapefileZipAsync(Stream zipStream, string countryIsoAlpha2Code,
		CancellationToken cancellationToken)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Extracting and processing Geofabrik city polygons ZIP for {CountryIso2}...",
			countryIsoAlpha2Code);

		var tempDir = Path.Combine(Path.GetTempPath(),
			$"geo-resolver-osm-cities-{countryIsoAlpha2Code}-{Guid.NewGuid()}");
		Directory.CreateDirectory(tempDir);

		try
		{
			// Extract ZIP
			var extractStopwatch = Stopwatch.StartNew();
			await using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, false))
			{
				foreach (var entry in archive.Entries)
				{
					var fullPath = Path.Combine(tempDir, entry.FullName);
					var directory = Path.GetDirectoryName(fullPath);
					if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
						Directory.CreateDirectory(directory);

					if (!string.IsNullOrEmpty(entry.Name))
					{
						await using var entryStream = await entry.OpenAsync(cancellationToken);
						await using var fileStream = File.Create(fullPath);
						await entryStream.CopyToAsync(fileStream, cancellationToken);
					}
				}
			}

			extractStopwatch.Stop();
			_logger.LogInformation(
				"ZIP archive for {CountryIso2} extracted to {TempDir} in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				countryIsoAlpha2Code, tempDir, extractStopwatch.ElapsedMilliseconds,
				extractStopwatch.Elapsed.TotalSeconds);

			// Find city polygon shapefile, typically gis_osm_places_a_free_1.shp
			var shpFile = Directory.GetFiles(tempDir, "gis_osm_places_a_*_1.shp", SearchOption.AllDirectories)
				              .FirstOrDefault()
			              ?? Directory.GetFiles(tempDir, "*places_a*.shp", SearchOption.AllDirectories).FirstOrDefault()
			              ?? Directory.GetFiles(tempDir, "*.shp", SearchOption.AllDirectories).FirstOrDefault();

			if (shpFile == null)
			{
				var allFiles = Directory.GetFiles(tempDir, "*", SearchOption.AllDirectories);
				_logger.LogError(
					"No city polygon shapefile (gis_osm_places_a_*.shp) found in Geofabrik ZIP for {CountryIso2}. Files found: {Count}",
					countryIsoAlpha2Code, allFiles.Length);
				foreach (var file in allFiles.Take(30))
					_logger.LogError("  Found file: {File}", Path.GetRelativePath(tempDir, file));

				throw new FileNotFoundException(
					"City polygon shapefile (gis_osm_places_a_*.shp) not found in Geofabrik ZIP archive");
			}

			_logger.LogInformation("Found city polygon shapefile for {CountryIso2}: {ShpFile}",
				countryIsoAlpha2Code, shpFile);

			// Convert Shapefile to GeoJSON using ogr2ogr (same approach as NaturalEarthShapefileLoader)
			var convertStopwatch = Stopwatch.StartNew();
			var geoJson = await ConvertShapefileToGeoJsonAsync(shpFile, cancellationToken);
			convertStopwatch.Stop();
			_logger.LogInformation(
				"Shapefile for {CountryIso2} converted to GeoJSON in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				countryIsoAlpha2Code, convertStopwatch.ElapsedMilliseconds, convertStopwatch.Elapsed.TotalMinutes);

			// Import into database; pass default country ISO2 code so it can be used if attributes miss ISO
			var importStopwatch = Stopwatch.StartNew();
			_logger.LogInformation("Importing cities for {CountryIso2} into database...", countryIsoAlpha2Code);
			await _databaseWriterService.ImportCitiesFromGeoJsonAsync(geoJson, countryIsoAlpha2Code, cancellationToken);
			importStopwatch.Stop();
			_logger.LogInformation(
				"Cities for {CountryIso2} imported to database in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				countryIsoAlpha2Code, importStopwatch.ElapsedMilliseconds, importStopwatch.Elapsed.TotalSeconds);

			overallStopwatch.Stop();
			_logger.LogInformation(
				"Geofabrik city polygons processing for {CountryIso2} completed in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				countryIsoAlpha2Code, overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
		}
		finally
		{
			// Cleanup
			try
			{
				Directory.Delete(tempDir, true);
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Failed to delete temporary directory {TempDir} for {CountryIso2}", tempDir,
					countryIsoAlpha2Code);
			}
		}
	}


	private async Task<string[]> GetAllCountryIso2FromDatabaseAsync(CancellationToken cancellationToken)
	{
		var codes = new List<string>();

		await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
		await using var cmd = new NpgsqlCommand(
			"SELECT DISTINCT iso_alpha2_code FROM countries WHERE iso_alpha2_code IS NOT NULL ORDER BY iso_alpha2_code;",
			connection);

		await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		while (await reader.ReadAsync(cancellationToken))
			if (!reader.IsDBNull(0))
			{
				var code = reader.GetString(0).Trim().ToUpperInvariant();
				if (code.Length == 2) codes.Add(code);
			}

		_logger.LogInformation("Loaded {Count} country ISO alpha-2 codes from 'countries' table for city loading",
			codes.Count);

		return codes.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
	}

	private async Task<string> ConvertShapefileToGeoJsonAsync(string shpFilePath, CancellationToken cancellationToken)
	{
		_logger.LogInformation("Converting OSM/Geofabrik Shapefile to GeoJSON format using ogr2ogr...");

		var geoJsonFile = Path.ChangeExtension(shpFilePath, ".geojson");

		// Фильтруем только крупные населенные пункты:
		// - fclass IN ('city', 'town', 'national_capital') - официальные города и крупные поселки
		// - ИЛИ population >= 10000 - населенные пункты с населением >= 10000
		// Это исключает деревни (village), хутора (hamlet), пригороды (suburb) и т.д.
		var whereClause =
			"fclass IN ('city', 'town', 'national_capital') OR population >= 10000";

		try
		{
			var processInfo = new ProcessStartInfo
			{
				FileName = "ogr2ogr",
				Arguments =
					$"-f GeoJSON \"{geoJsonFile}\" \"{shpFilePath}\" -lco RFC7946=YES -where \"{whereClause}\"",
				RedirectStandardOutput = true,
				RedirectStandardError = true,
				UseShellExecute = false,
				CreateNoWindow = true
			};

			using var process = Process.Start(processInfo);
			if (process == null) throw new InvalidOperationException("Failed to start ogr2ogr process");

			var errorOutput = await process.StandardError.ReadToEndAsync(cancellationToken);
			await process.WaitForExitAsync(cancellationToken);

			if (process.ExitCode != 0)
				throw new InvalidOperationException($"ogr2ogr failed with exit code {process.ExitCode}: {errorOutput}");

			if (!File.Exists(geoJsonFile))
				throw new FileNotFoundException($"GeoJSON file was not created: {geoJsonFile}");

			_logger.LogInformation("Successfully converted OSM/Geofabrik Shapefile to GeoJSON");
			var geoJsonContent = await File.ReadAllTextAsync(geoJsonFile, cancellationToken);

			// Clean up temporary GeoJSON file
			try
			{
				File.Delete(geoJsonFile);
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Failed to delete temporary OSM GeoJSON file");
			}

			return geoJsonContent;
		}
		catch (Win32Exception ex) when (ex.NativeErrorCode == 2) // File not found
		{
			throw new InvalidOperationException(
				"ogr2ogr (GDAL) is not installed or not in PATH. " +
				"Please install GDAL tools to process Shapefile data. " +
				"Install instructions: macOS: brew install gdal, Ubuntu/Debian: sudo apt-get install gdal-bin, Windows: Download from OSGeo4W",
				ex);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to convert OSM/Geofabrik Shapefile to GeoJSON");
			throw;
		}
	}
}