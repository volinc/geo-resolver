using System.ComponentModel;
using System.Diagnostics;
using System.IO.Compression;
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
	private readonly IHttpClientFactory _httpClientFactory;
	private readonly ILogger<OsmCityShapefileLoader> _logger;
	private readonly IOptions<CityLoaderOptions> _options;

	public OsmCityShapefileLoader(
		ILogger<OsmCityShapefileLoader> logger,
		IHttpClientFactory httpClientFactory,
		IDatabaseWriterService databaseWriterService,
		IOptions<CityLoaderOptions> options,
		NpgsqlDataSource dataSource)
	{
		_logger = logger;
		_httpClientFactory = httpClientFactory;
		_databaseWriterService = databaseWriterService;
		_options = options;
		_dataSource = dataSource;
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

		var urls = BuildGeofabrikUrls(countryIsoAlpha2Code);
		if (urls.Count == 0)
		{
			_logger.LogWarning("No Geofabrik URLs configured for country {CountryIso2} – skipping", countryIsoAlpha2Code);
			return;
		}

		if (urls.Count == 1)
			_logger.LogInformation("Using Geofabrik URL for {CountryIso2}: {Url}", countryIsoAlpha2Code, urls[0]);
		else
			_logger.LogInformation("Using {Count} Geofabrik regional URLs for {CountryIso2}: {Urls}",
				urls.Count, countryIsoAlpha2Code, string.Join(", ", urls));

		Exception? lastException;

		try
		{
			using var httpClient = _httpClientFactory.CreateClient();
			httpClient.Timeout = TimeSpan.FromMinutes(20);

			// Если один URL – сохраняем прежнюю последовательную логику
			if (urls.Count == 1)
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
			var downloadStopwatch = Stopwatch.StartNew();
			var response =
				await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

			if (!response.IsSuccessStatusCode)
			{
				_logger.LogError("Failed to download Geofabrik shapefile for {CountryIso2} from {Url}: {StatusCode}",
					countryIsoAlpha2Code, url, response.StatusCode);
				return false;
			}

			await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);
			downloadStopwatch.Stop();
			_logger.LogInformation(
				"Downloaded shapefile for {CountryIso2} from {Url} in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				countryIsoAlpha2Code, url, downloadStopwatch.ElapsedMilliseconds,
				downloadStopwatch.Elapsed.TotalSeconds);

			await ProcessCityShapefileZipAsync(zipStream, countryIsoAlpha2Code, cancellationToken);
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

	private static IReadOnlyList<string> BuildGeofabrikUrls(string countryIsoAlpha2Code)
	{
		// Hard-coded mapping from ISO alpha-2 to Geofabrik path.
		// For many countries Geofabrik uses a single shapefile per country:
		//   /europe/{country}-latest-free.shp.zip etc.
		// For some large countries (e.g. RU) only regional shapefiles exist,
		// so we provide a list of regional paths and merge them.

		var code = countryIsoAlpha2Code.ToUpperInvariant();

		// Special case: Russia – only regional shapefiles, no single country-wide shapefile
		if (code == "RU")
		{
			// Список федеральных округов России на Geofabrik.
			// Каждый из этих путей даёт свой *-latest-free.shp.zip.
			var regionPaths = new[]
			{
				"russia/central-fed-district",
				"russia/northwestern-fed-district",
				"russia/siberian-fed-district",
				"russia/ural-fed-district",
				"russia/far-eastern-fed-district",
				"russia/volga-fed-district",
				"russia/south-fed-district",
				"russia/north-caucasus-fed-district"
			};

			return regionPaths
				.Select(p => $"https://download.geofabrik.de/{p}-latest-free.shp.zip")
				.ToArray();
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

		return new[] {$"https://download.geofabrik.de/{path}-latest-free.shp.zip"};
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

		try
		{
			var processInfo = new ProcessStartInfo
			{
				FileName = "ogr2ogr",
				Arguments = $"-f GeoJSON \"{geoJsonFile}\" \"{shpFilePath}\" -lco RFC7946=YES",
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