using System.ComponentModel;
using System.Diagnostics;
using System.IO.Compression;
using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater.Services.Shapefile;

/// <summary>
///     Loads and processes Natural Earth Shapefile data
///     Converts Shapefile to GeoJSON format for processing
/// </summary>
public sealed class NaturalEarthShapefileLoader : INaturalEarthShapefileLoader
{
	// Natural Earth official download URLs for 10m datasets
	private const string Admin0CountriesUrl =
		"https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/";

	private const string Admin1StatesProvincesUrl =
		"https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-1-states-provinces/";

	private const string PopulatedPlacesUrl =
		"https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-populated-places/";

	private readonly IDatabaseWriterService _databaseWriterService;
	private readonly IHttpClientFactory _httpClientFactory;
	private readonly ILogger<NaturalEarthShapefileLoader> _logger;

	public NaturalEarthShapefileLoader(
		ILogger<NaturalEarthShapefileLoader> logger,
		IHttpClientFactory httpClientFactory,
		IDatabaseWriterService databaseWriterService)
	{
		_logger = logger;
		_httpClientFactory = httpClientFactory;
		_databaseWriterService = databaseWriterService;
	}

	/// <summary>
	///     Downloads Natural Earth Admin 0 (Countries) Shapefile ZIP and processes it
	/// </summary>
	public async Task LoadCountriesAsync(CancellationToken cancellationToken = default)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Starting to load countries from Natural Earth Admin 0 dataset...");

		var downloadUrls = new[]
		{
			// Try direct download URLs
			"https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries.zip",
			"https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip",
			// Fallback: try CDN or mirror
			"https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_10m_admin_0_countries.zip"
		};

		Exception? lastException = null;

		foreach (var url in downloadUrls)
			try
			{
				_logger.LogInformation("Attempting to download Admin 0 Countries Shapefile from {Url}...", url);

				using var httpClient = _httpClientFactory.CreateClient();
				httpClient.Timeout = TimeSpan.FromMinutes(15); // Large file, increase timeout

				var downloadStopwatch = Stopwatch.StartNew();
				var response =
					await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

				if (!response.IsSuccessStatusCode)
				{
					_logger.LogWarning("Failed to download from {Url}: {StatusCode}", url, response.StatusCode);
					continue;
				}

				await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);
				downloadStopwatch.Stop();
				_logger.LogInformation("Download completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
					downloadStopwatch.ElapsedMilliseconds, downloadStopwatch.Elapsed.TotalSeconds);

				await ProcessAdmin0CountriesShapefileZipAsync(zipStream, cancellationToken);

				overallStopwatch.Stop();
				_logger.LogInformation(
					"Successfully loaded countries from {Url} in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
					url, overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
				return;
			}
			catch (HttpRequestException ex)
			{
				_logger.LogWarning(ex, "Failed to download Admin 0 Countries Shapefile from {Url}", url);
				lastException = ex;
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Error processing Admin 0 Countries Shapefile from {Url}", url);
				lastException = ex;
			}

		// If all downloads failed, log instructions for manual download
		_logger.LogError(lastException, "Failed to download Admin 0 Countries Shapefile from all sources");
		_logger.LogInformation("To load countries manually:");
		_logger.LogInformation("1. Download ne_10m_admin_0_countries.zip from: {Url}", Admin0CountriesUrl);
		_logger.LogInformation("2. Place the ZIP file in the working directory or provide a local path");
		_logger.LogInformation("3. The application will process it automatically if found in the current directory");

		throw new InvalidOperationException(
			"Failed to download Admin 0 Countries Shapefile. Please download manually and place in working directory.",
			lastException);
	}

	/// <summary>
	///     Downloads Natural Earth Admin 1 (States/Provinces) Shapefile ZIP and processes it
	/// </summary>
	public async Task LoadRegionsAsync(CancellationToken cancellationToken = default)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Starting to load regions from Natural Earth Admin 1 dataset...");

		// Note: Natural Earth requires manual download from website
		// For automated processing, we'll use a fallback to try direct download URLs
		// The actual Shapefile ZIP should be downloaded manually or from a mirror

		var downloadUrls = new[]
		{
			// Try direct download URLs (these may not work due to website structure)
			"https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_1_states_provinces.zip",
			"https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_1_states_provinces.zip",
			// Fallback: try CDN or mirror
			"https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_10m_admin_1_states_provinces.zip"
		};

		Exception? lastException = null;

		foreach (var url in downloadUrls)
			try
			{
				_logger.LogInformation("Attempting to download Admin 1 Shapefile from {Url}...", url);

				using var httpClient = _httpClientFactory.CreateClient();
				httpClient.Timeout = TimeSpan.FromMinutes(15); // Large file, increase timeout

				var downloadStopwatch = Stopwatch.StartNew();
				var response =
					await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

				if (!response.IsSuccessStatusCode)
				{
					_logger.LogWarning("Failed to download from {Url}: {StatusCode}", url, response.StatusCode);
					continue;
				}

				await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);
				downloadStopwatch.Stop();
				_logger.LogInformation("Download completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
					downloadStopwatch.ElapsedMilliseconds, downloadStopwatch.Elapsed.TotalSeconds);

				await ProcessAdmin1ShapefileZipAsync(zipStream, cancellationToken);

				overallStopwatch.Stop();
				_logger.LogInformation(
					"Successfully loaded regions from {Url} in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
					url, overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
				return;
			}
			catch (HttpRequestException ex)
			{
				_logger.LogWarning(ex, "Failed to download Admin 1 Shapefile from {Url}", url);
				lastException = ex;
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Error processing Admin 1 Shapefile from {Url}", url);
				lastException = ex;
			}

		// If all downloads failed, log instructions for manual download
		_logger.LogError(lastException, "Failed to download Admin 1 Shapefile from all sources");
		_logger.LogInformation("To load regions manually:");
		_logger.LogInformation("1. Download ne_10m_admin_1_states_provinces.zip from: {Url}", Admin1StatesProvincesUrl);
		_logger.LogInformation("2. Place the ZIP file in the working directory or provide a local path");
		_logger.LogInformation("3. The application will process it automatically if found in the current directory");

		throw new InvalidOperationException(
			"Failed to download Admin 1 Shapefile. Please download manually and place in working directory.",
			lastException);
	}

	/// <summary>
	///     Downloads Natural Earth Populated Places Shapefile ZIP and processes it
	/// </summary>
	public async Task LoadCitiesAsync(CancellationToken cancellationToken = default)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Starting to load cities from Natural Earth Populated Places dataset...");

		var downloadUrls = new[]
		{
			// Try direct download URLs
			"https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_populated_places.zip",
			"https://naciscdn.org/naturalearth/10m/cultural/ne_10m_populated_places.zip",
			// Fallback: try CDN or mirror
			"https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_10m_populated_places.zip"
		};

		Exception? lastException = null;

		foreach (var url in downloadUrls)
			try
			{
				_logger.LogInformation("Attempting to download Populated Places Shapefile from {Url}...", url);

				using var httpClient = _httpClientFactory.CreateClient();
				httpClient.Timeout = TimeSpan.FromMinutes(15); // Large file, increase timeout

				var downloadStopwatch = Stopwatch.StartNew();
				var response =
					await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

				if (!response.IsSuccessStatusCode)
				{
					_logger.LogWarning("Failed to download from {Url}: {StatusCode}", url, response.StatusCode);
					continue;
				}

				await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);
				downloadStopwatch.Stop();
				_logger.LogInformation("Download completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
					downloadStopwatch.ElapsedMilliseconds, downloadStopwatch.Elapsed.TotalSeconds);

				await ProcessPopulatedPlacesShapefileZipAsync(zipStream, cancellationToken);

				overallStopwatch.Stop();
				_logger.LogInformation(
					"Successfully loaded cities from {Url} in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
					url, overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
				return;
			}
			catch (HttpRequestException ex)
			{
				_logger.LogWarning(ex, "Failed to download Populated Places Shapefile from {Url}", url);
				lastException = ex;
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Error processing Populated Places Shapefile from {Url}", url);
				lastException = ex;
			}

		// If all downloads failed, log instructions for manual download
		_logger.LogError(lastException, "Failed to download Populated Places Shapefile from all sources");
		_logger.LogInformation("To load cities manually:");
		_logger.LogInformation("1. Download ne_10m_populated_places.zip from: {Url}", PopulatedPlacesUrl);
		_logger.LogInformation("2. Place the ZIP file in the working directory or provide a local path");
		_logger.LogInformation("3. The application will process it automatically if found in the current directory");

		throw new InvalidOperationException(
			"Failed to download Populated Places Shapefile. Please download manually and place in working directory.",
			lastException);
	}

	private async Task ProcessAdmin0CountriesShapefileZipAsync(Stream zipStream, CancellationToken cancellationToken)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Extracting and processing Admin 0 Countries Shapefile ZIP...");

		var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
		Directory.CreateDirectory(tempDir);

		try
		{
			// Extract ZIP
			var extractStopwatch = Stopwatch.StartNew();
			using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, false);
			foreach (var entry in archive.Entries)
			{
				var fullPath = Path.Combine(tempDir, entry.FullName);
				var directory = Path.GetDirectoryName(fullPath);
				if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
					Directory.CreateDirectory(directory);

				if (!string.IsNullOrEmpty(entry.Name))
				{
					await using var entryStream = entry.Open();
					await using var fileStream = File.Create(fullPath);
					await entryStream.CopyToAsync(fileStream, cancellationToken);
				}
			}

			extractStopwatch.Stop();
			_logger.LogInformation(
				"ZIP archive extracted to {TempDir} in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				tempDir, extractStopwatch.ElapsedMilliseconds, extractStopwatch.Elapsed.TotalSeconds);

			// Convert Shapefile to GeoJSON
			var shpFile = Directory.GetFiles(tempDir, "ne_10m_admin_0_countries.shp", SearchOption.AllDirectories)
				              .FirstOrDefault()
			              ?? Directory.GetFiles(tempDir, "*.shp", SearchOption.AllDirectories).FirstOrDefault();

			if (shpFile == null) throw new FileNotFoundException("Shapefile (.shp) not found in ZIP archive");

			_logger.LogInformation("Found shapefile: {ShpFile}", shpFile);
			_logger.LogInformation("Converting Shapefile to GeoJSON for processing...");

			// Use ogr2ogr to read Shapefile and convert to GeoJSON
			var convertStopwatch = Stopwatch.StartNew();
			var geoJson = await ConvertShapefileToGeoJsonAsync(shpFile, cancellationToken);
			convertStopwatch.Stop();
			_logger.LogInformation(
				"Shapefile converted to GeoJSON in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				convertStopwatch.ElapsedMilliseconds, convertStopwatch.Elapsed.TotalMinutes);

			var importStopwatch = Stopwatch.StartNew();
			_logger.LogInformation("Importing countries to database...");
			await _databaseWriterService.ImportCountriesFromGeoJsonAsync(geoJson, cancellationToken);
			importStopwatch.Stop();
			_logger.LogInformation("Countries imported to database in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				importStopwatch.ElapsedMilliseconds, importStopwatch.Elapsed.TotalSeconds);

			overallStopwatch.Stop();
			_logger.LogInformation(
				"Admin 0 Countries Shapefile processing completed in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
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
				_logger.LogWarning(ex, "Failed to delete temporary directory {TempDir}", tempDir);
			}
		}
	}

	private async Task ProcessAdmin1ShapefileZipAsync(Stream zipStream, CancellationToken cancellationToken)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Extracting and processing Admin 1 Shapefile ZIP...");

		var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
		Directory.CreateDirectory(tempDir);

		try
		{
			// Extract ZIP
			var extractStopwatch = Stopwatch.StartNew();
			using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, false);
			foreach (var entry in archive.Entries)
			{
				var fullPath = Path.Combine(tempDir, entry.FullName);
				var directory = Path.GetDirectoryName(fullPath);
				if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
					Directory.CreateDirectory(directory);

				if (!string.IsNullOrEmpty(entry.Name))
				{
					await using var entryStream = entry.Open();
					await using var fileStream = File.Create(fullPath);
					await entryStream.CopyToAsync(fileStream, cancellationToken);
				}
			}

			extractStopwatch.Stop();
			_logger.LogInformation(
				"ZIP archive extracted to {TempDir} in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				tempDir, extractStopwatch.ElapsedMilliseconds, extractStopwatch.Elapsed.TotalSeconds);

			// Convert Shapefile to GeoJSON using NetTopologySuite
			var shpFile =
				Directory.GetFiles(tempDir, "ne_10m_admin_1_states_provinces.shp", SearchOption.AllDirectories)
					.FirstOrDefault()
				?? Directory.GetFiles(tempDir, "*.shp", SearchOption.AllDirectories).FirstOrDefault();

			if (shpFile == null) throw new FileNotFoundException("Shapefile (.shp) not found in ZIP archive");

			_logger.LogInformation("Found shapefile: {ShpFile}", shpFile);
			_logger.LogInformation("Converting Shapefile to GeoJSON for processing...");

			// Use NetTopologySuite to read Shapefile and convert to GeoJSON
			var convertStopwatch = Stopwatch.StartNew();
			var geoJson = await ConvertShapefileToGeoJsonAsync(shpFile, cancellationToken);
			convertStopwatch.Stop();
			_logger.LogInformation(
				"Shapefile converted to GeoJSON in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				convertStopwatch.ElapsedMilliseconds, convertStopwatch.Elapsed.TotalMinutes);

			var importStopwatch = Stopwatch.StartNew();
			_logger.LogInformation("Importing regions to database...");
			await _databaseWriterService.ImportRegionsFromGeoJsonAsync(geoJson, cancellationToken);
			importStopwatch.Stop();
			_logger.LogInformation("Regions imported to database in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				importStopwatch.ElapsedMilliseconds, importStopwatch.Elapsed.TotalSeconds);

			overallStopwatch.Stop();
			_logger.LogInformation(
				"Admin 1 Shapefile processing completed in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
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
				_logger.LogWarning(ex, "Failed to delete temporary directory {TempDir}", tempDir);
			}
		}
	}

	private async Task ProcessPopulatedPlacesShapefileZipAsync(Stream zipStream, CancellationToken cancellationToken)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Extracting and processing Populated Places Shapefile ZIP...");

		var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
		Directory.CreateDirectory(tempDir);

		try
		{
			// Extract ZIP
			var extractStopwatch = Stopwatch.StartNew();
			using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, false);
			foreach (var entry in archive.Entries)
			{
				var fullPath = Path.Combine(tempDir, entry.FullName);
				var directory = Path.GetDirectoryName(fullPath);
				if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
					Directory.CreateDirectory(directory);

				if (!string.IsNullOrEmpty(entry.Name))
				{
					await using var entryStream = entry.Open();
					await using var fileStream = File.Create(fullPath);
					await entryStream.CopyToAsync(fileStream, cancellationToken);
				}
			}

			extractStopwatch.Stop();
			_logger.LogInformation(
				"ZIP archive extracted to {TempDir} in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				tempDir, extractStopwatch.ElapsedMilliseconds, extractStopwatch.Elapsed.TotalSeconds);

			// Convert Shapefile to GeoJSON
			var shpFile = Directory.GetFiles(tempDir, "ne_10m_populated_places.shp", SearchOption.AllDirectories)
				              .FirstOrDefault()
			              ?? Directory.GetFiles(tempDir, "*.shp", SearchOption.AllDirectories).FirstOrDefault();

			if (shpFile == null) throw new FileNotFoundException("Shapefile (.shp) not found in ZIP archive");

			_logger.LogInformation("Found shapefile: {ShpFile}", shpFile);
			_logger.LogInformation("Converting Shapefile to GeoJSON for processing...");

			// Use NetTopologySuite to read Shapefile and convert to GeoJSON
			var convertStopwatch = Stopwatch.StartNew();
			var geoJson = await ConvertShapefileToGeoJsonAsync(shpFile, cancellationToken);
			convertStopwatch.Stop();
			_logger.LogInformation(
				"Shapefile converted to GeoJSON in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				convertStopwatch.ElapsedMilliseconds, convertStopwatch.Elapsed.TotalMinutes);

			var importStopwatch = Stopwatch.StartNew();
			_logger.LogInformation("Importing cities to database...");
			await _databaseWriterService.ImportCitiesFromGeoJsonAsync(geoJson, cancellationToken);
			importStopwatch.Stop();
			_logger.LogInformation("Cities imported to database in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				importStopwatch.ElapsedMilliseconds, importStopwatch.Elapsed.TotalSeconds);

			overallStopwatch.Stop();
			_logger.LogInformation(
				"Populated Places Shapefile processing completed in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
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
				_logger.LogWarning(ex, "Failed to delete temporary directory {TempDir}", tempDir);
			}
		}
	}

	private async Task<string> ConvertShapefileToGeoJsonAsync(string shpFilePath, CancellationToken cancellationToken)
	{
		// Use ogr2ogr (GDAL) to convert Shapefile to GeoJSON
		// This is the standard tool for geospatial data conversion
		// Natural Earth provides data in Shapefile format, so we use GDAL for conversion

		_logger.LogInformation("Converting Shapefile to GeoJSON format using ogr2ogr...");

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

			_logger.LogInformation("Successfully converted Shapefile to GeoJSON");
			var geoJsonContent = await File.ReadAllTextAsync(geoJsonFile, cancellationToken);

			// Clean up temporary GeoJSON file
			try
			{
				File.Delete(geoJsonFile);
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Failed to delete temporary GeoJSON file");
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
			_logger.LogError(ex, "Failed to convert Shapefile to GeoJSON");
			throw;
		}
	}

	/// <summary>
	///     Downloads and loads timezone boundaries from timezone-boundary-builder
	///     Source: https://github.com/evansiroky/timezone-boundary-builder/releases
	/// </summary>
	public async Task LoadTimezonesAsync(CancellationToken cancellationToken = default)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Starting to load timezones from timezone-boundary-builder...");

		// Timezone Boundary Builder provides GeoJSON files with timezone boundaries
		// The latest release can be downloaded from GitHub releases
		// Note: The ZIP may contain different file names depending on the release
		var downloadUrls = new[]
		{
			// Try direct download from GitHub releases (latest versions)
			"https://github.com/evansiroky/timezone-boundary-builder/releases/download/2024d/timezones.geojson.zip",
			"https://github.com/evansiroky/timezone-boundary-builder/releases/download/2024c/timezones.geojson.zip",
			"https://github.com/evansiroky/timezone-boundary-builder/releases/download/2024b/timezones.geojson.zip",
			"https://github.com/evansiroky/timezone-boundary-builder/releases/download/2024a/timezones.geojson.zip",
			// Alternative: try combined-shapefile-with-oceans.zip
			"https://github.com/evansiroky/timezone-boundary-builder/releases/download/2024d/combined-shapefile-with-oceans.zip"
		};

		Exception? lastException = null;

		foreach (var url in downloadUrls)
			try
			{
				_logger.LogInformation("Attempting to download timezones GeoJSON from {Url}...", url);

				using var httpClient = _httpClientFactory.CreateClient();
				httpClient.Timeout = TimeSpan.FromMinutes(30); // Very large file, increase timeout

				var downloadStopwatch = Stopwatch.StartNew();
				var response =
					await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

				if (!response.IsSuccessStatusCode)
				{
					_logger.LogWarning("Failed to download from {Url}: {StatusCode}", url, response.StatusCode);
					continue;
				}

				await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);
				downloadStopwatch.Stop();
				_logger.LogInformation("Download completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
					downloadStopwatch.ElapsedMilliseconds, downloadStopwatch.Elapsed.TotalSeconds);

				await ProcessTimezonesGeoJsonZipAsync(zipStream, cancellationToken);

				overallStopwatch.Stop();
				_logger.LogInformation(
					"Successfully loaded timezones from {Url} in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
					url, overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
				return;
			}
			catch (HttpRequestException ex)
			{
				_logger.LogWarning(ex, "Failed to download timezones GeoJSON from {Url}", url);
				lastException = ex;
			}
			catch (Exception ex)
			{
				_logger.LogWarning(ex, "Error processing timezones GeoJSON from {Url}", url);
				lastException = ex;
			}

		// If all downloads failed, log instructions for manual download
		_logger.LogError(lastException, "Failed to download timezones GeoJSON from all sources");
		_logger.LogInformation("To load timezones manually:");
		_logger.LogInformation(
			"1. Download timezones.geojson.zip from: https://github.com/evansiroky/timezone-boundary-builder/releases");
		_logger.LogInformation("2. Extract timezones.geojson from the ZIP");
		_logger.LogInformation("3. Place the GeoJSON file in the working directory");
		_logger.LogInformation("4. The application will process it automatically if found in the current directory");

		throw new InvalidOperationException(
			"Failed to download timezones GeoJSON. Please download manually from https://github.com/evansiroky/timezone-boundary-builder/releases",
			lastException);
	}

	private async Task ProcessTimezonesGeoJsonZipAsync(Stream zipStream, CancellationToken cancellationToken)
	{
		var overallStopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Extracting and processing timezones GeoJSON ZIP...");

		var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
		Directory.CreateDirectory(tempDir);

		try
		{
			// Extract ZIP
			var extractStopwatch = Stopwatch.StartNew();
			using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, false);
			foreach (var entry in archive.Entries)
			{
				var fullPath = Path.Combine(tempDir, entry.FullName);
				var directory = Path.GetDirectoryName(fullPath);
				if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
					Directory.CreateDirectory(directory);

				if (!string.IsNullOrEmpty(entry.Name))
				{
					await using var entryStream = entry.Open();
					await using var fileStream = File.Create(fullPath);
					await entryStream.CopyToAsync(fileStream, cancellationToken);
				}
			}

			extractStopwatch.Stop();
			_logger.LogInformation(
				"ZIP archive extracted to {TempDir} in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				tempDir, extractStopwatch.ElapsedMilliseconds, extractStopwatch.Elapsed.TotalSeconds);

			// Find GeoJSON file - timezone-boundary-builder may have different file names
			// Log available files for debugging
			var allFilesInArchive = Directory.GetFiles(tempDir, "*", SearchOption.AllDirectories);
			_logger.LogInformation("Files found in ZIP archive: {FileCount}", allFilesInArchive.Length);
			if (allFilesInArchive.Length <= 20)
				foreach (var file in allFilesInArchive)
					_logger.LogInformation("  Found file: {FileName}", Path.GetRelativePath(tempDir, file));

			// Timezone Boundary Builder uses different file names in different releases
			// Try various possible names, including .json extension (not just .geojson)
			var geoJsonFile = Directory.GetFiles(tempDir, "timezones.geojson", SearchOption.AllDirectories)
				                  .FirstOrDefault()
			                  ?? Directory.GetFiles(tempDir, "combined.json", SearchOption.AllDirectories)
				                  .FirstOrDefault()
			                  ?? Directory.GetFiles(tempDir, "combined-shapefile-with-oceans.geojson",
				                  SearchOption.AllDirectories).FirstOrDefault()
			                  ?? Directory.GetFiles(tempDir, "combined-shapefile.geojson", SearchOption.AllDirectories)
				                  .FirstOrDefault()
			                  ?? Directory.GetFiles(tempDir, "*.geojson", SearchOption.AllDirectories).FirstOrDefault()
			                  ?? Directory.GetFiles(tempDir, "*.json", SearchOption.AllDirectories).FirstOrDefault();

			if (geoJsonFile == null)
			{
				_logger.LogError("GeoJSON file not found in ZIP archive. Files found: {FileCount}",
					allFilesInArchive.Length);
				foreach (var file in allFilesInArchive.Take(20))
					_logger.LogError("  Found: {FileName}", Path.GetRelativePath(tempDir, file));
				throw new FileNotFoundException(
					"GeoJSON file (timezones.geojson, combined.json, or combined-shapefile*.geojson) not found in ZIP archive");
			}

			_logger.LogInformation("Found GeoJSON file: {GeoJsonFile}", geoJsonFile);

			// Read and import GeoJSON
			var importStopwatch = Stopwatch.StartNew();
			var geoJson = await File.ReadAllTextAsync(geoJsonFile, cancellationToken);
			await _databaseWriterService.ImportTimezonesFromGeoJsonAsync(geoJson, cancellationToken);
			importStopwatch.Stop();
			_logger.LogInformation("Timezones imported to database in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
				importStopwatch.ElapsedMilliseconds, importStopwatch.Elapsed.TotalSeconds);

			overallStopwatch.Stop();
			_logger.LogInformation(
				"Timezones GeoJSON processing completed in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)",
				overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
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
				_logger.LogWarning(ex, "Failed to delete temporary directory {TempDir}", tempDir);
			}
		}
	}
}