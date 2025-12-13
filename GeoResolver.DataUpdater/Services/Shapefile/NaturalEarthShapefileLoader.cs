using System.IO.Compression;
using System.Text.Json;
using GeoResolver.DataUpdater.Models;
using GeoResolver.DataUpdater.Services;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Logging;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace GeoResolver.DataUpdater.Services.Shapefile;

/// <summary>
/// Loads and processes Natural Earth Shapefile data
/// Converts Shapefile to GeoJSON format for processing
/// </summary>
public class NaturalEarthShapefileLoader
{
    private readonly ILogger<NaturalEarthShapefileLoader> _logger;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IDatabaseWriterService _databaseWriterService;

    // Natural Earth official download URLs for 10m datasets
    private const string Admin1StatesProvincesUrl = "https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-1-states-provinces/";
    private const string PopulatedPlacesUrl = "https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-populated-places/";

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
    /// Downloads Natural Earth Admin 1 (States/Provinces) Shapefile ZIP and processes it
    /// </summary>
    public async Task LoadRegionsAsync(CancellationToken cancellationToken = default)
    {
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
        {
            try
            {
                _logger.LogInformation("Attempting to download Admin 1 Shapefile from {Url}...", url);
                
                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(15); // Large file, increase timeout
                
                var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning("Failed to download from {Url}: {StatusCode}", url, response.StatusCode);
                    continue;
                }

                await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);
                await ProcessAdmin1ShapefileZipAsync(zipStream, cancellationToken);
                
                _logger.LogInformation("Successfully loaded regions from {Url}", url);
                return;
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(ex, "Failed to download Admin 1 Shapefile from {Url}", url);
                lastException = ex;
                continue;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing Admin 1 Shapefile from {Url}", url);
                lastException = ex;
                continue;
            }
        }

        // If all downloads failed, log instructions for manual download
        _logger.LogError(lastException, "Failed to download Admin 1 Shapefile from all sources");
        _logger.LogInformation("To load regions manually:");
        _logger.LogInformation("1. Download ne_10m_admin_1_states_provinces.zip from: {Url}", Admin1StatesProvincesUrl);
        _logger.LogInformation("2. Place the ZIP file in the working directory or provide a local path");
        _logger.LogInformation("3. The application will process it automatically if found in the current directory");
        
        throw new InvalidOperationException("Failed to download Admin 1 Shapefile. Please download manually and place in working directory.", lastException);
    }

    /// <summary>
    /// Downloads Natural Earth Populated Places Shapefile ZIP and processes it
    /// </summary>
    public async Task LoadCitiesAsync(CancellationToken cancellationToken = default)
    {
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
        {
            try
            {
                _logger.LogInformation("Attempting to download Populated Places Shapefile from {Url}...", url);
                
                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(15); // Large file, increase timeout
                
                var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning("Failed to download from {Url}: {StatusCode}", url, response.StatusCode);
                    continue;
                }

                await using var zipStream = await response.Content.ReadAsStreamAsync(cancellationToken);
                await ProcessPopulatedPlacesShapefileZipAsync(zipStream, cancellationToken);
                
                _logger.LogInformation("Successfully loaded cities from {Url}", url);
                return;
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(ex, "Failed to download Populated Places Shapefile from {Url}", url);
                lastException = ex;
                continue;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing Populated Places Shapefile from {Url}", url);
                lastException = ex;
                continue;
            }
        }

        // If all downloads failed, log instructions for manual download
        _logger.LogError(lastException, "Failed to download Populated Places Shapefile from all sources");
        _logger.LogInformation("To load cities manually:");
        _logger.LogInformation("1. Download ne_10m_populated_places.zip from: {Url}", PopulatedPlacesUrl);
        _logger.LogInformation("2. Place the ZIP file in the working directory or provide a local path");
        _logger.LogInformation("3. The application will process it automatically if found in the current directory");
        
        throw new InvalidOperationException("Failed to download Populated Places Shapefile. Please download manually and place in working directory.", lastException);
    }

    private async Task ProcessAdmin1ShapefileZipAsync(Stream zipStream, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Extracting and processing Admin 1 Shapefile ZIP...");
        
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Extract ZIP
            using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, leaveOpen: false);
            foreach (var entry in archive.Entries)
            {
                var fullPath = Path.Combine(tempDir, entry.FullName);
                var directory = Path.GetDirectoryName(fullPath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                if (!string.IsNullOrEmpty(entry.Name))
                {
                    await using var entryStream = entry.Open();
                    await using var fileStream = File.Create(fullPath);
                    await entryStream.CopyToAsync(fileStream, cancellationToken);
                }
            }

            _logger.LogInformation("ZIP archive extracted to {TempDir}", tempDir);

            // Convert Shapefile to GeoJSON using NetTopologySuite
            var shpFile = Directory.GetFiles(tempDir, "ne_10m_admin_1_states_provinces.shp", SearchOption.AllDirectories).FirstOrDefault()
                         ?? Directory.GetFiles(tempDir, "*.shp", SearchOption.AllDirectories).FirstOrDefault();

            if (shpFile == null)
            {
                throw new FileNotFoundException("Shapefile (.shp) not found in ZIP archive");
            }

            _logger.LogInformation("Found shapefile: {ShpFile}", shpFile);
            _logger.LogInformation("Converting Shapefile to GeoJSON for processing...");

            // Use NetTopologySuite to read Shapefile and convert to GeoJSON
            var geoJson = await ConvertShapefileToGeoJsonAsync(shpFile, cancellationToken);
            
            _logger.LogInformation("Shapefile converted to GeoJSON, importing regions...");
            await _databaseWriterService.ImportRegionsFromGeoJsonAsync(geoJson, cancellationToken);
            
            _logger.LogInformation("Regions import completed successfully");
        }
        finally
        {
            // Cleanup
            try
            {
                Directory.Delete(tempDir, recursive: true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete temporary directory {TempDir}", tempDir);
            }
        }
    }

    private async Task ProcessPopulatedPlacesShapefileZipAsync(Stream zipStream, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Extracting and processing Populated Places Shapefile ZIP...");
        
        var tempDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(tempDir);

        try
        {
            // Extract ZIP
            using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, leaveOpen: false);
            foreach (var entry in archive.Entries)
            {
                var fullPath = Path.Combine(tempDir, entry.FullName);
                var directory = Path.GetDirectoryName(fullPath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                if (!string.IsNullOrEmpty(entry.Name))
                {
                    await using var entryStream = entry.Open();
                    await using var fileStream = File.Create(fullPath);
                    await entryStream.CopyToAsync(fileStream, cancellationToken);
                }
            }

            _logger.LogInformation("ZIP archive extracted to {TempDir}", tempDir);

            // Convert Shapefile to GeoJSON
            var shpFile = Directory.GetFiles(tempDir, "ne_10m_populated_places.shp", SearchOption.AllDirectories).FirstOrDefault()
                         ?? Directory.GetFiles(tempDir, "*.shp", SearchOption.AllDirectories).FirstOrDefault();

            if (shpFile == null)
            {
                throw new FileNotFoundException("Shapefile (.shp) not found in ZIP archive");
            }

            _logger.LogInformation("Found shapefile: {ShpFile}", shpFile);
            _logger.LogInformation("Converting Shapefile to GeoJSON for processing...");

            // Use NetTopologySuite to read Shapefile and convert to GeoJSON
            var geoJson = await ConvertShapefileToGeoJsonAsync(shpFile, cancellationToken);
            
            _logger.LogInformation("Shapefile converted to GeoJSON, importing cities...");
            await _databaseWriterService.ImportCitiesFromGeoJsonAsync(geoJson, cancellationToken);
            
            _logger.LogInformation("Cities import completed successfully");
        }
        finally
        {
            // Cleanup
            try
            {
                Directory.Delete(tempDir, recursive: true);
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
            var processInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = "ogr2ogr",
                Arguments = $"-f GeoJSON \"{geoJsonFile}\" \"{shpFilePath}\" -lco RFC7946=YES",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = System.Diagnostics.Process.Start(processInfo);
            if (process == null)
            {
                throw new InvalidOperationException("Failed to start ogr2ogr process");
            }

            var errorOutput = await process.StandardError.ReadToEndAsync(cancellationToken);
            await process.WaitForExitAsync(cancellationToken);
            
            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"ogr2ogr failed with exit code {process.ExitCode}: {errorOutput}");
            }

            if (!File.Exists(geoJsonFile))
            {
                throw new FileNotFoundException($"GeoJSON file was not created: {geoJsonFile}");
            }

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
        catch (System.ComponentModel.Win32Exception ex) when (ex.NativeErrorCode == 2) // File not found
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
}

