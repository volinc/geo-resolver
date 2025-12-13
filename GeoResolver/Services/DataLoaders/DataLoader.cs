using System.Text.Json;
using GeoResolver.Models;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace GeoResolver.Services.DataLoaders;

public class DataLoader : IDataLoader
{
    private readonly IDatabaseService _databaseService;
    private readonly ILogger<DataLoader> _logger;
    private readonly IHttpClientFactory _httpClientFactory;

    public DataLoader(
        IDatabaseService databaseService,
        ILogger<DataLoader> logger,
        IHttpClientFactory httpClientFactory)
    {
        _databaseService = databaseService;
        _logger = logger;
        _httpClientFactory = httpClientFactory;
    }

    public async Task LoadAllDataAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Clearing existing data...");
        await _databaseService.ClearAllDataAsync(cancellationToken);

        _logger.LogInformation("Loading countries...");
        await LoadCountriesAsync(cancellationToken);

        _logger.LogInformation("Loading regions...");
        await LoadRegionsAsync(cancellationToken);

        _logger.LogInformation("Loading cities...");
        await LoadCitiesAsync(cancellationToken);

        _logger.LogInformation("Loading timezones...");
        await LoadTimezonesAsync(cancellationToken);

        _logger.LogInformation("Data loading completed");
    }

    private async Task LoadCountriesAsync(CancellationToken cancellationToken)
    {
        // Using Natural Earth 10m countries data - maximum detail suitable for parsing (~250KB)
        // Natural Earth 10m provides high detail country boundaries with ISO codes
        // File size is manageable for parsing while providing maximum geographic detail
        var urls = new[]
        {
            // Primary: GitHub source with countries data (works, ~250KB)
            "https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson",
            // Alternative: GitHub datasets repository (may have different structure)
            "https://raw.githubusercontent.com/datasets/geo-countries/main/data/countries.geojson"
        };
        
        Exception? lastException = null;
        
        foreach (var url in urls)
        {
            try
            {
                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(5);
                
                _logger.LogInformation("Downloading countries data from {Url}...", url);
                var response = await httpClient.GetStringAsync(url, cancellationToken);
                
                _logger.LogInformation("Parsing GeoJSON...");
                var jsonDoc = JsonDocument.Parse(response);
                var root = jsonDoc.RootElement;

                if (root.GetProperty("type").GetString() != "FeatureCollection")
                {
                    _logger.LogWarning("Invalid GeoJSON type from {Url}", url);
                    continue;
                }

                // Try to import - the method will skip features without ISO codes
                await _databaseService.ImportCountriesFromGeoJsonAsync(response, cancellationToken);
                
                // Check if we got any countries
                var features = root.GetProperty("features");
                var featureCount = features.GetArrayLength();
                _logger.LogInformation("Processed {Count} features from {Url}", featureCount, url);
                
                _logger.LogInformation("Countries import completed successfully");
                return; // Success, exit
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(ex, "Failed to download from {Url}, trying next source...", url);
                lastException = ex;
                continue;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing data from {Url}, trying next source...", url);
                lastException = ex;
                continue;
            }
        }
        
        // All sources failed
        _logger.LogError(lastException, "All data sources failed. Cannot load countries data.");
        throw new InvalidOperationException("Failed to load countries data from all available sources", lastException);
    }

    private async Task LoadRegionsAsync(CancellationToken cancellationToken)
    {
        // Using Natural Earth Admin 1 States/Provinces 10m - maximum detail suitable for parsing
        // Natural Earth Admin 1 10m provides first-level administrative boundaries (states, provinces, regions)
        // Contains ~4000+ regions worldwide with high geographic detail
        // File size: typically 15-30MB in GeoJSON format - manageable but requires adequate memory
        // Fields: name, name_en, admin, adm0_a3, iso_a2, postal, etc.
        // Note: Natural Earth official data is in Shapefile format, but there are converted GeoJSON versions available
        var urls = new[]
        {
            // Try potential sources for Natural Earth Admin 1 10m GeoJSON
            // These URLs may need to be updated based on available hosted sources
            "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_10m_admin_1_states_provinces.geojson"
            // Note: If CDN returns 403, data needs to be downloaded from Natural Earth and converted
        };
        
        Exception? lastException = null;
        
        foreach (var url in urls)
        {
            try
            {
                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(10); // Admin 1 file is large, increase timeout
                
                _logger.LogInformation("Downloading regions data from {Url}...", url);
                var response = await httpClient.GetStringAsync(url, cancellationToken);
                
                _logger.LogInformation("Parsing GeoJSON for regions...");
                var jsonDoc = JsonDocument.Parse(response);
                var root = jsonDoc.RootElement;

                if (root.GetProperty("type").GetString() != "FeatureCollection")
                {
                    _logger.LogWarning("Invalid GeoJSON type from {Url}", url);
                    continue;
                }

                // Check if this is actually Admin 1 data by looking at properties structure
                var features = root.GetProperty("features");
                if (features.GetArrayLength() == 0)
                {
                    _logger.LogWarning("No features found in {Url}", url);
                    continue;
                }

                // Try to import - the method will skip features without required data
                var featureCount = features.GetArrayLength();
                _logger.LogInformation("Found {Count} features in Admin 1 dataset from {Url}", featureCount, url);
                
                await _databaseService.ImportRegionsFromGeoJsonAsync(response, cancellationToken);
                
                _logger.LogInformation("Regions import completed from {Url}", url);
                return; // Success, exit
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(ex, "Failed to download from {Url}, trying next source...", url);
                lastException = ex;
                continue;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing data from {Url}, trying next source...", url);
                lastException = ex;
                continue;
            }
        }
        
        // All sources failed - log but don't throw, regions are optional
        _logger.LogWarning(lastException, "Could not load regions data from available sources");
        _logger.LogInformation("Regions data is optional. To load regions manually:");
        _logger.LogInformation("1. Download Natural Earth Admin 1 10m Shapefile from: https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-1-states-provinces/");
        _logger.LogInformation("2. Convert Shapefile to GeoJSON: ogr2ogr -f GeoJSON admin1.geojson ne_10m_admin_1_states_provinces.shp");
        _logger.LogInformation("3. Host the GeoJSON file and update the URL in LoadRegionsAsync");
    }

    private async Task LoadCitiesAsync(CancellationToken cancellationToken)
    {
        // Using Natural Earth Populated Places 10m - maximum detail suitable for parsing
        // Natural Earth Populated Places 10m includes ~7000+ cities, towns, and populated places worldwide
        // Contains capitals, major cities, and significant populated places with coordinates
        // File size: typically 5-10MB in GeoJSON format - manageable for parsing
        // Fields: name, nameascii, adm0name, adm0_a3, iso_a2, adm1name, geonameid, etc.
        // Note: Natural Earth official data is in Shapefile format, but there are converted GeoJSON versions available
        var urls = new[]
        {
            // Try potential sources for Natural Earth Populated Places 10m GeoJSON
            // These URLs may need to be updated based on available hosted sources
            "https://d2ad6b4ur7yvpq.cloudfront.net/naturalearth-3.3.0/ne_10m_populated_places.geojson"
            // Note: If CDN returns 403, data needs to be downloaded from Natural Earth and converted
        };
        
        Exception? lastException = null;
        
        foreach (var url in urls)
        {
            try
            {
                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(10); // Populated places file can be large
                
                _logger.LogInformation("Downloading cities data from {Url}...", url);
                var response = await httpClient.GetStringAsync(url, cancellationToken);
                
                _logger.LogInformation("Parsing GeoJSON for cities...");
                var jsonDoc = JsonDocument.Parse(response);
                var root = jsonDoc.RootElement;

                if (root.GetProperty("type").GetString() != "FeatureCollection")
                {
                    _logger.LogWarning("Invalid GeoJSON type from {Url}", url);
                    continue;
                }

                // Check if this is actually populated places data
                var features = root.GetProperty("features");
                if (features.GetArrayLength() == 0)
                {
                    _logger.LogWarning("No features found in {Url}", url);
                    continue;
                }

                // Try to import - the method will skip features without required data
                var featureCount = features.GetArrayLength();
                _logger.LogInformation("Found {Count} features in populated places dataset from {Url}", featureCount, url);
                
                await _databaseService.ImportCitiesFromGeoJsonAsync(response, cancellationToken);
                
                _logger.LogInformation("Cities import completed from {Url}", url);
                return; // Success, exit
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(ex, "Failed to download from {Url}, trying next source...", url);
                lastException = ex;
                continue;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing data from {Url}, trying next source...", url);
                lastException = ex;
                continue;
            }
        }
        
        // All sources failed - log but don't throw, cities are optional
        _logger.LogWarning(lastException, "Could not load cities data from available sources");
        _logger.LogInformation("Cities data is optional. To load cities manually:");
        _logger.LogInformation("1. Download Natural Earth Populated Places 10m Shapefile from: https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-populated-places/");
        _logger.LogInformation("2. Convert Shapefile to GeoJSON: ogr2ogr -f GeoJSON populated_places.geojson ne_10m_populated_places.shp");
        _logger.LogInformation("3. Host the GeoJSON file and update the URL in LoadCitiesAsync");
    }

    private async Task LoadTimezonesAsync(CancellationToken cancellationToken)
    {
        // Using timezone-boundary-builder data from GitHub
        // This provides accurate timezone boundaries with IANA timezone IDs
        // Note: The full file is very large, so for production you would need to:
        // 1. Download the ZIP file from: https://github.com/evansiroky/timezone-boundary-builder/releases
        // 2. Extract timezones.geojson
        // 3. Process and import
        
        // For now, timezone loading is skipped
        // The timezone calculation in DatabaseService.GetTimezoneOffsetAsync uses a longitude-based fallback
        _logger.LogInformation("Timezone data loading skipped - using longitude-based approximation");
    }

}

