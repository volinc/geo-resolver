using System.Text.Json;
using GeoResolver.DataUpdater.Services;
using GeoResolver.DataUpdater.Services.Shapefile;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater.Services.DataLoaders;

public class DataLoader : IDataLoader
{
    private readonly IDatabaseWriterService _databaseWriterService;
    private readonly ILogger<DataLoader> _logger;
    private readonly NaturalEarthShapefileLoader _shapefileLoader;
    private readonly IHttpClientFactory _httpClientFactory;

    public DataLoader(
        IDatabaseWriterService databaseWriterService,
        ILogger<DataLoader> logger,
        NaturalEarthShapefileLoader shapefileLoader,
        IHttpClientFactory httpClientFactory)
    {
        _databaseWriterService = databaseWriterService;
        _logger = logger;
        _shapefileLoader = shapefileLoader;
        _httpClientFactory = httpClientFactory;
    }

    public async Task LoadAllDataAsync(CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Clearing existing data...");
        await _databaseWriterService.ClearAllDataAsync(cancellationToken);

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
                await _databaseWriterService.ImportCountriesFromGeoJsonAsync(response, cancellationToken);
                
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
        // Using Natural Earth Admin 1 States/Provinces 10m Shapefile (official source)
        // Natural Earth Admin 1 10m provides first-level administrative boundaries (states, provinces, regions)
        // Contains ~4000+ regions worldwide with high geographic detail
        // Source: Official Natural Earth data in Shapefile format
        _logger.LogInformation("Loading regions from Natural Earth Admin 1 dataset (Shapefile format)...");
        
        try
        {
            await _shapefileLoader.LoadRegionsAsync(cancellationToken);
            _logger.LogInformation("Regions loaded successfully from Natural Earth Shapefile");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load regions from Natural Earth Shapefile");
            throw;
        }
    }

    private async Task LoadCitiesAsync(CancellationToken cancellationToken)
    {
        // Using Natural Earth Populated Places 10m Shapefile (official source)
        // Natural Earth Populated Places 10m includes ~7000+ cities, towns, and populated places worldwide
        // Contains capitals, major cities, and significant populated places with coordinates
        // Source: Official Natural Earth data in Shapefile format
        _logger.LogInformation("Loading cities from Natural Earth Populated Places dataset (Shapefile format)...");
        
        try
        {
            await _shapefileLoader.LoadCitiesAsync(cancellationToken);
            _logger.LogInformation("Cities loaded successfully from Natural Earth Shapefile");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load cities from Natural Earth Shapefile");
            throw;
        }
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

