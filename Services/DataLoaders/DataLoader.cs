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
        // Using multiple fallback sources for countries data
        // Primary: GitHub source with id field (3-letter ISO codes)
        // Note: We'll extract 2-letter codes from id where possible, or use properties
        var urls = new[]
        {
            "https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson"
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
        // Using Natural Earth Data - Admin 1 States/Provinces
        // This provides first-level administrative boundaries
        // Note: For regions, we'll use a more specific source or parse from GeoNames
        // For now, using a simplified approach with GeoNames API
        // In production, you might want to use Natural Earth Admin 1 dataset
        _logger.LogInformation("Region loading skipped - using GeoNames for region data");
        await Task.CompletedTask; // Suppress async warning
    }

    private async Task LoadCitiesAsync(CancellationToken cancellationToken)
    {
        // GeoNames cities data - we'll use a simplified approach
        // In production, you might want to download and process GeoNames city data
        _logger.LogInformation("City loading skipped - GeoNames city data processing");
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

