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
        // Using Natural Earth Data - Admin 0 Countries
        // Using a well-known GeoJSON source with ISO codes
        const string url = "https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson";
        
        try
        {
            using var httpClient = _httpClientFactory.CreateClient();
            var response = await httpClient.GetStringAsync(url, cancellationToken);
            var jsonDoc = JsonDocument.Parse(response);
            var root = jsonDoc.RootElement;

            if (root.GetProperty("type").GetString() != "FeatureCollection")
            {
                _logger.LogWarning("Invalid GeoJSON type");
                return;
            }

            var features = root.GetProperty("features");
            
            // Import directly using GeoJSON - let PostgreSQL parse it
            await _databaseService.ImportCountriesFromGeoJsonAsync(response, cancellationToken);
            _logger.LogInformation("Countries import completed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading countries");
        }
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

