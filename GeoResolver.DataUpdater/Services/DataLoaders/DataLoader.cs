using GeoResolver.DataUpdater.Services.Osm;
using GeoResolver.DataUpdater.Services.Shapefile;
using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater.Services.DataLoaders;

public class DataLoader : IDataLoader
{
    private readonly IDatabaseWriterService _databaseWriterService;
    private readonly ILogger<DataLoader> _logger;
    private readonly NaturalEarthShapefileLoader _shapefileLoader;
    private readonly OsmCityLoader _osmCityLoader;

    public DataLoader(
        IDatabaseWriterService databaseWriterService,
        ILogger<DataLoader> logger,
        NaturalEarthShapefileLoader shapefileLoader,
        OsmCityLoader osmCityLoader)
    {
        _databaseWriterService = databaseWriterService;
        _logger = logger;
        _shapefileLoader = shapefileLoader;
        _osmCityLoader = osmCityLoader;
    }

    public async Task LoadAllDataAsync(CancellationToken cancellationToken = default)
    {
        var overallStopwatch = System.Diagnostics.Stopwatch.StartNew();
        _logger.LogInformation("=== Starting data loading process ===");

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        _logger.LogInformation("Clearing existing data...");
        await _databaseWriterService.ClearAllDataAsync(cancellationToken);
        stopwatch.Stop();
        _logger.LogInformation("Clearing data completed in {ElapsedMilliseconds}ms", stopwatch.ElapsedMilliseconds);

        stopwatch.Restart();
        _logger.LogInformation("Loading countries...");
        await LoadCountriesAsync(cancellationToken);
        stopwatch.Stop();
        _logger.LogInformation("Countries loading completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
            stopwatch.ElapsedMilliseconds, stopwatch.Elapsed.TotalSeconds);

        stopwatch.Restart();
        _logger.LogInformation("Loading regions...");
        await LoadRegionsAsync(cancellationToken);
        stopwatch.Stop();
        _logger.LogInformation("Regions loading completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
            stopwatch.ElapsedMilliseconds, stopwatch.Elapsed.TotalSeconds);

        stopwatch.Restart();
        _logger.LogInformation("Loading cities...");
        await LoadCitiesAsync(cancellationToken);
        stopwatch.Stop();
        _logger.LogInformation("Cities loading completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
            stopwatch.ElapsedMilliseconds, stopwatch.Elapsed.TotalSeconds);

        stopwatch.Restart();
        _logger.LogInformation("Loading timezones...");
        await LoadTimezonesAsync(cancellationToken);
        stopwatch.Stop();
        _logger.LogInformation("Timezones loading completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)",
            stopwatch.ElapsedMilliseconds, stopwatch.Elapsed.TotalSeconds);

        overallStopwatch.Stop();
        _logger.LogInformation(
            "=== Data loading process completed in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes) ===",
            overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
    }

    private async Task LoadCountriesAsync(CancellationToken cancellationToken)
    {
        // Using Natural Earth Admin 0 Countries 10m Shapefile (official source)
        // Natural Earth Admin 0 Countries 10m provides country boundaries with ISO codes
        // Source: Official Natural Earth data in Shapefile format
        // This ensures data consistency with regions and cities from the same source
        _logger.LogInformation("Loading countries from Natural Earth Admin 0 dataset (Shapefile format)...");

        try
        {
            await _shapefileLoader.LoadCountriesAsync(cancellationToken);
            _logger.LogInformation("Countries loaded successfully from Natural Earth Shapefile");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load countries from Natural Earth Shapefile");
            throw;
        }
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
        // Using OpenStreetMap via Overpass API to get city boundaries
        // OSM provides polygon boundaries for cities with place=city or place=town tags
        // and boundary=administrative with appropriate admin_level
        _logger.LogInformation("Loading cities from OpenStreetMap via Overpass API...");

        try
        {
            await _osmCityLoader.LoadCitiesAsync(cancellationToken);
            _logger.LogInformation("Cities loaded successfully from OpenStreetMap");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load cities from OpenStreetMap");
            _logger.LogWarning("City loading failed - cities table may remain empty or partially populated");
            // Don't throw - allow the process to continue with other data
        }
    }

    private async Task LoadTimezonesAsync(CancellationToken cancellationToken)
    {
        // Using timezone-boundary-builder data from GitHub
        // This provides accurate timezone boundaries with IANA timezone IDs
        // Source: https://github.com/evansiroky/timezone-boundary-builder/releases
        _logger.LogInformation("Loading timezones from timezone-boundary-builder dataset...");

        try
        {
            await _shapefileLoader.LoadTimezonesAsync(cancellationToken);
            _logger.LogInformation("Timezones loaded successfully from timezone-boundary-builder");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load timezones from timezone-boundary-builder");
            _logger.LogWarning("Timezone data loading failed - will use longitude-based approximation as fallback");
            // Don't throw - allow the process to continue with fallback timezone calculation
        }
    }
}

