using GeoResolver.Services.DataLoaders;
using Medallion.Threading;

namespace GeoResolver.Services;

public sealed class DataUpdateBackgroundService : BackgroundService
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IDistributedLockProvider _distributedLockProvider;
    private readonly ILogger<DataUpdateBackgroundService> _logger;
    private readonly TimeSpan _checkInterval;
    private readonly TimeSpan _updateInterval;
    private readonly bool _forceUpdateOnStart;

    public DataUpdateBackgroundService(
        IServiceScopeFactory serviceScopeFactory,
        IDistributedLockProvider distributedLockProvider,
        ILogger<DataUpdateBackgroundService> logger,
        IConfiguration configuration)
    {
        _serviceScopeFactory = serviceScopeFactory;
        _distributedLockProvider = distributedLockProvider;
        _logger = logger;

        // Check for updates every 24 hours
        _checkInterval = TimeSpan.FromHours(24);
        
        // How old data can be before requiring update (default: 365 days)
        var intervalDays = configuration.GetValue("DataUpdate:IntervalDays", 365);
        _updateInterval = TimeSpan.FromDays(intervalDays);
        
        // Force update on start, ignoring last update time from DB
        _forceUpdateOnStart = configuration.GetValue("DataUpdate:ForceUpdateOnStart", false);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait a bit to ensure database is ready
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        // Force update on start if configured
        if (_forceUpdateOnStart)
        {
            _logger.LogInformation("Force update on start is enabled, updating data immediately");
            await TryUpdateDataAsync(stoppingToken);
        }

        // Periodic checks (every hour)
        while (!stoppingToken.IsCancellationRequested)
        {
            await CheckAndUpdateIfNeededAsync(stoppingToken);
            await Task.Delay(_checkInterval, stoppingToken);
        }
    }

    private async Task CheckAndUpdateIfNeededAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceScopeFactory.CreateScope();
        var databaseService = scope.ServiceProvider.GetRequiredService<IDatabaseService>();

        try
        {
            var lastUpdateTime = await databaseService.GetLastUpdateTimeAsync(cancellationToken);
            var now = DateTimeOffset.UtcNow;
            
            bool needsUpdate = false;
            
            if (lastUpdateTime == null)
            {
                _logger.LogInformation("No previous update time found, data update is needed");
                needsUpdate = true;
            }
            else
            {
                var timeSinceLastUpdate = now - lastUpdateTime.Value;
                if (timeSinceLastUpdate >= _updateInterval)
                {
                    _logger.LogInformation(
                        "Data is outdated (last update: {LastUpdate}, age: {Age}), update is needed",
                        lastUpdateTime.Value,
                        timeSinceLastUpdate);
                    needsUpdate = true;
                }
                else
                {
                    _logger.LogDebug(
                        "Data is still fresh (last update: {LastUpdate}, age: {Age}), no update needed",
                        lastUpdateTime.Value,
                        timeSinceLastUpdate);
                }
            }

            if (needsUpdate)
            {
                await TryUpdateDataAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking data update status");
        }
    }

    private async Task TryUpdateDataAsync(CancellationToken cancellationToken)
    {
        var distributedLock = _distributedLockProvider.CreateLock("geo_resolver_data_update");

        await using var handle = await distributedLock.TryAcquireAsync(TimeSpan.FromSeconds(30), cancellationToken);
        if (handle == null)
        {
            _logger.LogInformation("Data update lock already acquired by another instance, skipping update");
            return;
        }

        try
        {
            using var scope = _serviceScopeFactory.CreateScope();
            var databaseService = scope.ServiceProvider.GetRequiredService<IDatabaseService>();
            var dataLoader = scope.ServiceProvider.GetRequiredService<IDataLoader>();

            _logger.LogInformation("Starting data update...");
            await databaseService.InitializeAsync(cancellationToken);
            await dataLoader.LoadAllDataAsync(cancellationToken);
            
            // Update last update time
            await databaseService.SetLastUpdateTimeAsync(DateTimeOffset.UtcNow, cancellationToken);
            
            _logger.LogInformation("Data update completed successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during data update");
        }
    }
}
