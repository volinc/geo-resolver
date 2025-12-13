using GeoResolver.Services.DataLoaders;

namespace GeoResolver.Services;

public class DataUpdateBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<DataUpdateBackgroundService> _logger;
    private readonly IConfiguration _configuration;
    private readonly TimeSpan _updateInterval;

    public DataUpdateBackgroundService(
        IServiceProvider serviceProvider,
        ILogger<DataUpdateBackgroundService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _configuration = configuration;

        var intervalDays = _configuration.GetValue<int>("DataUpdate:IntervalDays", 365);
        _updateInterval = TimeSpan.FromDays(intervalDays);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait a bit to ensure database is ready
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        // Try to update on first start
        var shouldUpdateOnStart = _configuration.GetValue<bool>("DataUpdate:UpdateOnFirstStart", true);
        if (shouldUpdateOnStart)
        {
            await TryUpdateDataAsync(stoppingToken);
        }

        // Periodic updates
        // Task.Delay has a maximum of ~24.8 days, so we break large intervals into smaller chunks
        const int maxDelayDays = 24; // Use 24 days as safe maximum
        var maxDelay = TimeSpan.FromDays(maxDelayDays);
        
        while (!stoppingToken.IsCancellationRequested)
        {
            var remainingInterval = _updateInterval;
            while (remainingInterval > TimeSpan.Zero && !stoppingToken.IsCancellationRequested)
            {
                var delay = remainingInterval > maxDelay ? maxDelay : remainingInterval;
                await Task.Delay(delay, stoppingToken);
                remainingInterval = remainingInterval > maxDelay 
                    ? remainingInterval - maxDelay 
                    : TimeSpan.Zero;
            }
            
            if (!stoppingToken.IsCancellationRequested)
            {
                await TryUpdateDataAsync(stoppingToken);
            }
        }
    }

    private async Task TryUpdateDataAsync(CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var databaseService = scope.ServiceProvider.GetRequiredService<IDatabaseService>();
        var dataLoader = scope.ServiceProvider.GetRequiredService<IDataLoader>();

        const string lockName = "data_update";
        var lockTimeout = TimeSpan.FromHours(24); // Lock expires after 24 hours

        var lockAcquired = await databaseService.TryAcquireLockAsync(lockName, lockTimeout, cancellationToken);
        if (!lockAcquired)
        {
            _logger.LogInformation("Data update lock already acquired by another instance, skipping update");
            return;
        }

        try
        {
            _logger.LogInformation("Starting data update...");
            await databaseService.InitializeAsync(cancellationToken);
            await dataLoader.LoadAllDataAsync(cancellationToken);
            _logger.LogInformation("Data update completed successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during data update");
        }
        finally
        {
            await databaseService.ReleaseLockAsync(lockName, cancellationToken);
        }
    }
}

