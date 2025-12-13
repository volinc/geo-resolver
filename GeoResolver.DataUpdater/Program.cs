using GeoResolver.DataUpdater.Services;
using GeoResolver.DataUpdater.Services.DataLoaders;
using Medallion.Threading;
using Medallion.Threading.Postgres;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater;

internal class Program
{
    private static async Task<int> Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        var connectionString = configuration.GetConnectionString("DefaultConnection")
                               ?? throw new InvalidOperationException("'DefaultConnection' connection string is missing. Please configure it in appsettings.json or via environment variable ConnectionStrings__DefaultConnection.");

        var services = new ServiceCollection();

        // Configure logging
        services.AddLogging(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });

        // Configure database
        services.AddNpgsqlDataSource(connectionString);
        services.AddSingleton<IDistributedLockProvider>(_ => 
            new PostgresDistributedSynchronizationProvider(connectionString));

        // Configure HTTP client
        services.AddHttpClient();

        // Register services
        services.AddSingleton<IDatabaseWriterService, DatabaseWriterService>();
        services.AddSingleton<IDataLoader, DataLoader>();

        var serviceProvider = services.BuildServiceProvider();

        var logger = serviceProvider.GetRequiredService<ILogger<Program>>();
        var distributedLockProvider = serviceProvider.GetRequiredService<IDistributedLockProvider>();
        var databaseWriterService = serviceProvider.GetRequiredService<IDatabaseWriterService>();
        var dataLoader = serviceProvider.GetRequiredService<IDataLoader>();

        try
        {
            logger.LogInformation("Starting data loading process...");

            // Use distributed lock to prevent concurrent runs
            var distributedLock = distributedLockProvider.CreateLock("geo_resolver_data_update");
            
            await using var handle = await distributedLock.TryAcquireAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
            if (handle == null)
            {
                logger.LogWarning("Another data loading process is already running (lock acquired). Exiting.");
                return 1;
            }

            logger.LogInformation("Acquired lock, starting data update...");

            // Initialize database (ensure tables exist)
            await databaseWriterService.InitializeAsync();

            // Load all data
            await dataLoader.LoadAllDataAsync();

            // Update last update time
            await databaseWriterService.SetLastUpdateTimeAsync(DateTimeOffset.UtcNow);

            logger.LogInformation("Data loading completed successfully!");
            return 0;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error during data loading");
            return 1;
        }
        finally
        {
            await serviceProvider.DisposeAsync();
        }
    }
}
