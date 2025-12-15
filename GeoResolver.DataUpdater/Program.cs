using GeoResolver.DataUpdater;
using GeoResolver.DataUpdater.Logging;
using GeoResolver.DataUpdater.Services;
using GeoResolver.DataUpdater.Services.DataLoaders;
using GeoResolver.DataUpdater.Services.Shapefile;
using Medallion.Threading;
using Medallion.Threading.Postgres;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

var configuration = new ConfigurationBuilder()
	.SetBasePath(Directory.GetCurrentDirectory())
	.AddJsonFile("appsettings.json", false, true)
	.AddEnvironmentVariables()
	.Build();

var connectionString = configuration.GetConnectionString("DefaultConnection")
                       ?? throw new InvalidOperationException(
	                       "'DefaultConnection' connection string is missing. Please configure it in appsettings.json or via environment variable ConnectionStrings__DefaultConnection.");

var services = new ServiceCollection();

// Configure logging
var logFileName = $"georesolver-dataupdater-{DateTime.Now:yyyyMMdd-HHmmss}.log";
var logFilePath = Path.Combine(Directory.GetCurrentDirectory(), "logs", logFileName);
services.AddLogging(builder =>
{
	builder.AddConsole();
	builder.AddFile(logFilePath);
	builder.SetMinimumLevel(LogLevel.Information);
});
Console.WriteLine($"Logging to file: {logFilePath}");

// Configure database
services.AddNpgsqlDataSource(connectionString);
services.AddSingleton<IDistributedLockProvider>(_ =>
	new PostgresDistributedSynchronizationProvider(connectionString));

// Configure HTTP client
services.AddHttpClient();

// Configure options
services.AddOptions();
services.Configure<CityLoaderOptions>(configuration.GetSection("CityLoader"));

// Register services
services.AddSingleton<ICityPostProcessor, CityPostProcessor>();
services.AddSingleton<IDatabaseWriterService, DatabaseWriterService>();
services.AddSingleton<IGeofabrikRegionPathResolver, GeofabrikRegionPathResolver>();
services.AddSingleton<INaturalEarthShapefileLoader, NaturalEarthShapefileLoader>();
services.AddSingleton<IOsmCityShapefileLoader, OsmCityShapefileLoader>();
services.AddSingleton<IDataLoader, DataLoader>();

var serviceProvider = services.BuildServiceProvider();

var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
var logger = loggerFactory.CreateLogger("GeoResolver.DataUpdater");
var distributedLockProvider = serviceProvider.GetRequiredService<IDistributedLockProvider>();
var databaseWriterService = serviceProvider.GetRequiredService<IDatabaseWriterService>();
var dataLoader = serviceProvider.GetRequiredService<IDataLoader>();

try
{
	logger.LogInformation("Starting data loading process...");

	var distributedLock = distributedLockProvider.CreateLock("geo_resolver_data_update");
	await using var handle =
		await distributedLock.TryAcquireAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
	if (handle == null)
	{
		logger.LogWarning("Another data loading process is already running (lock acquired). Exiting.");
		return 1;
	}

	logger.LogInformation("Acquired lock, starting data update...");

	await dataLoader.LoadAllDataAsync();
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