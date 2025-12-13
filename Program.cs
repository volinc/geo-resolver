using System.Text.Json.Serialization;
using GeoResolver.Models;
using GeoResolver.Options;
using GeoResolver.Services;
using GeoResolver.Services.DataLoaders;
using Medallion.Threading;
using Medallion.Threading.Postgres;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;

// Register NetTopologySuite for Npgsql (for PostGIS geometry support)
// In Npgsql 8+, NetTopologySuite is registered automatically when package is referenced
// But we can explicitly ensure it's registered using DataSourceBuilder pattern if needed

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, GeoResolverJsonSerializerContext.Default);
});

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
                       ?? throw new InvalidOperationException("'DefaultConnection' is missing.");

builder.Services.AddNpgsqlDataSource(connectionString);
builder.Services.AddSingleton<IDistributedLockProvider>(_ => 
    new PostgresDistributedSynchronizationProvider(connectionString));

builder.Services.AddHttpClient();
builder.Services.AddSingleton<IDatabaseService, DatabaseService>();
builder.Services.AddSingleton<IGeoLocationService, GeoLocationService>();
builder.Services.AddSingleton<IDataLoader, DataLoader>();

// Configure DataUpdate options using Configuration.Binder
builder.Services.Configure<DataUpdateOptions>(
    builder.Configuration.GetSection(DataUpdateOptions.SectionName));

builder.Services.AddHostedService<DataUpdateBackgroundService>();

builder.Services.AddHealthChecks()
    .AddCheck<DatabaseHealthCheck>("database", tags: ["database"]);

var app = builder.Build();

app.UseHealthChecks("/health", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("database")
});

app.MapGet("/api/geolocation", async (
    double latitude,
    double longitude,
    IGeoLocationService geoLocationService,
    CancellationToken cancellationToken) =>
{
    if (latitude < -90 || latitude > 90)
    {
        return Results.BadRequest(new ErrorResponse("Latitude must be between -90 and 90"));
    }

    if (longitude < -180 || longitude > 180)
    {
        return Results.BadRequest(new ErrorResponse("Longitude must be between -180 and 180"));
    }

    var result = await geoLocationService.ResolveAsync(latitude, longitude, cancellationToken);
    
    if (result == null)
    {
        return Results.NotFound(new ErrorResponse("Location not found for the provided coordinates"));
    }

    return Results.Ok(result);
})
.WithName("GetGeoLocation");

app.Run();

[JsonSerializable(typeof(GeoLocationResponse))]
[JsonSerializable(typeof(ErrorResponse))]
internal partial class GeoResolverJsonSerializerContext : JsonSerializerContext
{
}

internal record ErrorResponse(string Error);

