using System.Text.Json.Serialization;
using GeoResolver.Models;
using GeoResolver.Services;
using GeoResolver.Services.DataLoaders;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, GeoResolverJsonSerializerContext.Default);
});

builder.Services.AddHttpClient();
builder.Services.AddSingleton<IDatabaseService, DatabaseService>();
builder.Services.AddSingleton<IGeoLocationService, GeoLocationService>();
builder.Services.AddSingleton<IDataLoader, DataLoader>();
builder.Services.AddHostedService<DataUpdateBackgroundService>();

builder.Services.AddHealthChecks()
    .AddCheck<DatabaseHealthCheck>("database", tags: new[] { "database" });

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

