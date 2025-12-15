using System.Text.Json.Serialization;
using GeoResolver.Models;
using GeoResolver.Services;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Npgsql;

var builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, GeoResolverJsonSerializerContext.Default);
});

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
                       ?? throw new InvalidOperationException("'DefaultConnection' is missing.");

var dataSourceBuilder = new NpgsqlDataSourceBuilder(connectionString);
dataSourceBuilder.UseNetTopologySuite();
var dataSource = dataSourceBuilder.Build();

builder.Services.AddSingleton(dataSource);
builder.Services.AddSingleton<IGeoLocationService, GeoLocationService>();

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
    if (latitude is < -90 or > 90)
    {
        return Results.BadRequest(new ErrorResponse("Latitude must be between -90 and 90"));
    }

    if (longitude is < -180 or > 180)
    {
        return Results.BadRequest(new ErrorResponse("Longitude must be between -180 and 180"));
    }

    var result = await geoLocationService.ResolveAsync(latitude, longitude, cancellationToken);
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

