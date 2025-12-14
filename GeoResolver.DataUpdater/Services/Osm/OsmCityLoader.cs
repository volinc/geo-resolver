using System.Text;
using System.Text.Json;
using GeoResolver.DataUpdater.Services;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater.Services.Osm;

/// <summary>
/// Loads city boundaries from OpenStreetMap using Overpass API
/// </summary>
public class OsmCityLoader
{
    private readonly ILogger<OsmCityLoader> _logger;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IDatabaseWriterService _databaseWriterService;

    // Public Overpass API endpoints
    private readonly string[] _overpassEndpoints = new[]
    {
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.openstreetmap.ru/api/interpreter"
    };

    public OsmCityLoader(
        ILogger<OsmCityLoader> logger,
        IHttpClientFactory httpClientFactory,
        IDatabaseWriterService databaseWriterService)
    {
        _logger = logger;
        _httpClientFactory = httpClientFactory;
        _databaseWriterService = databaseWriterService;
    }

    /// <summary>
    /// Loads cities from OpenStreetMap using Overpass API
    /// Queries for cities with administrative boundaries (admin_level 4-10)
    /// </summary>
    public async Task LoadCitiesAsync(CancellationToken cancellationToken = default)
    {
        var overallStopwatch = System.Diagnostics.Stopwatch.StartNew();
        _logger.LogInformation("Starting to load cities from OpenStreetMap...");

        // Overpass API query to get cities with administrative boundaries
        // This query gets:
        // - Cities (place=city) with admin_level 4-10
        // - Towns (place=town) with admin_level 4-10
        // - Only relations and ways with boundary=administrative
        // For relations, we need to get members (ways) and their geometry
        // Note: This is a VERY large query - may take 20+ minutes for global data
        // The query requests all ways referenced by relations, which can be millions of nodes
        // Consider using country-specific queries or OSM extracts for production use
        var overpassQuery = @"
[out:json][timeout:1800];
(
  relation[""place""=""city""][""boundary""=""administrative""][""admin_level""~""^[4-9]$|^10$""];
  relation[""place""=""town""][""boundary""=""administrative""][""admin_level""~""^[4-9]$|^10$""];
  way[""place""=""city""][""boundary""=""administrative""][""admin_level""~""^[4-9]$|^10$""];
  way[""place""=""town""][""boundary""=""administrative""][""admin_level""~""^[4-9]$|^10$""];
);
(._;>;);
out geom;";

        Exception? lastException = null;

        foreach (var endpoint in _overpassEndpoints)
        {
            try
            {
                _logger.LogInformation("Attempting to query Overpass API at {Endpoint}...", endpoint);

                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(30); // Overpass queries for global data with multipolygon can take 20+ minutes

                var requestContent = new StringContent(overpassQuery, Encoding.UTF8, "application/x-www-form-urlencoded");
                
                _logger.LogInformation("Sending Overpass API query (this may take 15-20 minutes for global data)...");
                var downloadStopwatch = System.Diagnostics.Stopwatch.StartNew();
                
                var response = await httpClient.PostAsync(endpoint, requestContent, cancellationToken);
                
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning("Failed to query Overpass API at {Endpoint}: {StatusCode}", endpoint, response.StatusCode);
                    continue;
                }

                _logger.LogInformation("Response received, reading content (this may take several minutes for large datasets)...");
                
                // Read content with progress indication for large responses
                var geoJsonContent = await response.Content.ReadAsStringAsync(cancellationToken);
                
                if (string.IsNullOrWhiteSpace(geoJsonContent))
                {
                    _logger.LogWarning("Empty response from Overpass API at {Endpoint}", endpoint);
                    continue;
                }
                
                _logger.LogInformation("Content read: {Size} characters", geoJsonContent.Length);
                downloadStopwatch.Stop();
                _logger.LogInformation("Download completed in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)", 
                    downloadStopwatch.ElapsedMilliseconds, downloadStopwatch.Elapsed.TotalSeconds);

                // Convert Overpass JSON to GeoJSON format
                var geoJson = ConvertOverpassToGeoJson(geoJsonContent);
                
                // Import to database
                var importStopwatch = System.Diagnostics.Stopwatch.StartNew();
                await _databaseWriterService.ImportCitiesFromGeoJsonAsync(geoJson, cancellationToken);
                importStopwatch.Stop();
                _logger.LogInformation("Cities imported to database in {ElapsedMilliseconds}ms ({ElapsedSeconds:F2}s)", 
                    importStopwatch.ElapsedMilliseconds, importStopwatch.Elapsed.TotalSeconds);

                overallStopwatch.Stop();
                _logger.LogInformation("Successfully loaded cities from OpenStreetMap via {Endpoint} in {ElapsedMilliseconds}ms ({ElapsedMinutes:F2} minutes)", 
                    endpoint, overallStopwatch.ElapsedMilliseconds, overallStopwatch.Elapsed.TotalMinutes);
                return;
            }
            catch (HttpRequestException ex)
            {
                _logger.LogWarning(ex, "Failed to query Overpass API at {Endpoint}", endpoint);
                lastException = ex;
                continue;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error processing cities from Overpass API at {Endpoint}", endpoint);
                lastException = ex;
                continue;
            }
        }

        _logger.LogError(lastException, "Failed to load cities from OpenStreetMap via all Overpass API endpoints");
        throw new InvalidOperationException("Failed to load cities from OpenStreetMap. Please check Overpass API availability.", lastException);
    }

    /// <summary>
    /// Converts Overpass API JSON response to GeoJSON format
    /// </summary>
    private string ConvertOverpassToGeoJson(string overpassJson)
    {
        try
        {
            var jsonDoc = JsonDocument.Parse(overpassJson);
            var root = jsonDoc.RootElement;

            if (!root.TryGetProperty("elements", out var elements))
            {
                _logger.LogWarning("Overpass API response does not contain 'elements' property");
                return CreateEmptyGeoJson();
            }

            // Build index of all elements by ID and type
            var elementsById = new Dictionary<(string type, long id), JsonElement>();
            var relations = new List<JsonElement>();
            var ways = new List<JsonElement>();

            foreach (var element in elements.EnumerateArray())
            {
                if (!element.TryGetProperty("type", out var type) || !element.TryGetProperty("id", out var id))
                    continue;

                var typeStr = type.GetString() ?? "";
                var idValue = id.GetInt64();

                elementsById[(typeStr, idValue)] = element;

                if (typeStr == "relation")
                {
                    relations.Add(element);
                }
                else if (typeStr == "way")
                {
                    ways.Add(element);
                }
            }

            _logger.LogInformation("Found {RelationCount} relations and {WayCount} ways in Overpass response", 
                relations.Count, ways.Count);

            var features = new List<JsonElement>();

            // Process relations first (they may reference ways)
            foreach (var relation in relations)
            {
                var feature = ConvertRelationToFeature(relation, elementsById);
                if (feature.HasValue)
                {
                    features.Add(feature.Value);
                }
            }

            // Process standalone ways (not part of relations)
            foreach (var way in ways)
            {
                // Check if this way is already processed as part of a relation
                var wayId = way.GetProperty("id").GetInt64();
                var isPartOfRelation = relations.Any(r =>
                {
                    if (!r.TryGetProperty("members", out var members))
                        return false;
                    return members.EnumerateArray().Any(m =>
                        m.TryGetProperty("type", out var mType) && mType.GetString() == "way" &&
                        m.TryGetProperty("ref", out var mRef) && mRef.GetInt64() == wayId);
                });

                if (!isPartOfRelation)
                {
                    var feature = ConvertWayToFeature(way);
                    if (feature.HasValue)
                    {
                        features.Add(feature.Value);
                    }
                }
            }

            _logger.LogInformation("Converted {FeatureCount} features to GeoJSON", features.Count);
            return CreateGeoJsonFromFeatures(features);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error converting Overpass JSON to GeoJSON");
            return CreateEmptyGeoJson();
        }
    }

    private JsonElement? ConvertRelationToFeature(JsonElement relation, Dictionary<(string type, long id), JsonElement> elementsById)
    {
        try
        {
            if (!relation.TryGetProperty("members", out var members))
            {
                _logger.LogDebug("Relation {RelationId} has no members", relation.GetProperty("id").GetInt64());
                return null;
            }

            // Extract tags/properties
            var properties = new Dictionary<string, JsonElement>();
            if (relation.TryGetProperty("tags", out var tags))
            {
                foreach (var tag in tags.EnumerateObject())
                {
                    properties[tag.Name] = tag.Value;
                }
            }

            // Check if this is a multipolygon
            var isMultipolygon = tags.TryGetProperty("type", out var typeTag) && 
                                typeTag.GetString() == "multipolygon";

            // Collect outer and inner ways
            var outerWays = new List<List<double[]>>();
            var innerWays = new List<List<double[]>>();

            foreach (var member in members.EnumerateArray())
            {
                if (!member.TryGetProperty("type", out var memberType) || 
                    !member.TryGetProperty("ref", out var memberRef) ||
                    !member.TryGetProperty("role", out var role))
                    continue;

                var memberTypeStr = memberType.GetString();
                var memberRefId = memberRef.GetInt64();
                var roleStr = role.GetString() ?? "";

                if (memberTypeStr != "way")
                    continue;

                // Get the way element
                if (!elementsById.TryGetValue(("way", memberRefId), out var wayElement))
                {
                    _logger.LogDebug("Way {WayId} referenced by relation {RelationId} not found in elements", 
                        memberRefId, relation.GetProperty("id").GetInt64());
                    continue;
                }

                // Extract coordinates from way
                var coordinates = ExtractWayCoordinates(wayElement);
                if (coordinates == null || coordinates.Count < 3)
                    continue;

                // Close the polygon if not already closed
                if (coordinates[0][0] != coordinates[^1][0] || coordinates[0][1] != coordinates[^1][1])
                {
                    coordinates.Add(coordinates[0]);
                }

                // Add to outer or inner based on role
                if (roleStr == "outer")
                {
                    outerWays.Add(coordinates);
                }
                else if (roleStr == "inner")
                {
                    innerWays.Add(coordinates);
                }
                else if (isMultipolygon && string.IsNullOrEmpty(roleStr))
                {
                    // In multipolygons, members without role are typically outer
                    outerWays.Add(coordinates);
                }
            }

            if (outerWays.Count == 0)
            {
                _logger.LogDebug("Relation {RelationId} has no valid outer ways", relation.GetProperty("id").GetInt64());
                return null;
            }

            // Build MultiPolygon coordinates
            // Format: [[[outer1], [inner1], [inner2]], [[outer2], [inner3]]]
            var multiPolygonCoordinates = new List<List<List<double[]>>>();

            foreach (var outerWay in outerWays)
            {
                var polygon = new List<List<double[]>> { outerWay };
                
                // Add inner ways that are inside this outer way
                // For simplicity, we'll add all inner ways to the first outer way
                // A more sophisticated implementation would check spatial containment
                if (multiPolygonCoordinates.Count == 0)
                {
                    polygon.AddRange(innerWays);
                }

                multiPolygonCoordinates.Add(polygon);
            }

            // If only one polygon, use Polygon instead of MultiPolygon
            object geometry;
            if (multiPolygonCoordinates.Count == 1 && multiPolygonCoordinates[0].Count == 1)
            {
                // Single polygon with no holes
                geometry = new
                {
                    type = "Polygon",
                    coordinates = multiPolygonCoordinates[0]
                };
            }
            else
            {
                // MultiPolygon
                geometry = new
                {
                    type = "MultiPolygon",
                    coordinates = multiPolygonCoordinates
                };
            }

            var feature = new
            {
                type = "Feature",
                properties = properties,
                geometry = geometry
            };

            return JsonSerializer.SerializeToElement(feature);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error converting relation to GeoJSON feature");
            return null;
        }
    }

    private List<double[]>? ExtractWayCoordinates(JsonElement way)
    {
        try
        {
            if (!way.TryGetProperty("geometry", out var geometry) || geometry.ValueKind != JsonValueKind.Array)
            {
                return null;
            }

            var coordinates = new List<double[]>();
            foreach (var point in geometry.EnumerateArray())
            {
                if (point.TryGetProperty("lat", out var lat) && point.TryGetProperty("lon", out var lon))
                {
                    coordinates.Add(new[] { lon.GetDouble(), lat.GetDouble() });
                }
            }

            return coordinates.Count >= 3 ? coordinates : null;
        }
        catch
        {
            return null;
        }
    }

    private JsonElement? ConvertWayToFeature(JsonElement way)
    {
        try
        {
            if (!way.TryGetProperty("geometry", out var geometry) || !geometry.ValueKind.Equals(JsonValueKind.Array))
            {
                return null;
            }

            // Extract coordinates from geometry
            var coordinates = new List<double[]>();
            foreach (var point in geometry.EnumerateArray())
            {
                if (point.TryGetProperty("lat", out var lat) && point.TryGetProperty("lon", out var lon))
                {
                    coordinates.Add(new[] { lon.GetDouble(), lat.GetDouble() });
                }
            }

            if (coordinates.Count < 4) // Need at least 4 points for a polygon (first = last)
            {
                return null;
            }

            // Close the polygon if not already closed
            if (coordinates[0][0] != coordinates[^1][0] || coordinates[0][1] != coordinates[^1][1])
            {
                coordinates.Add(coordinates[0]);
            }

            // Extract properties
            var properties = new Dictionary<string, JsonElement>();
            if (way.TryGetProperty("tags", out var tags))
            {
                foreach (var tag in tags.EnumerateObject())
                {
                    properties[tag.Name] = tag.Value;
                }
            }

            // Create GeoJSON feature
            var feature = new
            {
                type = "Feature",
                properties = properties,
                geometry = new
                {
                    type = "Polygon",
                    coordinates = new[] { coordinates }
                }
            };

            return JsonSerializer.SerializeToElement(feature);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error converting way to GeoJSON feature");
            return null;
        }
    }

    private string CreateGeoJsonFromFeatures(List<JsonElement> features)
    {
        var geoJson = new
        {
            type = "FeatureCollection",
            features = features
        };

        return JsonSerializer.Serialize(geoJson);
    }

    private string CreateEmptyGeoJson()
    {
        return JsonSerializer.Serialize(new
        {
            type = "FeatureCollection",
            features = Array.Empty<object>()
        });
    }
}

