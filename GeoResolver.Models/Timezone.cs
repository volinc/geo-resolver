namespace GeoResolver.Models;

public sealed class Timezone
{
    public int Id { get; set; }
    public required string TimezoneId { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

