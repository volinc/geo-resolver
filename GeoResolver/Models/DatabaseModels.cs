namespace GeoResolver.Models;

public sealed class CountryEntity
{
    public int Id { get; set; }
    public string? IsoAlpha2Code { get; set; }
    public string? IsoAlpha3Code { get; set; }
    public required string NameLatin { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

public sealed class RegionEntity
{
    public int Id { get; set; }
    public required string Identifier { get; set; }
    public required string NameLatin { get; set; }
    public string? CountryIsoAlpha2Code { get; set; }
    public string? CountryIsoAlpha3Code { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

public sealed class CityEntity
{
    public int Id { get; set; }
    public required string Identifier { get; set; }
    public required string NameLatin { get; set; }
    public string? CountryIsoAlpha2Code { get; set; }
    public string? CountryIsoAlpha3Code { get; set; }
    public string? RegionIdentifier { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

public sealed class TimezoneEntity
{
    public int Id { get; set; }
    public required string TimezoneId { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

