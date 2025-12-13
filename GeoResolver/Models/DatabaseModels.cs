namespace GeoResolver.Models;

/// <summary>
/// Database model for countries
/// </summary>
public class CountryEntity
{
    public int Id { get; set; }
    public required string IsoAlpha2Code { get; set; }
    public required string NameLatin { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

/// <summary>
/// Database model for regions (admin level 1)
/// </summary>
public class RegionEntity
{
    public int Id { get; set; }
    public required string Identifier { get; set; }
    public required string NameLatin { get; set; }
    public required string CountryIsoAlpha2Code { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

/// <summary>
/// Database model for cities
/// </summary>
public class CityEntity
{
    public int Id { get; set; }
    public required string Identifier { get; set; }
    public required string NameLatin { get; set; }
    public required string CountryIsoAlpha2Code { get; set; }
    public string? RegionIdentifier { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

/// <summary>
/// Database model for timezones
/// </summary>
public class TimezoneEntity
{
    public int Id { get; set; }
    public required string TimezoneId { get; set; }
    public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}

