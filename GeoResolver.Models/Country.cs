namespace GeoResolver.Models;

public sealed class Country
{
	public int Id { get; set; }
	public string? IsoAlpha2Code { get; set; }
	public string? IsoAlpha3Code { get; set; }
	public required string NameLatin { get; set; }
	public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}