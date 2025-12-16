namespace GeoResolver.Models;

public sealed class Country
{
	public int Id { get; set; }
	public required string IsoAlpha2Code { get; set; }
	public required string IsoAlpha3Code { get; set; }
	public required string NameLatin { get; set; }
	public required string WikidataId { get; set; }
	public string? NameLocal { get; set; }
	public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}