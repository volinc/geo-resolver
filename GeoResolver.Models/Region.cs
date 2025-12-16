namespace GeoResolver.Models;

public sealed class Region
{
	public int Id { get; set; }
	public required string Identifier { get; set; }
	public required string NameLatin { get; set; }
	public string? CountryIsoAlpha2Code { get; set; }
	public string? CountryIsoAlpha3Code { get; set; }
	public string? WikidataId { get; set; }
	public string? NameLocal { get; set; }
	public required NetTopologySuite.Geometries.Geometry Geometry { get; set; }
}