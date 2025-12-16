namespace GeoResolver.DataUpdater;

/// <summary>
///     Options for configuring which countries' regions
///     should be loaded from Natural Earth Admin 1 dataset.
/// </summary>
public sealed class RegionLoaderOptions
{
	// List of countries (2-letter ISO codes) for which regions
	// should be loaded from Natural Earth Admin 1. Example:
	// "Countries": [ "DE", "FR", "RU" ]
	// If empty or not configured, all regions will be loaded.
	public string[] Countries { get; init; } = [];
}
