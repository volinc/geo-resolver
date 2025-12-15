namespace GeoResolver.DataUpdater.Services;

/// <summary>
///     Post-processes imported cities and regions: fills missing region_identifier using spatial queries
///     and transliterates non-Latin names to Latin.
/// </summary>
public interface ICityPostProcessor
{
	/// <summary>
	///     Post-processes imported cities: fills missing region_identifier using spatial queries
	///     and transliterates non-Latin names to Latin.
	/// </summary>
	Task PostProcessCitiesAsync(CancellationToken cancellationToken = default);

	/// <summary>
	///     Post-processes imported regions: transliterates non-Latin names to Latin.
	/// </summary>
	Task PostProcessRegionsAsync(CancellationToken cancellationToken = default);
}
