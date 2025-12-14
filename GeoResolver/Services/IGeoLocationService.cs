using GeoResolver.Models;

namespace GeoResolver.Services;

public interface IGeoLocationService
{
	/// <summary>
	/// Resolves geo-location information for given coordinates
	/// Always returns a response with timezone data (required), country/region/city data is optional
	/// </summary>
	Task<GeoLocationResponse> ResolveAsync(double latitude, double longitude,
		CancellationToken cancellationToken = default);
}

