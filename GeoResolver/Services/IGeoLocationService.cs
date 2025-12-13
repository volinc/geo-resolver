using GeoResolver.Models;

namespace GeoResolver.Services;

public interface IGeoLocationService
{
	Task<GeoLocationResponse?> ResolveAsync(double latitude, double longitude,
		CancellationToken cancellationToken = default);
}

