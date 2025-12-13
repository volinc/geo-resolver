using GeoResolver.Models;

namespace GeoResolver.Services;

public interface IGeoLocationService
{
    Task<GeoLocationResponse?> GetLocationAsync(double latitude, double longitude, CancellationToken cancellationToken = default);
}

