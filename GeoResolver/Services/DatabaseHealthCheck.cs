using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace GeoResolver.Services;

public class DatabaseHealthCheck : IHealthCheck
{
    private readonly IDatabaseService _databaseService;

    public DatabaseHealthCheck(IDatabaseService databaseService)
    {
        _databaseService = databaseService;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Try to query a simple point to check database connectivity
            await _databaseService.FindCountryByPointAsync(0, 0, cancellationToken);
            return HealthCheckResult.Healthy("Database is accessible");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Database is not accessible", ex);
        }
    }
}

