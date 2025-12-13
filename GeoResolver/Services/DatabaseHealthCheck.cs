using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;

namespace GeoResolver.Services;

public sealed class DatabaseHealthCheck : IHealthCheck
{
    private readonly NpgsqlDataSource _npgsqlDataSource;

    public DatabaseHealthCheck(NpgsqlDataSource npgsqlDataSource)
    {
        _npgsqlDataSource = npgsqlDataSource;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = _npgsqlDataSource.CreateConnection();
            await connection.OpenAsync(cancellationToken);
            
            await using var cmd = new NpgsqlCommand("SELECT 1;", connection);
            await cmd.ExecuteScalarAsync(cancellationToken);
            
            return HealthCheckResult.Healthy("Database connection is working");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Database connection failed", ex);
        }
    }
}
