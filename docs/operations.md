# Operations & scaling

This document covers operational aspects of running GeoResolver in production:
health checks, caching and general troubleshooting.

## Health checks

GeoResolver exposes a simple health endpoint:

- `GET /health`

The implementation (see `DatabaseHealthCheck`) verifies:

- database connectivity,
- ability to execute a basic query against PostgreSQL.

This endpoint is intended to be used by:

- container orchestrators (Kubernetes, Docker Swarm),
- load balancers and reverse proxies,
- external monitoring systems.

You can safely configure it as a **liveness** and/or **readiness** probe.

## Caching considerations

For the current use case, in‑process caching is **not strictly required**:

1. PostgreSQL + PostGIS are already well‑optimized for spatial queries
   using GIST indexes on geometry columns.
2. Queries are relatively fast thanks to these indexes and the filtered
   dataset size (only significant cities).
3. Data changes infrequently (typically yearly), so the working set in
   PostgreSQL buffers is very stable.
4. Keeping the API stateless and cache‑free simplifies deployment and
   reduces operational complexity.

However, if you need to handle very high read throughput (thousands of
requests per second), you can add caching on top:

- **In‑memory cache** (`IMemoryCache`) with a short TTL for hot coordinates
  or common regions.
- **Distributed cache** (e.g. Redis) if you run multiple API instances
  behind a load balancer.
- **HTTP‑level caching** via response headers if clients can reuse results
  for identical coordinate requests.

Any caching layer must take into account that the database can be updated
by `GeoResolver.DataUpdater`. If you schedule regular data refreshes,
consider:

- using a reasonably small TTL for cache entries, or
- explicitly invalidating cache when a data update completes.

## Scaling the API

GeoResolver is stateless with respect to HTTP requests. Scaling is primarily
about CPU and memory for PostGIS queries and network bandwidth.

Recommended practices:

- Run multiple API instances behind a load balancer (Docker Compose, Kubernetes, etc.).
- Monitor PostgreSQL CPU, I/O and buffer/cache hit ratios.
- Ensure the PostGIS GIST indexes from `scripts/docker-03-create-tables.sql`
  / `scripts/manual-03-create-tables.sql` are present and used.
- Consider placing PostgreSQL on a separate VM or managed database service
  if container resources are limited.

## Troubleshooting

When you see issues in production, check the following:

1. **Database connectivity**
   - Can the API connect using `ConnectionStrings:DefaultConnection`?
   - Does the `reader` (or equivalent) user have `SELECT` privileges on all tables?

2. **PostGIS availability**
   - Is the PostGIS extension enabled in the `geo_resolver` database?
   - Are geometry columns and GIST indexes present on `countries`, `regions`,
     `cities` and `timezones` tables?

3. **Data coverage**
   - Has `GeoResolver.DataUpdater` been run at least once to populate tables?
   - Does the `last_update` table show a recent timestamp?

4. **Performance**
   - Use PostgreSQL logs / `EXPLAIN ANALYZE` to inspect spatial queries.
   - Check for sequential scans on large tables; if present, verify indexes.

5. **Logs**
   - Inspect container logs (`docker compose logs geo-resolver`) for exceptions
     during request handling or data loading.

With correct database initialization and data loading in place, the API should
be effectively read‑only and stable under typical production workloads.
