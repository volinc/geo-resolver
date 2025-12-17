# Architecture

This document gives a high‑level overview of the GeoResolver architecture and
how its components interact.

## High‑level diagram

```text
┌─────────────────┐
│   HTTP Client   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐          ┌──────────────────────────┐
│  Minimal API    │          │ GeoResolver.DataUpdater  │
│  (GeoResolver)  │          │     (Console app)       │
└────────┬────────┘          └──────────┬──────────────┘
         │                              │
         ▼                              ▼
┌────────────────────┐        ┌────────────────────────┐
│  GeoLocationService│        │   Data loading logic   │
│  (read‑only)       │        │ (download, convert,    │
└────────┬───────────┘        │  import, clean‑up)     │
         │                    └──────────┬────────────┘
         ▼                               ▼
┌─────────────────────────────────────────────────────┐
│                  PostgreSQL + PostGIS              │
│  countries, regions, cities, timezones, last_update│
└─────────────────────────────────────────────────────┘
```

## Projects

- **GeoResolver**
  - ASP.NET Core Minimal API (.NET 10).
  - Exposes HTTP endpoints:
    - `GET /api/geolocation` – resolve country/region/city/timezone for a point.
    - `GET /health` – health check (database connectivity).
  - Contains `GeoLocationService` which performs spatial queries using PostGIS
    (`ST_Contains(geometry, point)`) against `countries`, `regions`, `cities` and `timezones` tables.

- **GeoResolver.DataUpdater**
  - Console application responsible for loading and refreshing geospatial data.
  - Uses GDAL/OGR (`ogr2ogr`) to convert Shapefiles to GeoJSON and filter features.
  - Writes into the same PostgreSQL/PostGIS database that the API reads from.
  - Uses distributed locks (PostgreSQL advisory locks) to prevent concurrent imports.

- **GeoResolver.Models**
  - Shared POCO models for countries, regions, cities and timezones.
  - Used by both the API and the data updater.

## Data flow

1. **Data import** (GeoResolver.DataUpdater):
   - Downloads source archives from external providers.
   - Converts and filters data with GDAL/OGR.
   - Imports geometries and attributes into PostGIS tables.
   - Updates `last_update` to track when data was last refreshed.

2. **Request processing** (GeoResolver API):
   - Validates latitude/longitude range.
   - Builds a `POINT` geometry and queries:
     - `countries` (required for any country/region/city data),
     - `regions` (admin level 1),
     - `cities` (significant places),
     - `timezones` (timezone polygon, if available).
   - If no timezone polygon is found, falls back to a simple approximation
     based on longitude.
   - Returns a `GeoLocationResponse` with country/region/city fields (optional)
     and timezone offsets (always present).

## Deployment model

- Recommended deployment uses **Docker Compose**:
  - `geo-resolver-postgres` – PostgreSQL + PostGIS with initialization scripts.
  - `geo-resolver-api` – GeoResolver API container (Native AOT, chiseled runtime image).
- The API runs as a non‑root user inside the container and exposes HTTP on port `8080`.
- The same PostgreSQL instance can be shared between multiple GeoResolver API
  instances and a scheduled DataUpdater job.
