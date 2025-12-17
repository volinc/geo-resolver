## GeoResolver

GeoResolver is a minimal .NET 10 API that resolves **country, region, city and timezone offset** for a given latitude/longitude using PostgreSQL + PostGIS and open geospatial datasets.

### Features

- Resolve **country** by coordinates (ISO 3166‑1 alpha‑2/alpha‑3, Latin name)
- Resolve **admin level 1 region** (identifier + Latin name)
- Resolve **city** (identifier + Latin name; region is optional)
- Resolve **timezone** offsets (raw offset, DST offset, total offset in seconds)
- Simple **health check** endpoint for monitoring

> Detailed information about database setup, data loading, architecture and operations is available in separate documents (see “Further documentation”).

### Requirements

- .NET 10 SDK
- PostgreSQL 12+ with PostGIS
- Docker (recommended way to run the full stack)

### Quickstart

#### Run with Docker Compose (recommended)

From the repository root:

```bash
docker compose up -d
```

This will:

- start PostgreSQL with PostGIS and initialize the schema,
- build and run the GeoResolver API,
- wire everything together on a single Docker network.

By default the API will be available at:

- `http://localhost:8080/api/geolocation`
- `http://localhost:8080/health`

View logs:

```bash
docker compose logs -f geo-resolver
```

Stop services:

```bash
docker compose down
```

Stop and remove data volume:

```bash
docker compose down -v
```

#### Local development (without Docker)

1. Start PostgreSQL with PostGIS and create the `geo_resolver` database  
   (see [Database setup](docs/database-setup.md)).
2. Configure the connection string in `GeoResolver/appsettings.json` or via environment variable:

   ```jsonc
   {
     "ConnectionStrings": {
       "DefaultConnection": "Host=localhost;Port=5432;Database=geo_resolver;Username=geo_resolver;Password=pass"
     }
   }
   ```

   Environment variable equivalent:

   ```bash
   export ConnectionStrings__DefaultConnection="Host=localhost;Port=5432;Database=geo_resolver;Username=geo_resolver;Password=pass"
   ```

3. Run the API:

   ```bash
   dotnet run --project GeoResolver
   ```

   Check the console output for the actual listening URL (Kestrel chooses a free port by default).

### API

#### GET `/api/geolocation`

Resolve geo‑location for a point.

**Query parameters**

- `latitude` (required, double) – from `-90` to `90`
- `longitude` (required, double) – from `-180` to `180`

**Example request**

```http
GET /api/geolocation?latitude=55.7558&longitude=37.6173
```

**Example response**

```json
{
  "countryIsoAlpha2Code": "RU",
  "countryIsoAlpha3Code": "RUS",
  "countryNameLatin": "Russia",
  "regionIdentifier": null,
  "regionNameLatin": null,
  "cityIdentifier": null,
  "cityNameLatin": null,
  "timezoneRawOffsetSeconds": 10800,
  "timezoneDstOffsetSeconds": 0,
  "timezoneTotalOffsetSeconds": 10800
}
```

**Status codes**

- `200 OK` – location resolved (timezone offsets are always present)
- `400 Bad Request` – invalid coordinates (latitude or longitude out of range)

#### GET `/health`

Health check endpoint for the API and database connectivity.

**Example**

```http
GET /health
```

**Example response**

```json
{
  "status": "Healthy",
  "totalDuration": "00:00:00.0123456"
}
```

### Testing

You can use the `GeoResolver.http` file for quick manual testing in IDEs that support HTTP files
(for example JetBrains Rider or VS Code with REST Client extension).

Example requests:

```http
### Get GeoLocation - Moscow
GET http://localhost:8080/api/geolocation?latitude=55.7558&longitude=37.6173

### Get GeoLocation - New York
GET http://localhost:8080/api/geolocation?latitude=40.7128&longitude=-74.0060

### Health Check
GET http://localhost:8080/health
```

### Further documentation

More detailed information is available in separate documents under `docs/`:

- [Database setup](docs/database-setup.md) – PostgreSQL/PostGIS initialization scripts and Docker‑based database setup.
- [Data updater](docs/data-updater.md) – `GeoResolver.DataUpdater` console app, GDAL/OGR requirements and configuration.
- [Data sources](docs/data-sources.md) – Natural Earth, OSM/Geofabrik, Timezone Boundary Builder and filtering rules.
- [Architecture](docs/architecture.md) – high‑level architecture, components and data flows.
- [Operations & scaling](docs/operations.md) – health checks, caching considerations and running in production.
