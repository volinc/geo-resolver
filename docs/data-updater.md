# GeoResolver.DataUpdater

`GeoResolver.DataUpdater` is a console application that downloads, converts and imports
geospatial data into the GeoResolver PostgreSQL/PostGIS database.

It is intended to be run **manually or by a scheduled job** when you want to refresh
countries, regions, cities and timezone polygons.

## Responsibilities

When you run the data updater it:

1. Downloads original Shapefile or GeoJSON archives from open data sources
   (Natural Earth, OSM/Geofabrik, Timezone Boundary Builder).
2. Uses **GDAL/OGR** (`ogr2ogr`) to convert Shapefiles to GeoJSON and apply filters.
3. Filters cities by feature type and population (to keep only significant places).
4. Imports countries, regions, cities and timezones into PostgreSQL with PostGIS.
5. Clears existing data before loading new data (idempotent refresh).
6. Uses distributed locks so that only one updater instance runs at a time.
7. Updates the `last_update` table with the timestamp of the last successful load.
8. Logs timing information for each step (download, extraction, conversion, import).

## Prerequisites

### GDAL / OGR

The updater relies on **GDAL** (Geospatial Data Abstraction Library), specifically on
the `ogr2ogr` tool for Shapefile → GeoJSON conversion and attribute filtering.

Install GDAL on the machine where you run `GeoResolver.DataUpdater`:

- **macOS (Homebrew)**

  ```bash
  brew install gdal
  ```

- **Ubuntu / Debian**

  ```bash
  sudo apt-get update
  sudo apt-get install gdal-bin
  ```

- **Windows**

  - Install OSGeo4W and select the `gdal` package, or
  - Download GDAL binaries from the official website: `https://gdal.org/download.html`.

After installation, verify that `ogr2ogr` is on the PATH:

```bash
ogr2ogr --version
```

If you run the updater inside a Docker image, add GDAL to the image, for example:

```dockerfile
RUN apt-get update \
    && apt-get install -y gdal-bin \
    && rm -rf /var/lib/apt/lists/*
```

## Configuration

Configuration for the updater lives in `GeoResolver.DataUpdater/appsettings.json`.

### Connection string

```jsonc
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Port=5432;Database=geo_resolver;Username=postgres;Password=pass"
  }
}
```

You can override it via environment variable:

```bash
export ConnectionStrings__DefaultConnection="Host=localhost;Port=5432;Database=geo_resolver;Username=postgres;Password=your_password"
```

### CityLoader options

```jsonc
{
  "CityLoader": {
    "Countries": ["RU", "DE", "FR"]
  }
}
```

- `Countries` – array of ISO 3166‑1 alpha‑2 country codes for which cities should be loaded.
  - If the list is **empty or missing**, cities are loaded for **all countries** in the
    `countries` table.

Examples:

- `"Countries": ["RU"]` – load cities only for Russia.
- `"Countries": ["DE", "FR", "IT"]` – load cities for Germany, France and Italy.
- `"Countries": []` – load cities for all countries.

For some large countries (e.g. Russia) Geofabrik does not provide a single shapefile;
`GeoResolver.DataUpdater` automatically downloads multiple regional files in parallel
and merges them during import.

## Running the updater

### From the project directory

```bash
cd GeoResolver.DataUpdater
 dotnet run
```

### From the solution root

```bash
 dotnet run --project GeoResolver.DataUpdater
```

The application will log each phase of the process, for example:

```text
[INFO] === Starting data loading process ===
[INFO] Clearing existing data...
[INFO] Clearing data completed in 234ms
[INFO] Loading countries...
[INFO] Downloading countries data from https://...
[INFO] Download completed in 1234ms (1.23s)
[INFO] Countries loading completed in 5678ms (5.68s)
...
[INFO] === Data loading process completed in 234567ms (3.91 minutes) ===
```

## Concurrency and distributed locks

To avoid concurrent imports from multiple instances, the updater uses PostgreSQL
advisory locks via the `DistributedLock.Postgres` library:

1. On startup it tries to acquire a global advisory lock.
2. If the lock is already held by another process, the current instance exits
   with a warning.
3. After successful completion it releases the lock and updates the `last_update` table.

This allows you to safely schedule the updater (e.g. via cron or a CI/CD job)
without worrying about overlapping runs.
