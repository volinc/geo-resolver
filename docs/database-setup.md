# Database setup

GeoResolver stores all geospatial data in **PostgreSQL** with the **PostGIS** extension.
This document describes how to create and initialize the database both with Docker Compose and manually.

## Requirements

- PostgreSQL 12+ (or compatible image in Docker)
- PostGIS extension

## Using Docker Compose (recommended)

When you run the stack via `docker compose up -d` from the repository root, the PostgreSQL
container is initialized automatically using scripts from the `scripts/` directory.

The following scripts are executed by `docker-entrypoint-initdb.d` **on first start**:

1. `scripts/docker-01-create-user.sql`
   - Creates a read‑only PostgreSQL user `reader` with password `pass`
   - Grants `USAGE` on the `public` schema
   - Grants `SELECT` on all existing and future tables/sequences in the `public` schema
2. `scripts/docker-02-enable-postgis.sql`
   - Enables the PostGIS extension in the database specified by `POSTGRES_DB`
3. `scripts/docker-03-create-tables.sql`
   - Creates all required tables, indexes and the `last_update` tracking table

All scripts are **idempotent** and safe to run multiple times.

The database name, superuser and password are controlled by environment variables
in `compose.yaml` (`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`).

## Manual setup (without Docker)

If you run PostgreSQL outside of Docker, you can initialize the database using
shell and SQL scripts from the `scripts/` directory.

### Option 1: one‑shot helper script

From the repository root:

```bash
chmod +x scripts/setup-database.sh
./scripts/setup-database.sh
```

This script will:

- create the `geo_resolver` database (if it does not exist),
- create the `reader` user with read‑only permissions,
- enable PostGIS,
- create all tables and indexes.

Make sure you run it as a PostgreSQL superuser (commonly `postgres`).

### Option 2: step‑by‑step scripts

Run the SQL scripts manually using `psql`.

1. **Create users and database**

   ```bash
   psql -U postgres -f scripts/manual-01-create-user-and-database.sql
   ```

   This script:

   - creates a read‑only user `reader` (if missing),
   - creates the `geo_resolver` database (if missing),
   - grants read‑only access for `reader` to the `public` schema and future tables.

2. **Enable PostGIS**

   ```bash
   psql -U postgres -d geo_resolver -f scripts/manual-02-enable-postgis.sql
   ```

   This script enables the PostGIS extension in the `geo_resolver` database.

3. **Create tables and indexes**

   ```bash
   psql -U postgres -d geo_resolver -f scripts/manual-03-create-tables.sql
   ```

   It creates:

   - `countries`, `regions`, `cities`, `timezones` tables,
   - GIST indexes on geometry columns,
   - helper `last_update` table for tracking data refresh time.

4. **(Optional) Drop tables**

   To completely clear the schema while keeping the database itself:

   ```bash
   psql -U postgres -d geo_resolver -f scripts/manual-04-drop-tables.sql
   ```

## Connection string

The API expects a standard ADO.NET connection string under
`ConnectionStrings:DefaultConnection` (see `GeoResolver/appsettings.json`).

Example for local development:

```jsonc
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Port=5432;Database=geo_resolver;Username=reader;Password=pass"
  }
}
```

Environment variable equivalent:

```bash
export ConnectionStrings__DefaultConnection="Host=localhost;Port=5432;Database=geo_resolver;Username=reader;Password=pass"
```

You can use a different user/password; just keep the same privileges as the `reader`
user created by the scripts above (read‑only access to all tables).
