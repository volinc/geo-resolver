-- Idempotent script to create PostgreSQL users
-- This script is executed automatically by docker-entrypoint-initdb.d
-- Runs as postgres superuser AFTER database is created (via POSTGRES_DB environment variable in docker-compose.yml)

-- Create reader user (read-only, used by GeoResolver API)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'reader') THEN
        CREATE USER reader WITH PASSWORD 'pass';
    END IF;
END
$$;

-- Grant usage on schema public to reader user
GRANT USAGE ON SCHEMA public TO reader;

-- Grant SELECT on all existing and future tables in public schema
-- This allows reader to read from all tables that will be created later
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO reader;
