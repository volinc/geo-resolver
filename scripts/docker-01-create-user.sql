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
