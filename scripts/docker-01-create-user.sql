-- Idempotent script to create PostgreSQL user and set as database owner
-- This script is executed automatically by docker-entrypoint-initdb.d
-- Runs as postgres superuser AFTER database is created (via POSTGRES_DB environment variable in docker-compose.yml)

-- Create user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'geo_resolver') THEN
        CREATE USER geo_resolver WITH PASSWORD 'pass';
    END IF;
END
$$;

-- Set geo_resolver as owner of the database
ALTER DATABASE geo_resolver OWNER TO geo_resolver;
