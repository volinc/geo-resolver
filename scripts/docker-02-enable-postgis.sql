-- Idempotent script to enable PostGIS extension in the database
-- This script is executed automatically by docker-entrypoint-initdb.d
-- Runs after database is created, connected to the target database (geo_resolver)

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Grant usage on schema public to geo_resolver user
GRANT ALL ON SCHEMA public TO geo_resolver;

