-- Idempotent script to enable PostGIS extension in the database
-- This script is for MANUAL execution
-- Requires: database 'georesolver' must exist
-- Execute as postgres superuser: psql -U postgres -d georesolver -f scripts/manual-02-enable-postgis.sql

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Grant usage on schema public to georesolver user
GRANT ALL ON SCHEMA public TO georesolver;

