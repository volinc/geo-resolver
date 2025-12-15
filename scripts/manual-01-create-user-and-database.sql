-- Idempotent script to create PostgreSQL users and database
-- This script is for MANUAL execution (not for docker-entrypoint-initdb.d)
-- Execute as postgres superuser: psql -U postgres -f scripts/manual-01-create-users-and-database.sql

-- Create reader user (read-only, used by GeoResolver API)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'reader') THEN
        CREATE USER reader WITH PASSWORD 'pass';
    END IF;
END
$$;

-- Create database if it doesn't exist (owner will be postgres by default)
SELECT 'CREATE DATABASE geo_resolver'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'geo_resolver')\gexec

-- Connect to geo_resolver database and grant privileges to reader
\c geo_resolver

-- Grant usage on schema public to reader user
GRANT USAGE ON SCHEMA public TO reader;
