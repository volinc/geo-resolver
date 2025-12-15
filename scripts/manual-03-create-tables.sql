-- Idempotent script to create tables, indexes and other objects inside the database
-- This script is for MANUAL execution
-- Requires: database 'geo_resolver' exists, user 'geo_resolver' exists, PostGIS extension enabled
-- Execute as postgres superuser: psql -U postgres -d geo_resolver -f scripts/manual-03-create-tables.sql

-- Create table for tracking last update time
CREATE TABLE IF NOT EXISTS last_update (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT '1970-01-01 00:00:00+00'::timestamptz
);

-- Insert initial row if not exists with minimal date to trigger data update on first start
INSERT INTO last_update (id, updated_at)
VALUES (1, '1970-01-01 00:00:00+00'::timestamptz)
ON CONFLICT (id) DO NOTHING;

-- Create countries table
CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    iso_alpha2_code VARCHAR(2),
    iso_alpha3_code VARCHAR(3),
    name_latin VARCHAR(255) NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    CONSTRAINT countries_iso_code_check CHECK (iso_alpha2_code IS NOT NULL OR iso_alpha3_code IS NOT NULL),
    CONSTRAINT countries_iso_alpha2_unique UNIQUE (iso_alpha2_code),
    CONSTRAINT countries_iso_alpha3_unique UNIQUE (iso_alpha3_code)
);

-- Create indexes for countries
CREATE INDEX IF NOT EXISTS idx_countries_geometry ON countries USING GIST (geometry);

-- Create regions table
CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(100) NOT NULL,
    name_latin VARCHAR(255) NOT NULL,
    country_iso_alpha2_code VARCHAR(2),
    country_iso_alpha3_code VARCHAR(3),
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    CONSTRAINT regions_country_code_check CHECK (country_iso_alpha2_code IS NOT NULL OR country_iso_alpha3_code IS NOT NULL)
);

-- Create unique indexes for regions (without WHERE clause)
-- PostgreSQL treats NULL as distinct, so records with NULL won't conflict
CREATE UNIQUE INDEX IF NOT EXISTS regions_unique_alpha2 ON regions (identifier, country_iso_alpha2_code);
CREATE UNIQUE INDEX IF NOT EXISTS regions_unique_alpha3 ON regions (identifier, country_iso_alpha3_code);

-- Create indexes for regions
CREATE INDEX IF NOT EXISTS idx_regions_geometry ON regions USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_regions_country_alpha2 ON regions (country_iso_alpha2_code);
CREATE INDEX IF NOT EXISTS idx_regions_country_alpha3 ON regions (country_iso_alpha3_code);

-- Create cities table
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(100) NOT NULL,
    name_latin VARCHAR(255) NOT NULL,
    country_iso_alpha2_code VARCHAR(2),
    country_iso_alpha3_code VARCHAR(3),
    region_identifier VARCHAR(100),
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    CONSTRAINT cities_country_code_check CHECK (country_iso_alpha2_code IS NOT NULL OR country_iso_alpha3_code IS NOT NULL),
    CONSTRAINT cities_unique UNIQUE(identifier, country_iso_alpha2_code, country_iso_alpha3_code)
);

-- Create indexes for cities
CREATE INDEX IF NOT EXISTS idx_cities_geometry ON cities USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_cities_country_alpha2 ON cities (country_iso_alpha2_code);
CREATE INDEX IF NOT EXISTS idx_cities_country_alpha3 ON cities (country_iso_alpha3_code);
CREATE INDEX IF NOT EXISTS idx_cities_region ON cities (region_identifier);

-- Create timezones table
CREATE TABLE IF NOT EXISTS timezones (
    id SERIAL PRIMARY KEY,
    timezone_id VARCHAR(100) NOT NULL UNIQUE,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);

-- Create indexes for timezones
CREATE INDEX IF NOT EXISTS idx_timezones_geometry ON timezones USING GIST (geometry);

-- Grant privileges on tables to geo_resolver user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO geo_resolver;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO geo_resolver;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO geo_resolver;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO geo_resolver;

