-- Idempotent script to create tables, indexes and other objects inside the database
-- This script is executed automatically by docker-entrypoint-initdb.d
-- Runs after database is created and PostGIS is enabled

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
    iso_alpha2_code VARCHAR(2) NOT NULL UNIQUE,
    name_latin VARCHAR(255) NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);

-- Create indexes for countries
CREATE INDEX IF NOT EXISTS idx_countries_geometry ON countries USING GIST (geometry);

-- Create regions table
CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(100) NOT NULL,
    name_latin VARCHAR(255) NOT NULL,
    country_iso_alpha2_code VARCHAR(2) NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    UNIQUE(identifier, country_iso_alpha2_code)
);

-- Create indexes for regions
CREATE INDEX IF NOT EXISTS idx_regions_geometry ON regions USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_regions_country ON regions (country_iso_alpha2_code);

-- Create cities table
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(100) NOT NULL,
    name_latin VARCHAR(255) NOT NULL,
    country_iso_alpha2_code VARCHAR(2) NOT NULL,
    region_identifier VARCHAR(100),
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    UNIQUE(identifier, country_iso_alpha2_code)
);

-- Create indexes for cities
CREATE INDEX IF NOT EXISTS idx_cities_geometry ON cities USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_cities_country ON cities (country_iso_alpha2_code);
CREATE INDEX IF NOT EXISTS idx_cities_region ON cities (region_identifier);

-- Create timezones table
CREATE TABLE IF NOT EXISTS timezones (
    id SERIAL PRIMARY KEY,
    timezone_id VARCHAR(100) NOT NULL UNIQUE,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);

-- Create indexes for timezones
CREATE INDEX IF NOT EXISTS idx_timezones_geometry ON timezones USING GIST (geometry);

-- Grant privileges on tables to georesolver user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO georesolver;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO georesolver;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO georesolver;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO georesolver;

