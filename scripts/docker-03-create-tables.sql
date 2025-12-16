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
    iso_alpha2_code TEXT COLLATE "C" NOT NULL,
    iso_alpha3_code TEXT COLLATE "C" NOT NULL,
    name_latin TEXT COLLATE "C" NOT NULL,
    wikidataid VARCHAR(20) COLLATE "C" NOT NULL,
    name_local TEXT,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL,
    CONSTRAINT countries_iso_alpha2_unique UNIQUE (iso_alpha2_code),
    CONSTRAINT countries_iso_alpha3_unique UNIQUE (iso_alpha3_code)
);

-- Create indexes for countries
CREATE UNIQUE INDEX IF NOT EXISTS idx_countries_iso_alpha2 ON countries (iso_alpha2_code);
CREATE UNIQUE INDEX IF NOT EXISTS idx_countries_iso_alpha3 ON countries (iso_alpha3_code);
CREATE INDEX IF NOT EXISTS idx_countries_name_latin ON countries (name_latin text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_countries_geometry ON countries USING GIST (geometry);

-- Create regions table
CREATE TABLE IF NOT EXISTS regions (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(20) COLLATE "C" NOT NULL,
    name_latin TEXT COLLATE "C" NOT NULL,
    country_iso_alpha2_code TEXT COLLATE "C" NOT NULL,
    country_iso_alpha3_code TEXT COLLATE "C" NOT NULL,
    wikidataid VARCHAR(20) COLLATE "C" NOT NULL,
    name_local TEXT NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);

-- Create indexes for regions
CREATE UNIQUE INDEX IF NOT EXISTS idx_regions_identifier ON regions (identifier);
CREATE INDEX IF NOT EXISTS idx_regions_country_alpha2 ON regions (country_iso_alpha2_code);
CREATE INDEX IF NOT EXISTS idx_regions_country_alpha3 ON regions (country_iso_alpha3_code);
CREATE INDEX IF NOT EXISTS idx_regions_name_latin ON regions (name_latin text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_regions_geometry ON regions USING GIST (geometry);

-- Create cities table
CREATE TABLE IF NOT EXISTS cities (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(20) COLLATE "C" NOT NULL,
    name_latin TEXT COLLATE "C" NOT NULL,
    country_iso_alpha2_code TEXT COLLATE "C" NOT NULL,
    country_iso_alpha3_code TEXT COLLATE "C" NOT NULL,
    region_identifier VARCHAR(20) COLLATE "C",
    wikidataid VARCHAR(20) COLLATE "C",
    name_local TEXT NOT NULL,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);

-- Create indexes for cities
CREATE UNIQUE INDEX IF NOT EXISTS idx_cities_identifier ON cities (identifier);
CREATE INDEX IF NOT EXISTS idx_cities_country_alpha2 ON cities (country_iso_alpha2_code);
CREATE INDEX IF NOT EXISTS idx_cities_country_alpha3 ON cities (country_iso_alpha3_code);
CREATE INDEX IF NOT EXISTS idx_cities_region ON cities (region_identifier);
CREATE INDEX IF NOT EXISTS idx_cities_name_latin ON cities (name_latin text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_cities_geometry ON cities USING GIST (geometry);

-- Create timezones table
CREATE TABLE IF NOT EXISTS timezones (
    id SERIAL PRIMARY KEY,
    timezone_id VARCHAR(100) NOT NULL UNIQUE,
    geometry GEOMETRY(MULTIPOLYGON, 4326) NOT NULL
);

-- Create indexes for timezones
CREATE INDEX IF NOT EXISTS idx_timezones_geometry ON timezones USING GIST (geometry);
