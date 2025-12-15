-- Idempotent script to create PostgreSQL user and database
-- This script is for MANUAL execution (not for docker-entrypoint-initdb.d)
-- Execute as postgres superuser: psql -U postgres -f scripts/manual-01-create-user-and-database.sql

-- Create user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'geo_resolver') THEN
        CREATE USER geo_resolver WITH PASSWORD 'pass';
    END IF;
END
$$;

-- Create database if not exists
SELECT 'CREATE DATABASE geo_resolver OWNER geo_resolver'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'geo_resolver')\gexec

