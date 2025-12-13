-- Idempotent script to create PostgreSQL user and database
-- This script is for MANUAL execution (not for docker-entrypoint-initdb.d)
-- Execute as postgres superuser: psql -U postgres -f scripts/manual-01-create-user-and-database.sql

-- Create user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'georesolver') THEN
        CREATE USER georesolver WITH PASSWORD 'georesolver_password';
    END IF;
END
$$;

-- Create database if not exists
SELECT 'CREATE DATABASE georesolver OWNER georesolver'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'georesolver')\gexec

