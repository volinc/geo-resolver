-- Idempotent script to create PostgreSQL user
-- This script is executed automatically by docker-entrypoint-initdb.d
-- Runs as postgres superuser BEFORE database is created
-- Note: Database creation is handled by POSTGRES_DB environment variable in docker-compose.yml

-- Create user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'georesolver') THEN
        CREATE USER georesolver WITH PASSWORD 'georesolver_password';
    END IF;
END
$$;

