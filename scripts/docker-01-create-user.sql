-- Idempotent script to create PostgreSQL user and set as database owner
-- This script is executed automatically by docker-entrypoint-initdb.d
-- Runs as postgres superuser AFTER database is created (via POSTGRES_DB environment variable in docker-compose.yml)

-- Create user if not exists
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'georesolver') THEN
        CREATE USER georesolver WITH PASSWORD 'georesolver_password';
    END IF;
END
$$;

-- Set georesolver as owner of the database
ALTER DATABASE georesolver OWNER TO georesolver;
