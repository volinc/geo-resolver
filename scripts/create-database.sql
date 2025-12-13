-- Create database user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'georesolver') THEN
        CREATE USER georesolver WITH PASSWORD 'georesolver_password';
    END IF;
END
$$;

-- Create database
SELECT 'CREATE DATABASE georesolver OWNER georesolver'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'georesolver')\gexec

-- Connect to the new database
\c georesolver

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE georesolver TO georesolver;

-- Enable PostGIS extension (will be created by application, but can be done here too)
-- CREATE EXTENSION IF NOT EXISTS postgis;

