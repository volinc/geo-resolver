ALTER DATABASE geo_resolver OWNER TO postgres;

REASSIGN OWNED BY reader TO postgres;
DROP OWNED BY reader;
DROP USER reader;

CREATE USER reader WITH PASSWORD 'pass'
GRANT CONNECT ON DATABASE geo_resolver TO reader;

-- Important: You must switch to the geo_resolver database for the following commands to take effect
-- 1. Grant usage on the schema (typically public)
GRANT USAGE ON SCHEMA public TO reader;
-- 2. Grant SELECT on all CURRENT tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;
-- 3. Grant SELECT on all CURRENT sequences (optional)
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO reader;
-- 4. Automate permissions for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reader;