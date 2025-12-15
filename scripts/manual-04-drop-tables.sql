-- Script to drop all tables, indexes and objects created by manual-03-create-tables.sql
-- This script is idempotent and can be safely run multiple times
-- For MANUAL execution: psql -U postgres -d geo_resolver -f scripts/manual-04-drop-tables.sql

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS timezones CASCADE;
DROP TABLE IF EXISTS cities CASCADE;
DROP TABLE IF EXISTS regions CASCADE;
DROP TABLE IF EXISTS countries CASCADE;
DROP TABLE IF EXISTS last_update CASCADE;

-- Note: Indexes are automatically dropped when tables are dropped
-- Sequences are automatically dropped when tables are dropped

