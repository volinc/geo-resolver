#!/bin/bash
# Convenience script for manual database setup
# This script executes all manual initialization scripts in correct order
# Usage: ./scripts/setup-database.sh

set -e

DB_USER="${POSTGRES_USER:-postgres}"
DB_NAME="${POSTGRES_DB:-georesolver}"

echo "Step 1/3: Creating user and database..."
psql -U "$DB_USER" -f scripts/manual-01-create-user-and-database.sql

echo "Step 2/3: Enabling PostGIS extension..."
psql -U "$DB_USER" -d "$DB_NAME" -f scripts/manual-02-enable-postgis.sql

echo "Step 3/3: Creating tables and indexes..."
psql -U "$DB_USER" -d "$DB_NAME" -f scripts/manual-03-create-tables.sql

echo "Database setup completed successfully!"
