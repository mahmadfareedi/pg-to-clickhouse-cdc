#!/bin/bash

# PostgreSQL connection details
PG_HOST="payments-live-db.c74eo0akc0v7.eu-west-2.rds.amazonaws.com"
PG_PORT="5432"
PG_USER="postgres_admin"
PG_DB="payments_db"
export PGPASSWORD="EvenBetterSecret!2025"

# Create data directory
mkdir -p /tmp/migration_data

echo "Starting full data extraction from PostgreSQL..."

# Extract payments table
echo "Extracting payments table..."
psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c "\COPY (SELECT * FROM payments) TO '/tmp/migration_data/payments.csv' WITH CSV HEADER"

# Extract merchant_application table
echo "Extracting merchant_application table..."
psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c "\COPY (SELECT * FROM merchant_application) TO '/tmp/migration_data/merchant_application.csv' WITH CSV HEADER"

# Extract user_merchant table
echo "Extracting user_merchant table..."
psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c "\COPY (SELECT * FROM user_merchant) TO '/tmp/migration_data/user_merchant.csv' WITH CSV HEADER"

echo "Data extraction completed!"
echo "Files created:"
ls -la /tmp/migration_data/
