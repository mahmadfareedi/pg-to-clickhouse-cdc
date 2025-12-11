#!/bin/bash

echo "Getting PostgreSQL table structures..."

# Get merchant_application structure
echo "=== MERCHANT_APPLICATION TABLE ==="
PGPASSWORD='EvenBetterSecret!2025' psql -h payments-live-db.c74eo0akc0v7.eu-west-2.rds.amazonaws.com -p 5432 -U postgres_admin -d payments_db -c "\d merchant_application"

echo ""
echo "=== USER_MERCHANT TABLE ==="
# Get user_merchant structure  
PGPASSWORD='EvenBetterSecret!2025' psql -h payments-live-db.c74eo0akc0v7.eu-west-2.rds.amazonaws.com -p 5432 -U postgres_admin -d payments_db -c "\d user_merchant"

echo ""
echo "Creating ClickHouse tables based on PostgreSQL structure..."

# Create basic tables (you'll need to adjust based on actual structure)
clickhouse-client --query "
CREATE TABLE IF NOT EXISTS payments_analytics.merchant_application (
    id UInt64,
    merchant_id String,
    business_name String,
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY id;
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS payments_analytics.user_merchant (
    id UInt64,
    user_id String,
    merchant_id String,
    role String,
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY id;
"

echo "Checking created tables..."
clickhouse-client --query "SHOW TABLES FROM payments_analytics;"
