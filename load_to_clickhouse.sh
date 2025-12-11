#!/bin/bash

# ClickHouse connection (assuming default local connection)
CH_HOST="18.133.124.75"
CH_PORT="8123"
CH_USER="default"

echo "Starting data load to ClickHouse..."

# Create tables first
echo "Creating ClickHouse tables..."
clickhouse-client --host $CH_HOST --port $CH_PORT --user $CH_USER --query "$(cat /Users/kms.admin/clickhouse_migration.sql)"

# Load payments data
echo "Loading payments data..."
clickhouse-client --host $CH_HOST --port $CH_PORT --user $CH_USER --query "
INSERT INTO payments_analytics.payments 
SELECT 
    id,
    merchant_id,
    user_id,
    amount,
    currency,
    status,
    payment_method,
    transaction_id,
    reference_id,
    description,
    metadata,
    created_at,
    updated_at,
    'INITIAL_LOAD' as _cdc_operation,
    now64() as _cdc_timestamp
FROM input('id UInt64, merchant_id UInt64, user_id UInt64, amount Decimal(15,2), currency String, status String, payment_method String, transaction_id String, reference_id Nullable(String), description Nullable(String), metadata Nullable(String), created_at DateTime64(3), updated_at DateTime64(3)') 
FORMAT CSV" < /tmp/migration_data/payments.csv

# Load merchant_application data
echo "Loading merchant_application data..."
clickhouse-client --host $CH_HOST --port $CH_PORT --user $CH_USER --query "
INSERT INTO payments_analytics.merchant_application 
SELECT 
    *,
    'INITIAL_LOAD' as _cdc_operation,
    now64() as _cdc_timestamp
FROM input('id UInt64, merchant_name String, business_type String, business_category Nullable(String), email String, phone String, address Nullable(String), city Nullable(String), country String, status String, application_date DateTime64(3), approval_date Nullable(DateTime64(3)), rejected_reason Nullable(String), documents Nullable(String), created_at DateTime64(3), updated_at DateTime64(3)') 
FORMAT CSV" < /tmp/migration_data/merchant_application.csv

# Load user_merchant data
echo "Loading user_merchant data..."
clickhouse-client --host $CH_HOST --port $CH_PORT --user $CH_USER --query "
INSERT INTO payments_analytics.user_merchant 
SELECT 
    *,
    'INITIAL_LOAD' as _cdc_operation,
    now64() as _cdc_timestamp
FROM input('id UInt64, user_id UInt64, merchant_id UInt64, role String, permissions String, status String, invited_by Nullable(UInt64), invited_at Nullable(DateTime64(3)), accepted_at Nullable(DateTime64(3)), created_at DateTime64(3), updated_at DateTime64(3)') 
FORMAT CSV" < /tmp/migration_data/user_merchant.csv

echo "Data loading completed!"

# Verify data
echo "Verifying data counts..."
clickhouse-client --host $CH_HOST --port $CH_PORT --user $CH_USER --query "
SELECT 
    'payments' as table_name, 
    count() as row_count 
FROM payments_analytics.payments
UNION ALL
SELECT 
    'merchant_application' as table_name, 
    count() as row_count 
FROM payments_analytics.merchant_application
UNION ALL
SELECT 
    'user_merchant' as table_name, 
    count() as row_count 
FROM payments_analytics.user_merchant"
