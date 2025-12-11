-- CDC Setup for PostgreSQL to ClickHouse
-- This sets up logical replication for ongoing changes

-- 1. Create CDC queue tables in ClickHouse for incoming changes
USE payments_analytics;

-- CDC queue for payments
CREATE TABLE IF NOT EXISTS payments_cdc_queue (
    id UInt64,
    merchant_id UInt64,
    user_id UInt64,
    amount Decimal(15,2),
    currency FixedString(3),
    status LowCardinality(String),
    payment_method LowCardinality(String),
    transaction_id String,
    reference_id Nullable(String),
    description Nullable(String),
    metadata Nullable(String),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _cdc_operation LowCardinality(String),
    _cdc_timestamp DateTime64(3),
    _cdc_lsn UInt64
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'payments_cdc',
    kafka_group_name = 'clickhouse_payments_cdc',
    kafka_format = 'JSONEachRow';

-- CDC queue for merchant_application
CREATE TABLE IF NOT EXISTS merchant_application_cdc_queue (
    id UInt64,
    merchant_name String,
    business_type LowCardinality(String),
    business_category Nullable(String),
    email String,
    phone String,
    address Nullable(String),
    city Nullable(String),
    country FixedString(2),
    status LowCardinality(String),
    application_date DateTime64(3),
    approval_date Nullable(DateTime64(3)),
    rejected_reason Nullable(String),
    documents Nullable(String),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _cdc_operation LowCardinality(String),
    _cdc_timestamp DateTime64(3),
    _cdc_lsn UInt64
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'merchant_application_cdc',
    kafka_group_name = 'clickhouse_merchant_application_cdc',
    kafka_format = 'JSONEachRow';

-- CDC queue for user_merchant
CREATE TABLE IF NOT EXISTS user_merchant_cdc_queue (
    id UInt64,
    user_id UInt64,
    merchant_id UInt64,
    role LowCardinality(String),
    permissions String,
    status LowCardinality(String),
    invited_by Nullable(UInt64),
    invited_at Nullable(DateTime64(3)),
    accepted_at Nullable(DateTime64(3)),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    _cdc_operation LowCardinality(String),
    _cdc_timestamp DateTime64(3),
    _cdc_lsn UInt64
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'user_merchant_cdc',
    kafka_group_name = 'clickhouse_user_merchant_cdc',
    kafka_format = 'JSONEachRow';

-- 2. Create materialized views to process CDC data
CREATE MATERIALIZED VIEW IF NOT EXISTS payments_cdc_mv TO payments AS
SELECT 
    id, merchant_id, user_id, amount, currency, status, payment_method,
    transaction_id, reference_id, description, metadata, created_at, updated_at,
    _cdc_operation, _cdc_timestamp
FROM payments_cdc_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS merchant_application_cdc_mv TO merchant_application AS
SELECT 
    id, merchant_name, business_type, business_category, email, phone,
    address, city, country, status, application_date, approval_date,
    rejected_reason, documents, created_at, updated_at,
    _cdc_operation, _cdc_timestamp
FROM merchant_application_cdc_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_merchant_cdc_mv TO user_merchant AS
SELECT 
    id, user_id, merchant_id, role, permissions, status,
    invited_by, invited_at, accepted_at, created_at, updated_at,
    _cdc_operation, _cdc_timestamp
FROM user_merchant_cdc_queue;
