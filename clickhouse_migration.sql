-- ClickHouse Migration Script
-- Source: PostgreSQL RDS payments_db
-- Target: ClickHouse

-- 1. Create Database
CREATE DATABASE IF NOT EXISTS payments_analytics;
USE payments_analytics;

-- 2. Create ClickHouse tables matching PostgreSQL schema

-- Payments table
CREATE TABLE IF NOT EXISTS payments (
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
    -- CDC tracking
    _cdc_operation LowCardinality(String) DEFAULT '',
    _cdc_timestamp DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(_cdc_timestamp)
ORDER BY (id, merchant_id)
PARTITION BY toYYYYMM(created_at)
SETTINGS index_granularity = 8192;

-- Merchant Application table
CREATE TABLE IF NOT EXISTS merchant_application (
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
    -- CDC tracking
    _cdc_operation LowCardinality(String) DEFAULT '',
    _cdc_timestamp DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(_cdc_timestamp)
ORDER BY (id)
PARTITION BY toYYYYMM(application_date)
SETTINGS index_granularity = 8192;

-- User Merchant table
CREATE TABLE IF NOT EXISTS user_merchant (
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
    -- CDC tracking
    _cdc_operation LowCardinality(String) DEFAULT '',
    _cdc_timestamp DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(_cdc_timestamp)
ORDER BY (id, user_id, merchant_id)
PARTITION BY toYYYYMM(created_at)
SETTINGS index_granularity = 8192;
