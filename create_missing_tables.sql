-- Create merchant_application table
CREATE TABLE payments_analytics.merchant_application (
    id UInt64,
    merchant_id String,
    application_type String,
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (id, merchant_id);

-- Create user_merchant table  
CREATE TABLE payments_analytics.user_merchant (
    id UInt64,
    user_id String,
    merchant_id String,
    role String,
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (id, user_id, merchant_id);

-- Create materialized views for CDC
CREATE MATERIALIZED VIEW payments_analytics.merchant_application_mv TO payments_analytics.merchant_application AS
SELECT
    JSONExtractUInt(payload, 'after', 'id') as id,
    JSONExtractString(payload, 'after', 'merchant_id') as merchant_id,
    JSONExtractString(payload, 'after', 'application_type') as application_type,
    JSONExtractString(payload, 'after', 'status') as status,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'created_at')) as created_at,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'updated_at')) as updated_at
FROM payments_analytics.kafka_merchant_application
WHERE JSONExtractString(payload, 'op') != 'd';

CREATE MATERIALIZED VIEW payments_analytics.user_merchant_mv TO payments_analytics.user_merchant AS
SELECT
    JSONExtractUInt(payload, 'after', 'id') as id,
    JSONExtractString(payload, 'after', 'user_id') as user_id,
    JSONExtractString(payload, 'after', 'merchant_id') as merchant_id,
    JSONExtractString(payload, 'after', 'role') as role,
    JSONExtractString(payload, 'after', 'status') as status,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'created_at')) as created_at,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'updated_at')) as updated_at
FROM payments_analytics.kafka_user_merchant
WHERE JSONExtractString(payload, 'op') != 'd';
