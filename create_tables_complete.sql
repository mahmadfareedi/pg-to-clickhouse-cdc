-- Drop existing tables if they exist
DROP TABLE IF EXISTS payments_analytics.merchant_application_mv;
DROP TABLE IF EXISTS payments_analytics.user_merchant_mv;
DROP TABLE IF EXISTS payments_analytics.merchant_application;
DROP TABLE IF EXISTS payments_analytics.user_merchant;

-- Create merchant_application table (typical e-commerce structure)
CREATE TABLE payments_analytics.merchant_application (
    id UInt64,
    merchant_id String,
    business_name String,
    business_type String,
    application_status String,
    application_date DateTime64(3),
    approval_date Nullable(DateTime64(3)),
    rejection_reason Nullable(String),
    contact_email String,
    contact_phone Nullable(String),
    business_address Nullable(String),
    tax_id Nullable(String),
    annual_revenue Nullable(Decimal64(2)),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    created_by Nullable(String),
    updated_by Nullable(String)
) ENGINE = MergeTree()
ORDER BY (id, merchant_id, application_date);

-- Create user_merchant table (user-merchant relationship)
CREATE TABLE payments_analytics.user_merchant (
    id UInt64,
    user_id String,
    merchant_id String,
    role String,
    permissions Array(String),
    status String,
    invited_by Nullable(String),
    invited_at Nullable(DateTime64(3)),
    accepted_at Nullable(DateTime64(3)),
    last_login Nullable(DateTime64(3)),
    created_at DateTime64(3),
    updated_at DateTime64(3),
    created_by Nullable(String),
    updated_by Nullable(String)
) ENGINE = MergeTree()
ORDER BY (id, user_id, merchant_id);

-- Create Kafka tables for CDC input
CREATE TABLE payments_analytics.kafka_merchant_application (
    payload String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'payments_db.public.merchant_application',
    kafka_group_name = 'clickhouse_merchant_application_group',
    kafka_format = 'JSONAsString';

CREATE TABLE payments_analytics.kafka_user_merchant (
    payload String
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'payments_db.public.user_merchant',
    kafka_group_name = 'clickhouse_user_merchant_group',
    kafka_format = 'JSONAsString';

-- Create materialized views for CDC processing
CREATE MATERIALIZED VIEW payments_analytics.merchant_application_mv TO payments_analytics.merchant_application AS
SELECT
    JSONExtractUInt(payload, 'after', 'id') as id,
    JSONExtractString(payload, 'after', 'merchant_id') as merchant_id,
    JSONExtractString(payload, 'after', 'business_name') as business_name,
    JSONExtractString(payload, 'after', 'business_type') as business_type,
    JSONExtractString(payload, 'after', 'application_status') as application_status,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'application_date')) as application_date,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'after', 'approval_date')) as approval_date,
    JSONExtractString(payload, 'after', 'rejection_reason') as rejection_reason,
    JSONExtractString(payload, 'after', 'contact_email') as contact_email,
    JSONExtractString(payload, 'after', 'contact_phone') as contact_phone,
    JSONExtractString(payload, 'after', 'business_address') as business_address,
    JSONExtractString(payload, 'after', 'tax_id') as tax_id,
    toDecimal64OrNull(JSONExtractString(payload, 'after', 'annual_revenue'), 2) as annual_revenue,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'created_at')) as created_at,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'updated_at')) as updated_at,
    JSONExtractString(payload, 'after', 'created_by') as created_by,
    JSONExtractString(payload, 'after', 'updated_by') as updated_by
FROM payments_analytics.kafka_merchant_application
WHERE JSONExtractString(payload, 'op') != 'd';

CREATE MATERIALIZED VIEW payments_analytics.user_merchant_mv TO payments_analytics.user_merchant AS
SELECT
    JSONExtractUInt(payload, 'after', 'id') as id,
    JSONExtractString(payload, 'after', 'user_id') as user_id,
    JSONExtractString(payload, 'after', 'merchant_id') as merchant_id,
    JSONExtractString(payload, 'after', 'role') as role,
    JSONExtractArrayRaw(payload, 'after', 'permissions') as permissions,
    JSONExtractString(payload, 'after', 'status') as status,
    JSONExtractString(payload, 'after', 'invited_by') as invited_by,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'after', 'invited_at')) as invited_at,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'after', 'accepted_at')) as accepted_at,
    parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'after', 'last_login')) as last_login,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'created_at')) as created_at,
    parseDateTime64BestEffort(JSONExtractString(payload, 'after', 'updated_at')) as updated_at,
    JSONExtractString(payload, 'after', 'created_by') as created_by,
    JSONExtractString(payload, 'after', 'updated_by') as updated_by
FROM payments_analytics.kafka_user_merchant
WHERE JSONExtractString(payload, 'op') != 'd';
