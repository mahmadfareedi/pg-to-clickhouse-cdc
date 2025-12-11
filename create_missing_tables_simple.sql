-- Create merchant_application table
CREATE TABLE payments_analytics.merchant_application (
    id UInt64,
    merchant_id String,
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY id;

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
ORDER BY id;
