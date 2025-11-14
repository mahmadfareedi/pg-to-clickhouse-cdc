-- Simple Direct Replication Setup
-- Create exact same table structure as PostgreSQL

-- Example table (replace with your actual PostgreSQL schema)
CREATE TABLE IF NOT EXISTS wide_table_100cols (
    id Int32,
    created_at DateTime,
    smallint_col Int16,
    integer_col Int32,
    bigint_col Int64,
    decimal_col Decimal(10,2),
    numeric_col Decimal(15,5),
    real_col Float32,
    double_col Float64,
    varchar_col String,
    text_col String,
    boolean_col UInt8,
    date_col Date,
    timestamp_col DateTime,
    json_col String,
    uuid_col String
) ENGINE = MergeTree()
ORDER BY id;

-- No Kafka engine needed - using direct JDBC sink
-- No materialized views needed - direct insert
