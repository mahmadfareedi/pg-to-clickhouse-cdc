-- Create tables for each synced table using native Kafka engine

-- Users table
CREATE TABLE users (
    id Int32,
    name String,
    email String,
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE users_kafka (
    _raw String
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'mydb-replication.public.users',
    kafka_group_name = 'clickhouse_users',
    kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW users_mv TO users AS
SELECT 
    JSONExtractInt(_raw, 'id') as id,
    JSONExtractString(_raw, 'name') as name,
    JSONExtractString(_raw, 'email') as email,
    parseDateTime32BestEffort(JSONExtractString(_raw, 'created_at')) as created_at
FROM users_kafka;

-- Products table
CREATE TABLE products (
    id Int32,
    name String,
    price Decimal(10,2),
    category String,
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE products_kafka (
    _raw String
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'mydb-replication.public.products',
    kafka_group_name = 'clickhouse_products',
    kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW products_mv TO products AS
SELECT 
    JSONExtractInt(_raw, 'id') as id,
    JSONExtractString(_raw, 'name') as name,
    JSONExtractFloat(_raw, 'price') as price,
    JSONExtractString(_raw, 'category') as category,
    parseDateTime32BestEffort(JSONExtractString(_raw, 'created_at')) as created_at
FROM products_kafka;

-- Orders table
CREATE TABLE orders (
    id Int32,
    user_id Int32,
    product_id Int32,
    quantity Int32,
    total_amount Decimal(10,2),
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE orders_kafka (
    _raw String
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'mydb-replication.public.orders',
    kafka_group_name = 'clickhouse_orders',
    kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW orders_mv TO orders AS
SELECT 
    JSONExtractInt(_raw, 'id') as id,
    JSONExtractInt(_raw, 'user_id') as user_id,
    JSONExtractInt(_raw, 'product_id') as product_id,
    JSONExtractInt(_raw, 'quantity') as quantity,
    JSONExtractFloat(_raw, 'total_amount') as total_amount,
    parseDateTime32BestEffort(JSONExtractString(_raw, 'created_at')) as created_at
FROM orders_kafka;

-- Customers table
CREATE TABLE customers (
    id Int32,
    name String,
    email String,
    phone String,
    address String,
    created_at DateTime
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE customers_kafka (
    _raw String
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'mydb-replication.public.customers',
    kafka_group_name = 'clickhouse_customers',
    kafka_format = 'JSONAsString';

CREATE MATERIALIZED VIEW customers_mv TO customers AS
SELECT 
    JSONExtractInt(_raw, 'id') as id,
    JSONExtractString(_raw, 'name') as name,
    JSONExtractString(_raw, 'email') as email,
    JSONExtractString(_raw, 'phone') as phone,
    JSONExtractString(_raw, 'address') as address,
    parseDateTime32BestEffort(JSONExtractString(_raw, 'created_at')) as created_at
FROM customers_kafka;
