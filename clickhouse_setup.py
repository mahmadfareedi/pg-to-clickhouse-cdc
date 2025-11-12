#!/usr/bin/env python3
import requests
import json
import time

# ClickHouse connection details
CLICKHOUSE_HOST = "18.133.124.75"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_USER = "volume"
CLICKHOUSE_PASSWORD = "msu&2GS%$*sf"
CLICKHOUSE_DB = "default"

BASE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"

def execute_query(query):
    """Execute ClickHouse query via HTTP"""
    try:
        response = requests.post(
            BASE_URL,
            params={
                'user': CLICKHOUSE_USER,
                'password': CLICKHOUSE_PASSWORD,
                'database': CLICKHOUSE_DB
            },
            data=query,
            timeout=30
        )
        if response.status_code == 200:
            print(f"✅ Success: {query[:50]}...")
            return response.text
        else:
            print(f"❌ Error: {response.text}")
            return None
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None

def create_payments_table():
    """Create payments table matching PostgreSQL structure"""
    query = """
    CREATE TABLE IF NOT EXISTS payments (
        id String,
        created_at DateTime64(3),
        updated_at DateTime64(3),
        amount Decimal(18,2),
        currency String,
        reference String,
        payment_type String,
        volume_payment_status String,
        merchant_id String,
        application_id String,
        shopper_id String,
        payee__account_holder_name String,
        payer__name String,
        payer__email String,
        institution_id String,
        integration_type String,
        failure_reason String,
        _kafka_topic String,
        _kafka_partition Int32,
        _kafka_offset Int64,
        _kafka_timestamp DateTime64(3)
    ) ENGINE = MergeTree()
    ORDER BY (created_at, id)
    PARTITION BY toYYYYMM(created_at)
    """
    return execute_query(query)

def create_materialized_view():
    """Create materialized view for CDC data from Kafka"""
    query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS payments_mv TO payments AS
    SELECT
        JSONExtractString(after, 'id') as id,
        parseDateTime64BestEffort(JSONExtractString(after, 'created_at')) as created_at,
        parseDateTime64BestEffort(JSONExtractString(after, 'updated_at')) as updated_at,
        toDecimal64(JSONExtractString(after, 'amount'), 2) as amount,
        JSONExtractString(after, 'currency') as currency,
        JSONExtractString(after, 'reference') as reference,
        JSONExtractString(after, 'payment_type') as payment_type,
        JSONExtractString(after, 'volume_payment_status') as volume_payment_status,
        JSONExtractString(after, 'merchant_id') as merchant_id,
        JSONExtractString(after, 'application_id') as application_id,
        JSONExtractString(after, 'shopper_id') as shopper_id,
        JSONExtractString(after, 'payee__account_holder_name') as payee__account_holder_name,
        JSONExtractString(after, 'payer__name') as payer__name,
        JSONExtractString(after, 'payer__email') as payer__email,
        JSONExtractString(after, 'institution_id') as institution_id,
        JSONExtractString(after, 'integration_type') as integration_type,
        JSONExtractString(after, 'failure_reason') as failure_reason,
        _topic as _kafka_topic,
        _partition as _kafka_partition,
        _offset as _kafka_offset,
        _timestamp as _kafka_timestamp
    FROM kafka_payments
    WHERE after != ''
    """
    return execute_query(query)

def create_kafka_table():
    """Create Kafka table engine for consuming CDC data"""
    query = """
    CREATE TABLE IF NOT EXISTS kafka_payments (
        after String,
        before String,
        op String,
        ts_ms UInt64,
        _topic String,
        _partition Int32,
        _offset Int64,
        _timestamp DateTime64(3)
    ) ENGINE = Kafka()
    SETTINGS
        kafka_broker_list = 'localhost:9092',
        kafka_topic_list = 'payments-db.public.payments',
        kafka_group_name = 'clickhouse_payments_group',
        kafka_format = 'JSONEachRow',
        kafka_num_consumers = 1
    """
    return execute_query(query)

def create_analytics_views():
    """Create useful analytics views"""
    
    # Daily payments summary
    daily_summary = """
    CREATE VIEW IF NOT EXISTS daily_payments_summary AS
    SELECT
        toDate(created_at) as date,
        currency,
        count() as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        volume_payment_status,
        payment_type
    FROM payments
    GROUP BY date, currency, volume_payment_status, payment_type
    ORDER BY date DESC
    """
    
    # Payment status overview
    status_overview = """
    CREATE VIEW IF NOT EXISTS payment_status_overview AS
    SELECT
        volume_payment_status,
        count() as count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        min(created_at) as first_payment,
        max(created_at) as last_payment
    FROM payments
    GROUP BY volume_payment_status
    ORDER BY count DESC
    """
    
    execute_query(daily_summary)
    execute_query(status_overview)

def main():
    print("🚀 Setting up ClickHouse for CDC payments data...")
    
    # Test connection
    result = execute_query("SELECT 1")
    if not result:
        print("❌ Failed to connect to ClickHouse")
        return
    
    print("✅ Connected to ClickHouse")
    
    # Create tables and views
    print("\n📊 Creating tables...")
    create_kafka_table()
    create_payments_table()
    
    print("\n🔄 Creating materialized view...")
    create_materialized_view()
    
    print("\n📈 Creating analytics views...")
    create_analytics_views()
    
    print("\n✅ Setup complete!")
    print("\nYou can now:")
    print("1. Connect DBeaver to view data")
    print("2. Query payments table: SELECT * FROM payments LIMIT 10")
    print("3. Check daily summary: SELECT * FROM daily_payments_summary")
    print("4. Monitor status: SELECT * FROM payment_status_overview")

if __name__ == "__main__":
    main()
