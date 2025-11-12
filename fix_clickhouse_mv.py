#!/usr/bin/env python3
import requests

# ClickHouse connection
CLICKHOUSE_HOST = "18.133.124.75"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_USER = "volume"
CLICKHOUSE_PASSWORD = "msu&2GS%$*sf"
BASE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"

def execute_query(query):
    response = requests.post(
        BASE_URL,
        params={'user': CLICKHOUSE_USER, 'password': CLICKHOUSE_PASSWORD, 'database': 'default'},
        data=query,
        timeout=30
    )
    if response.status_code == 200:
        print(f"✅ Success: {query[:50]}...")
        return True
    else:
        print(f"❌ Error: {response.text}")
        return False

def fix_materialized_view():
    """Drop and recreate materialized view with correct Debezium format parsing"""
    
    # Drop existing materialized view
    drop_mv = "DROP VIEW IF EXISTS payments_mv"
    execute_query(drop_mv)
    
    # Create new Kafka table with correct format
    drop_kafka = "DROP TABLE IF EXISTS kafka_payments"
    execute_query(drop_kafka)
    
    kafka_table = """
    CREATE TABLE kafka_payments (
        payload String
    ) ENGINE = Kafka()
    SETTINGS
        kafka_broker_list = 'localhost:9092',
        kafka_topic_list = 'payments-db.public.payments',
        kafka_group_name = 'clickhouse_payments_group_v2',
        kafka_format = 'JSONAsString',
        kafka_num_consumers = 1
    """
    execute_query(kafka_table)
    
    # Create new materialized view with proper Debezium parsing
    mv_query = """
    CREATE MATERIALIZED VIEW payments_mv TO payments AS
    SELECT
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'id') as id,
        fromUnixTimestamp64Micro(toUInt64(JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'created_at'))) as created_at,
        fromUnixTimestamp64Micro(toUInt64(JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'updated_at'))) as updated_at,
        toDecimal64OrZero(base64Decode(JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'amount')), 2) as amount,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'currency') as currency,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'reference') as reference,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'payment_type') as payment_type,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'volume_payment_status') as volume_payment_status,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'merchant_id') as merchant_id,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'application_id') as application_id,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'shopper_id') as shopper_id,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'payee__account_holder_name') as payee__account_holder_name,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'payer__name') as payer__name,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'payer__email') as payer__email,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'institution_id') as institution_id,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'integration_type') as integration_type,
        JSONExtractString(JSONExtractString(payload, 'payload'), 'after', 'failure_reason') as failure_reason,
        JSONExtractString(payload, 'topic') as _kafka_topic,
        toInt32(JSONExtractString(payload, 'partition')) as _kafka_partition,
        toInt64(JSONExtractString(payload, 'offset')) as _kafka_offset,
        fromUnixTimestamp64Milli(toUInt64(JSONExtractString(payload, 'timestamp'))) as _kafka_timestamp
    FROM kafka_payments
    WHERE JSONExtractString(JSONExtractString(payload, 'payload'), 'after') != ''
    """
    execute_query(mv_query)

if __name__ == "__main__":
    print("🔧 Fixing ClickHouse materialized view for Debezium format...")
    fix_materialized_view()
    print("✅ Fixed! Data should start flowing now.")
    print("Wait 30 seconds then check: SELECT count() FROM payments")
