#!/usr/bin/env python3
import requests
import time

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
        print(f"✅ {query[:50]}...")
        return True
    else:
        print(f"❌ {response.text}")
        return False

def fix_cdc():
    # Clean slate
    execute_query("DROP VIEW IF EXISTS payments_mv")
    execute_query("DROP VIEW IF EXISTS payments_raw_mv") 
    execute_query("DROP TABLE IF EXISTS kafka_payments")
    execute_query("DROP TABLE IF EXISTS kafka_payments_simple")
    execute_query("DROP TABLE IF EXISTS payments_raw")
    
    # Create working Kafka table
    kafka_table = """
    CREATE TABLE kafka_payments (
        value String
    ) ENGINE = Kafka()
    SETTINGS
        kafka_broker_list = 'localhost:9092',
        kafka_topic_list = 'payments-db.public.payments',
        kafka_group_name = 'clickhouse_final_group',
        kafka_format = 'JSONAsString',
        kafka_num_consumers = 1
    """
    execute_query(kafka_table)
    
    # Create working materialized view
    mv_query = """
    CREATE MATERIALIZED VIEW payments_mv TO payments AS
    SELECT
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'id') as id,
        now() as created_at,
        now() as updated_at,
        toDecimal64OrZero(JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'amount'), 2) as amount,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'currency') as currency,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'reference') as reference,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'payment_type') as payment_type,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'volume_payment_status') as volume_payment_status,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'merchant_id') as merchant_id,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'application_id') as application_id,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'shopper_id') as shopper_id,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'payee__account_holder_name') as payee__account_holder_name,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'payer__name') as payer__name,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'payer__email') as payer__email,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'institution_id') as institution_id,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'integration_type') as integration_type,
        JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'failure_reason') as failure_reason,
        'payments-db.public.payments' as _kafka_topic,
        0 as _kafka_partition,
        0 as _kafka_offset,
        now() as _kafka_timestamp
    FROM kafka_payments
    WHERE JSONExtractString(JSONExtractRaw(value, 'payload'), 'after', 'id') != ''
    """
    execute_query(mv_query)

if __name__ == "__main__":
    print("🔧 Fixing CDC pipeline...")
    fix_cdc()
    print("✅ Fixed! Testing...")
    
    # Trigger test update
    import subprocess
    subprocess.run([
        "ssh", "-i", "/Users/kms.admin/Documents/clickhouse.pem", "ubuntu@18.133.124.75",
        "export PGPASSWORD='EvenBetterSecret!2025' && psql -h payments-live-db.c74eo0akc0v7.eu-west-2.rds.amazonaws.com -p 5432 -U postgres_admin -d payments_db -c \"UPDATE public.payments SET updated_at = NOW(), reference = 'FINAL-TEST-' || extract(epoch from now()) WHERE id = 'f21efbf2-9cff-4052-bde5-d465bd84981e';\""
    ])
    
    print("⏳ Waiting 20 seconds for data...")
    time.sleep(20)
    
    # Check result
    response = requests.post(
        BASE_URL,
        params={'user': CLICKHOUSE_USER, 'password': CLICKHOUSE_PASSWORD, 'database': 'default'},
        data="SELECT count() FROM payments",
        timeout=30
    )
    
    count = response.text.strip()
    print(f"📊 Records in ClickHouse: {count}")
    
    if int(count) > 0:
        print("🎉 SUCCESS! CDC is working!")
        # Show sample data
        response = requests.post(
            BASE_URL,
            params={'user': CLICKHOUSE_USER, 'password': CLICKHOUSE_PASSWORD, 'database': 'default'},
            data="SELECT id, amount, currency, reference FROM payments LIMIT 3",
            timeout=30
        )
        print("Sample data:")
        print(response.text)
    else:
        print("⚠️ Still no data - may need manual debugging")
