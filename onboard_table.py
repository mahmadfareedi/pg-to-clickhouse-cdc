#!/usr/bin/env python3
import requests
import sys

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

def onboard_table(table_name, columns_definition):
    """
    Onboard new table with CDC
    
    Args:
        table_name: Name of the table (e.g., 'users')
        columns_definition: ClickHouse column definitions
    """
    
    # 1. Create main table
    main_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_definition},
        _kafka_topic String,
        _kafka_partition Int32,
        _kafka_offset Int64,
        _kafka_timestamp DateTime64(3)
    ) ENGINE = MergeTree()
    ORDER BY (created_at, id)
    PARTITION BY toYYYYMM(created_at)
    """
    
    # 2. Create Kafka table
    kafka_table = f"""
    CREATE TABLE IF NOT EXISTS kafka_{table_name} (
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
        kafka_topic_list = 'payments-db.public.{table_name}',
        kafka_group_name = 'clickhouse_{table_name}_group',
        kafka_format = 'JSONEachRow',
        kafka_num_consumers = 1
    """
    
    # 3. Create materialized view (you'll need to customize the JSON extraction)
    mv_query = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}_mv TO {table_name} AS
    SELECT
        -- Add your JSON extraction logic here based on table structure
        JSONExtractString(after, 'id') as id,
        parseDateTime64BestEffort(JSONExtractString(after, 'created_at')) as created_at,
        -- Add more columns as needed
        _topic as _kafka_topic,
        _partition as _kafka_partition,
        _offset as _kafka_offset,
        _timestamp as _kafka_timestamp
    FROM kafka_{table_name}
    WHERE after != ''
    """
    
    print(f"🚀 Onboarding table: {table_name}")
    execute_query(kafka_table)
    execute_query(main_table)
    execute_query(mv_query)
    print(f"✅ Table {table_name} onboarded successfully!")

# Example usage
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 onboard_table.py <table_name>")
        print("Example: python3 onboard_table.py users")
        sys.exit(1)
    
    table_name = sys.argv[1]
    
    # Example for users table - customize columns as needed
    if table_name == "users":
        columns = """
        id String,
        created_at DateTime64(3),
        updated_at DateTime64(3),
        email String,
        name String,
        status String
        """
        onboard_table(table_name, columns)
    else:
        print(f"Please customize column definitions for table: {table_name}")
        print("Edit the script and add column definitions for your table")
