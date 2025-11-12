#!/usr/bin/env python3
import requests
import sys
import json

def add_table_to_cdc(table_name, columns_definition):
    """
    Add new table to CDC pipeline
    
    Args:
        table_name: Name of the table (e.g., 'users')
        columns_definition: ClickHouse column definitions
    """
    
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
    
    print(f"🚀 Adding table {table_name} to CDC pipeline...")
    
    # 1. Create main table
    main_table = f"""
    CREATE TABLE default.{table_name} (
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
    CREATE TABLE cdc_internal.kafka_{table_name} (
        after String
    ) ENGINE = Kafka()
    SETTINGS
        kafka_broker_list = 'kafka:9092',
        kafka_topic_list = 'payments-db.public.{table_name}',
        kafka_group_name = 'clickhouse_{table_name}',
        kafka_format = 'JSONEachRow',
        kafka_num_consumers = 1
    """
    
    # 3. Create materialized view template
    mv_query = f"""
    CREATE MATERIALIZED VIEW cdc_internal.{table_name}_mv TO default.{table_name} AS
    SELECT
        JSONExtractString(after, 'id') as id,
        fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'created_at'))) as created_at,
        fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'updated_at'))) as updated_at,
        -- TODO: Add your specific column mappings here
        -- JSONExtractString(after, 'column_name') as column_name,
        'payments-db.public.{table_name}' as _kafka_topic,
        0 as _kafka_partition,
        0 as _kafka_offset,
        now() as _kafka_timestamp
    FROM cdc_internal.kafka_{table_name}
    WHERE JSONExtractString(after, 'id') != ''
    """
    
    execute_query(main_table)
    execute_query(kafka_table)
    execute_query(mv_query)
    
    print(f"✅ Table {table_name} added to CDC pipeline!")
    print(f"\n📝 Next steps:")
    print(f"   1. Add to PostgreSQL publication:")
    print(f"      ALTER PUBLICATION debezium_publication ADD TABLE public.{table_name};")
    print(f"   2. Update source connector to include: public.payments,public.{table_name}")
    print(f"   3. Customize materialized view column mappings in cdc_internal.{table_name}_mv")
    print(f"   4. Test with: UPDATE public.{table_name} SET updated_at = NOW() WHERE id = 'test-id';")

def update_source_connector(table_list):
    """Update source connector with new table list"""
    
    connector_config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "payments-live-db.c74eo0akc0v7.eu-west-2.rds.amazonaws.com",
        "database.port": "5432",
        "database.user": "cdc_user",
        "database.password": "cdc_password",
        "database.dbname": "payments_db",
        "topic.prefix": "payments-db",
        "table.include.list": table_list,
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.name": "debezium_publication",
        "publication.autocreate.mode": "disabled",
        "tasks.max": "1"
    }
    
    try:
        response = requests.put(
            "http://18.133.124.75:8083/connectors/postgres-source/config",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config),
            timeout=30
        )
        if response.status_code == 200:
            print("✅ Source connector updated successfully!")
            return True
        else:
            print(f"❌ Failed to update connector: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error updating connector: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 add_table_to_cdc.py <table_name> [update_connector]")
        print("Example: python3 add_table_to_cdc.py users")
        print("Example: python3 add_table_to_cdc.py users update")
        sys.exit(1)
    
    table_name = sys.argv[1]
    update_connector_flag = len(sys.argv) > 2 and sys.argv[2] == "update"
    
    # Example columns - customize as needed
    columns = """
    id String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    name String,
    email String,
    status String
    """
    
    add_table_to_cdc(table_name, columns)
    
    if update_connector_flag:
        current_tables = f"public.payments,public.{table_name}"
        print(f"\n🔄 Updating source connector...")
        update_source_connector(current_tables)
