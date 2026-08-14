#!/usr/bin/env python3

import json
import requests
import sys
import subprocess

def onboard_table(table_name, columns):
    """Onboard new table to CDC pipeline"""
    
    # 1. Create ClickHouse objects
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    ) ENGINE = MergeTree() ORDER BY tuple();

    CREATE TABLE IF NOT EXISTS {table_name}_kafka (
        _raw String
    ) ENGINE = Kafka()
    SETTINGS 
        kafka_broker_list = 'kafka:9092',
        kafka_topic_list = 'mydb-replication.public.{table_name}',
        kafka_group_name = '{table_name}_group',
        kafka_format = 'JSONAsString';

    CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}_mv TO {table_name} AS
    SELECT 
        JSONExtractInt(_raw, 'after', 'id') as id,
        JSONExtractString(_raw, 'after', 'name') as name,
        JSONExtractString(_raw, 'after', 'email') as email
    FROM {table_name}_kafka
    WHERE JSONExtractString(_raw, 'op') != 'd';
    """
    
    # Execute ClickHouse SQL
    cmd = ['docker', 'exec', '-i', 'clickhouse', 'clickhouse-client']
    subprocess.run(cmd, input=sql, text=True, check=True)
    print(f"✅ ClickHouse objects created for {table_name}")
    
    # 2. Update connector config
    with open('connectors/postgres-source.json', 'r') as f:
        config = json.load(f)
    
    current_tables = config['config']['table.include.list']
    if table_name not in current_tables:
        config['config']['table.include.list'] = f"{current_tables},public.{table_name}"
        
        with open('connectors/postgres-source.json', 'w') as f:
            json.dump(config, f, indent=2)
        
        # Restart connector
        requests.post('http://localhost:8083/connectors/debezium-postgresql-connector/restart')
        print(f"✅ Added {table_name} to connector and restarted")
    else:
        print(f"⚠️  Table {table_name} already configured")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python onboard_table.py <table_name> \"<columns>\"")
        print("Example: python onboard_table.py users \"id Int32, name String\"")
        sys.exit(1)
    
    table_name = sys.argv[1]
    columns = sys.argv[2]
    
    onboard_table(table_name, columns)
    print(f"🚀 Table {table_name} onboarded successfully!")
