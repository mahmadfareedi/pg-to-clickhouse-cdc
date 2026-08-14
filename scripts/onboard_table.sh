#!/bin/bash

# PostgreSQL to ClickHouse CDC Table Onboarding Script
# Usage: ./onboard_table.sh <table_name> <postgres_schema_file>

set -e

TABLE_NAME=$1
POSTGRES_SCHEMA_FILE=$2

if [ -z "$TABLE_NAME" ] || [ -z "$POSTGRES_SCHEMA_FILE" ]; then
    echo "Usage: $0 <table_name> <postgres_schema_file>"
    echo "Example: $0 users /path/to/users_table.sql"
    exit 1
fi

if [ ! -f "$POSTGRES_SCHEMA_FILE" ]; then
    echo "Error: PostgreSQL schema file not found: $POSTGRES_SCHEMA_FILE"
    exit 1
fi

echo "🚀 Onboarding table: $TABLE_NAME"

# ClickHouse connection details
CLICKHOUSE_HOST="localhost:8123"
CLICKHOUSE_USER="volume"
CLICKHOUSE_PASSWORD="msu&2GS%\$*sf"

# Kafka Connect details
CONNECT_HOST="localhost:8083"

echo "📋 Step 1: Creating ClickHouse table structure..."

# Generate ClickHouse table from PostgreSQL schema
# This is a simplified mapping - you may need to adjust for complex types
cat > "/tmp/${TABLE_NAME}_clickhouse.sql" << EOF
-- Auto-generated ClickHouse table for $TABLE_NAME
-- Modify data types as needed for your specific schema

CREATE TABLE ${TABLE_NAME} (
    id Int64,
    created_at DateTime64(6, 'UTC'),
    -- Add your columns here based on PostgreSQL schema
    -- Example mappings:
    -- BIGSERIAL -> Int64
    -- VARCHAR -> String  
    -- TIMESTAMPTZ -> DateTime64(6, 'UTC')
    -- BOOLEAN -> Bool
    -- JSON/JSONB -> String
    -- UUID -> UUID
    -- Arrays -> Array(Type)
    
    -- Copy column definitions from: $POSTGRES_SCHEMA_FILE
    -- and convert to ClickHouse types
    
) ENGINE = MergeTree()
ORDER BY id;

-- Kafka consumer table
CREATE TABLE ${TABLE_NAME}_kafka (_raw String) 
ENGINE = Kafka() 
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'mydb-replication.public.${TABLE_NAME}',
    kafka_group_name = 'clickhouse_${TABLE_NAME}',
    kafka_format = 'LineAsString';

-- Materialized view for real-time data transformation
CREATE MATERIALIZED VIEW ${TABLE_NAME}_mv TO ${TABLE_NAME} AS
SELECT 
    JSONExtractInt(_raw, 'after', 'id') as id,
    parseDateTimeBestEffort(JSONExtractString(_raw, 'after', 'created_at')) as created_at
    -- Add JSON extraction for all your columns:
    -- JSONExtractString(_raw, 'after', 'column_name') as column_name,
    -- JSONExtractInt(_raw, 'after', 'int_column') as int_column,
    -- JSONExtractBool(_raw, 'after', 'bool_column') as bool_column,
    -- toUUIDOrNull(JSONExtractString(_raw, 'after', 'uuid_column')) as uuid_column
FROM ${TABLE_NAME}_kafka;
EOF

echo "📝 Generated ClickHouse schema: /tmp/${TABLE_NAME}_clickhouse.sql"
echo "⚠️  Please review and modify the schema to match your PostgreSQL table structure"
echo ""
echo "📖 PostgreSQL schema reference:"
echo "----------------------------------------"
head -20 "$POSTGRES_SCHEMA_FILE"
echo "----------------------------------------"
echo ""

read -p "🤔 Have you reviewed and updated the ClickHouse schema? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Please edit /tmp/${TABLE_NAME}_clickhouse.sql and run this script again"
    exit 1
fi

echo "📊 Step 2: Creating ClickHouse tables..."
curl -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" -X POST "http://$CLICKHOUSE_HOST/" --data-binary "@/tmp/${TABLE_NAME}_clickhouse.sql"

echo "🔗 Step 3: Updating source connector..."

# Get current connector config
curl -s "http://$CONNECT_HOST/connectors/debezium-postgresql-connector/config" > "/tmp/current_config.json"

# Update table.include.list
python3 << EOF
import json
import sys

try:
    with open('/tmp/current_config.json', 'r') as f:
        config = json.load(f)
    
    current_tables = config.get('table.include.list', '')
    if '$TABLE_NAME' not in current_tables:
        if current_tables:
            new_tables = current_tables + ',public.$TABLE_NAME'
        else:
            new_tables = 'public.$TABLE_NAME'
        config['table.include.list'] = new_tables
        
        with open('/tmp/updated_config.json', 'w') as f:
            json.dump(config, f, indent=2)
        print("✅ Updated connector configuration")
    else:
        print("ℹ️  Table already in connector configuration")
        with open('/tmp/updated_config.json', 'w') as f:
            json.dump(config, f, indent=2)
            
except Exception as e:
    print(f"❌ Error updating config: {e}")
    sys.exit(1)
EOF

# Update the connector
curl -X PUT -H 'Content-Type: application/json' \
  --data @/tmp/updated_config.json \
  "http://$CONNECT_HOST/connectors/debezium-postgresql-connector/config"

echo ""
echo "🎉 Table onboarding completed!"
echo ""
echo "📋 Next steps:"
echo "1. Insert test data in PostgreSQL:"
echo "   INSERT INTO public.$TABLE_NAME (column1) VALUES ('test');"
echo ""
echo "2. Verify data in ClickHouse:"
echo "   SELECT * FROM $TABLE_NAME ORDER BY id DESC LIMIT 5;"
echo ""
echo "3. Monitor CDC flow:"
echo "   - Kafka UI: http://localhost:8080"
echo "   - ClickHouse Tabix: http://localhost:8082"
echo ""
echo "🔍 Check connector status:"
echo "curl http://$CONNECT_HOST/connectors/debezium-postgresql-connector/status"

# Cleanup
rm -f /tmp/current_config.json /tmp/updated_config.json

echo ""
echo "🚀 CDC pipeline ready for $TABLE_NAME!"
