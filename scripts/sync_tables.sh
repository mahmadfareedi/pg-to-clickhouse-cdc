#!/bin/bash

# Get script directory and config file path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$SCRIPT_DIR/../tables_to_sync.txt"

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Config file not found: $CONFIG_FILE"
    echo "Create it with table names (one per line)"
    exit 1
fi

echo "📁 Reading config from: $CONFIG_FILE"

# Read tables from config file (ignore comments and empty lines)
TABLES=($(grep -v '^#' "$CONFIG_FILE" | grep -v '^$' | tr '\n' ' '))

echo "🔍 Found tables: ${TABLES[*]}"

if [ ${#TABLES[@]} -eq 0 ]; then
    echo "❌ No tables found in $CONFIG_FILE"
    exit 1
fi

echo "🚀 Syncing ${#TABLES[@]} tables: ${TABLES[*]}"

# Build table list for connector
TABLE_LIST=""
for table in "${TABLES[@]}"; do
    if [ -z "$TABLE_LIST" ]; then
        TABLE_LIST="public.${table}"
    else
        TABLE_LIST="${TABLE_LIST},public.${table}"
    fi
done

echo "📝 Table list: $TABLE_LIST"

# Update source connector
cat > /tmp/source_config.json << EOF
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "topic.prefix": "mydb-replication",
  "database.hostname": "postgres_db",
  "database.port": "5432",
  "database.user": "cdc_user",
  "database.password": "cdc_password",
  "database.dbname": "mydb",
  "database.sslmode": "disable",
  "plugin.name": "pgoutput",
  "slot.name": "mydb_slot",
  "snapshot.mode": "initial",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
  "schema.history.internal.kafka.topic": "schema-changes.mydb",
  "table.include.list": "$TABLE_LIST",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
EOF

# Update source connector
echo "📝 Updating source connector..."
curl -X PUT -H 'Content-Type: application/json' --data @/tmp/source_config.json \
  http://localhost:8083/connectors/debezium-postgresql-connector/config

# Create/update sink connectors
echo "📤 Creating sink connectors..."
for table in "${TABLES[@]}"; do
    echo "Processing: $table"
    
    # Check if sink already exists
    if curl -s http://localhost:8083/connectors/clickhouse-sink-${table} > /dev/null 2>&1; then
        echo "  ↻ Sink exists, skipping: $table"
        continue
    fi
    
    cat > /tmp/sink_${table}.json << EOF
{
  "name": "clickhouse-sink-${table}",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "mydb-replication.public.${table}",
    "connection.url": "jdbc:clickhouse+notx://clickhouse:8123/default",
    "connection.user": "volume",
    "connection.password": "msu&2GS%$*sf",
    "table.name.format": "${table}",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id",
    "auto.create": "true",
    "auto.evolve": "true",
    "batch.size": "1000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
EOF

    curl -X POST -H 'Content-Type: application/json' --data @/tmp/sink_${table}.json \
      http://localhost:8083/connectors
    
    rm -f /tmp/sink_${table}.json
    echo "  ✅ Created sink: $table"
done

echo "✅ Sync complete!"
echo "📊 Active connectors: $(curl -s http://localhost:8083/connectors | jq length) total"

# Cleanup
rm -f /tmp/source_config.json
