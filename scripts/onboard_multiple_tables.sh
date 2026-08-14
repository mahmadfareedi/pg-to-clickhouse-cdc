#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: $0 <table1> <table2> <table3> <table4> ..."
    echo "Example: $0 products orders customers invoices"
    exit 1
fi

TABLES=("$@")
echo "🚀 Onboarding ${#TABLES[@]} tables: ${TABLES[*]}"

# 1. Get current tables from source connector
echo "📝 Getting current table list..."
CURRENT_TABLES=$(curl -s http://localhost:8083/connectors/debezium-postgresql-connector/config | jq -r '.["table.include.list"]')
echo "Current tables: $CURRENT_TABLES"

# 2. Build new table list
NEW_TABLE_LIST="$CURRENT_TABLES"
for table in "${TABLES[@]}"; do
    NEW_TABLE_LIST="${NEW_TABLE_LIST},public.${table}"
done

echo "New table list: $NEW_TABLE_LIST"

# 3. Update source connector
echo "📝 Updating source connector..."
cat > /tmp/batch_source_update.json << EOF
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
  "table.include.list": "$NEW_TABLE_LIST",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
EOF

curl -X PUT -H 'Content-Type: application/json' --data @/tmp/batch_source_update.json \
  http://localhost:8083/connectors/debezium-postgresql-connector/config

echo "✅ Source connector updated"

# 4. Create sink connectors for each table
echo "📤 Creating sink connectors..."
for table in "${TABLES[@]}"; do
    echo "Creating sink for: $table"
    
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
    sleep 2
done

echo "✅ All ${#TABLES[@]} tables onboarded!"
echo "📊 Check status: curl http://localhost:8083/connectors"
echo "🔍 Monitor: http://localhost:8080"

# Cleanup
rm -f /tmp/batch_source_update.json
