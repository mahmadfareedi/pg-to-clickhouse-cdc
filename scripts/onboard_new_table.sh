#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <table_name>"
    echo "Example: $0 products"
    exit 1
fi

TABLE_NAME=$1
echo "🚀 Onboarding table: $TABLE_NAME"

# 1. Update source connector to include new table
echo "📝 Updating source connector..."
CURRENT_TABLES=$(curl -s http://localhost:8083/connectors/debezium-postgresql-connector/config | jq -r '.["table.include.list"]')
NEW_TABLES="${CURRENT_TABLES},public.${TABLE_NAME}"

# Create updated config
cat > /tmp/updated_source.json << EOF
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
  "table.include.list": "$NEW_TABLES",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
}
EOF

# Update connector
curl -X PUT -H 'Content-Type: application/json' --data @/tmp/updated_source.json \
  http://localhost:8083/connectors/debezium-postgresql-connector/config

# 2. Create sink connector for new table
echo "📤 Creating sink connector for $TABLE_NAME..."
cat > /tmp/sink_${TABLE_NAME}.json << EOF
{
  "name": "clickhouse-sink-${TABLE_NAME}",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "mydb-replication.public.${TABLE_NAME}",
    "connection.url": "jdbc:clickhouse://clickhouse:8123/default",
    "connection.user": "volume",
    "connection.password": "msu&2GS%$*sf",
    "table.name.format": "${TABLE_NAME}",
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

curl -X POST -H 'Content-Type: application/json' --data @/tmp/sink_${TABLE_NAME}.json \
  http://localhost:8083/connectors

echo "✅ Table $TABLE_NAME onboarded!"
echo "📊 Check status: curl http://localhost:8083/connectors"
echo "🔍 Monitor: http://localhost:8080 (Redpanda Console)"

# Cleanup
rm -f /tmp/updated_source.json /tmp/sink_${TABLE_NAME}.json
