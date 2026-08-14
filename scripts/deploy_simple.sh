#!/bin/bash

echo "🚀 Deploying Simple Direct Replication..."

# 1. Start services
docker compose up -d

# 2. Wait for ClickHouse
echo "Waiting for ClickHouse..."
sleep 10

# 3. Create table in ClickHouse
echo "Creating ClickHouse table..."
docker exec clickhouse clickhouse-client --query "$(cat scripts/simple_setup.sql)"

# 4. Deploy source connector
echo "Deploying source connector..."
curl -X POST -H 'Content-Type: application/json' \
  --data @connectors/postgres-source.json \
  http://localhost:8083/connectors

# 5. Deploy sink connector  
echo "Deploying sink connector..."
curl -X POST -H 'Content-Type: application/json' \
  --data @connectors/clickhouse-direct-sink.json \
  http://localhost:8083/connectors

echo "✅ Simple replication deployed!"
echo "📊 Check status: curl http://localhost:8083/connectors"
