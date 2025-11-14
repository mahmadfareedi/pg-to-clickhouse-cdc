#!/bin/bash

# CDC Disk Space Cleanup Script
set -e

echo "🧹 Starting CDC cleanup..."

# Stop containers
echo "Stopping containers..."
docker compose down

# Clean Docker system
echo "Cleaning Docker system..."
docker system prune -f --volumes
docker image prune -f
docker container prune -f
docker volume prune -f

# Clean temporary data directories
echo "Cleaning temporary data..."
sudo rm -rf /tmp/kafka_data/* 2>/dev/null || true
sudo rm -rf /tmp/clickhouse_data/* 2>/dev/null || true  
sudo rm -rf /tmp/clickhouse_logs/* 2>/dev/null || true

# Create fresh directories
echo "Creating fresh data directories..."
mkdir -p /tmp/kafka_data /tmp/clickhouse_data /tmp/clickhouse_logs
chmod 777 /tmp/kafka_data /tmp/clickhouse_data /tmp/clickhouse_logs

# Clean Docker logs
echo "Cleaning Docker logs..."
sudo truncate -s 0 /var/lib/docker/containers/*/*-json.log 2>/dev/null || true

echo "✅ Cleanup complete!"
echo "💡 Run 'docker compose up -d' to restart services"
