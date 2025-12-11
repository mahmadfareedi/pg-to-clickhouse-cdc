#!/bin/bash

# Kafka CDC Setup for PostgreSQL to ClickHouse

echo "Setting up Kafka topics for CDC..."

# Create Kafka topics
docker exec -it kafka kafka-topics.sh --create --topic payments_cdc --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics.sh --create --topic merchant_application_cdc --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics.sh --create --topic user_merchant_cdc --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "Kafka topics created successfully!"

# List topics to verify
docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092
