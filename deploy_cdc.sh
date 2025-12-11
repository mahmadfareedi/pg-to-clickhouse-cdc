#!/bin/bash

# Deploy CDC pipeline to EC2 instance
EC2_HOST="18.133.124.75"

echo "Deploying CDC setup to EC2..."

# Copy files to EC2
scp -i ~/.ssh/clickhouse.pem setup_kafka_cdc.sh ec2-user@$EC2_HOST:/tmp/
scp -i ~/.ssh/clickhouse.pem debezium_postgres_connector.json ec2-user@$EC2_HOST:/tmp/
scp -i ~/.ssh/clickhouse.pem setup_cdc.sql ec2-user@$EC2_HOST:/tmp/

# Execute on EC2
ssh -i ~/.ssh/clickhouse.pem ec2-user@$EC2_HOST << 'EOF'
# Setup Kafka topics
chmod +x /tmp/setup_kafka_cdc.sh
/tmp/setup_kafka_cdc.sh

# Deploy Debezium connector
curl -X POST -H "Content-Type: application/json" --data @/tmp/debezium_postgres_connector.json http://localhost:8083/connectors

# Setup ClickHouse CDC
clickhouse-client --query "$(cat /tmp/setup_cdc.sql)"

echo "CDC pipeline deployed successfully!"
EOF
