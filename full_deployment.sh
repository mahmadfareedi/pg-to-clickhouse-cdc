#!/bin/bash

echo "Starting full CDC deployment..."

# Step 1: Extract data from PostgreSQL (full load)
echo "Step 1: Extracting data from PostgreSQL..."
./extract_data.sh

# Step 2: Load data to ClickHouse
echo "Step 2: Loading data to ClickHouse..."
./load_to_clickhouse.sh

# Step 3: Setup CDC pipeline
echo "Step 3: Setting up CDC pipeline..."
./deploy_cdc.sh

echo "Full deployment completed!"
