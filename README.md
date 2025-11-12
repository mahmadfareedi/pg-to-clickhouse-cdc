# PostgreSQL to ClickHouse CDC Pipeline

Real-time Change Data Capture (CDC) pipeline from PostgreSQL to ClickHouse using Debezium, Kafka, and ClickHouse native Kafka engine.

## Architecture

```
PostgreSQL → Debezium → Kafka → ClickHouse (Native Kafka Engine)
```

**Key Components:**
- **Debezium**: Captures PostgreSQL changes via logical replication
- **Kafka**: Message streaming (Redpanda - no ZooKeeper needed)
- **ClickHouse**: Native Kafka engine + Materialized Views for real-time data transformation
- **Management UIs**: Kafka UI, Redpanda Console, ClickHouse Tabix

## Features

✅ **Real-time CDC** with sub-second latency  
✅ **Full initial load** + ongoing change capture  
✅ **Exact schema matching** (100+ column support)  
✅ **Complex data types** (Arrays, JSON, UUID, Enums, etc.)  
✅ **No JDBC limitations** (uses ClickHouse native Kafka engine)  
✅ **Multi-table support** with easy onboarding  
✅ **Apple Silicon compatible** (Redpanda instead of Kafka+ZooKeeper)  

## Quick Start

### 1. Start the Stack
```bash
docker compose up -d --build
```

### 2. Access UIs
- **Kafka UI**: http://localhost:8080
- **Redpanda Console**: http://localhost:8081  
- **ClickHouse Tabix**: http://localhost:8082
- **Kafka Connect REST**: http://localhost:8083
- **ClickHouse HTTP**: http://localhost:8123

### 3. Configure Source Database
Ensure your PostgreSQL has:
```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
-- Restart PostgreSQL
```

### 4. Set Up CDC
```bash
# Configure your database connection in connectors/postgres-source.json
# Then create the connector
curl -X POST -H 'Content-Type: application/json' \
  --data @connectors/postgres-source.json \
  http://localhost:8083/connectors
```

## Configuration Files

### Source Connector (`connectors/postgres-source.json`)
```json
{
  "name": "debezium-postgresql-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "your-postgres-host",
    "database.port": "5432",
    "database.user": "cdc_user",
    "database.password": "cdc_password",
    "database.dbname": "your_database",
    "table.include.list": "public.your_table",
    "snapshot.mode": "initial"
  }
}
```

### ClickHouse Setup (`scripts/setup_clickhouse_table.sql`)
```sql
-- Main table (exact schema match)
CREATE TABLE your_table (...) ENGINE = MergeTree() ORDER BY id;

-- Kafka consumer table  
CREATE TABLE your_table_kafka (_raw String) ENGINE = Kafka() 
SETTINGS kafka_topic_list = 'mydb-replication.public.your_table';

-- Materialized view for real-time transformation
CREATE MATERIALIZED VIEW your_table_mv TO your_table AS
SELECT JSONExtractInt(_raw, 'after', 'id') as id, ...
FROM your_table_kafka;
```

## Adding New Tables

### 1. Update Source Connector
```bash
# Add table to include list
"table.include.list": "public.table1,public.table2,public.new_table"
```

### 2. Create ClickHouse Schema
```bash
# Use the onboarding script
./scripts/onboard_table.sh new_table_name /path/to/postgres/schema.sql
```

### 3. Verify CDC Flow
```bash
# Insert test data in PostgreSQL
INSERT INTO new_table (col1) VALUES ('test');

# Check ClickHouse (should appear within seconds)
SELECT * FROM new_table ORDER BY id DESC LIMIT 1;
```

## Monitoring & Management

### Check Connector Status
```bash
curl http://localhost:8083/connectors/debezium-postgresql-connector/status
```

### View Kafka Topics
- Kafka UI: http://localhost:8080
- Topics follow pattern: `{topic.prefix}.{schema}.{table}`

### ClickHouse Queries
```sql
-- Check data count
SELECT count(*) FROM your_table;

-- View recent changes  
SELECT * FROM your_table ORDER BY id DESC LIMIT 10;

-- Check Kafka consumption
SELECT count(*) FROM your_table_kafka 
SETTINGS stream_like_engine_allow_direct_select=1;
```

## Production Considerations

### Security
- Enable authentication for all services
- Use SSL/TLS for connections
- Restrict network access with firewalls

### Performance
- Tune Kafka partitions for high throughput
- Configure ClickHouse MergeTree settings
- Monitor replication lag

### High Availability
- Set up Kafka replication
- Configure ClickHouse clusters
- Implement backup strategies

## Troubleshooting

### Common Issues

**No data in ClickHouse:**
```bash
# Check connector status
curl http://localhost:8083/connectors/debezium-postgresql-connector/status

# Verify Kafka messages
# Access Kafka UI at http://localhost:8080
```

**Schema evolution:**
```sql
-- ClickHouse handles schema changes automatically
-- For major changes, recreate materialized view
DROP VIEW your_table_mv;
CREATE MATERIALIZED VIEW your_table_mv TO your_table AS ...
```

**Performance tuning:**
```sql
-- Optimize ClickHouse table
OPTIMIZE TABLE your_table FINAL;

-- Check table statistics  
SELECT * FROM system.parts WHERE table = 'your_table';
```

## File Structure

```
cdc/
├── docker-compose.yml          # Main stack definition
├── connectors/                 # Kafka Connect configurations
│   ├── postgres-source.json    # Debezium source connector
│   └── postgres-source.example.json
├── scripts/                    # Automation scripts
│   ├── setup_clickhouse_table.sql
│   ├── onboard_table.sh
│   └── monitoring.sql
├── clickhouse/                 # ClickHouse configurations
│   ├── exact_clickhouse_table.sql
│   └── materialized_view.sql
└── README.md                   # This file
```

## Support

For issues and questions:
1. Check connector status via REST API
2. Review Kafka UI for message flow
3. Verify ClickHouse table structures
4. Monitor system resources

**Real-time CDC pipeline ready for production! 🚀**
