# Real-time CDC Data Pipeline Setup

## Architecture Overview

```
PostgreSQL RDS → Debezium → Kafka → ClickHouse → Analytics Tools
     ↓              ↓         ↓         ↓            ↓
  Source DB    CDC Capture  Message   Data WH    Metabase/DBeaver
```

## Current Infrastructure

### Components
- **PostgreSQL RDS**: Source database with logical replication enabled
- **Kafka (Redpanda)**: Message broker for CDC events
- **Debezium**: PostgreSQL CDC connector
- **ClickHouse**: Data warehouse for analytics
- **Kafka Connect**: Connector runtime

### Database Structure
```
ClickHouse:
├── default (PUBLIC - for analytics tools)
│   └── payments (main table - 862K+ records)
└── cdc_internal (HIDDEN - CDC infrastructure)
    ├── kafka_payments (Kafka engine table)
    └── payments_mv (materialized view)
```

## Connection Details

### ClickHouse Data Warehouse
- **Host**: `18.133.124.75`
- **HTTP Port**: `8123`
- **Native Port**: `9000`
- **Database**: `default`
- **Username**: `volume`
- **Password**: `msu&2GS%$*sf`

### PostgreSQL Source
- **Host**: `payments-live-db.c74eo0akc0v7.eu-west-2.rds.amazonaws.com`
- **Port**: `5432`
- **Database**: `payments_db`
- **Admin User**: `postgres_admin`
- **CDC User**: `cdc_user`

### Kafka Connect API
- **URL**: `http://18.133.124.75:8083`

## Current Tables in CDC

### 1. payments
- **Status**: ✅ Active
- **Records**: 862K+
- **Real-time**: Yes
- **Columns**: id, created_at, updated_at, amount, currency, reference, payment_type, volume_payment_status, merchant_id, application_id, shopper_id, payee__account_holder_name, payer__name, payer__email, institution_id, integration_type, failure_reason

## Adding New Tables to CDC

### Step 1: Add Table to PostgreSQL Publication
```sql
-- Connect to PostgreSQL as postgres_admin
ALTER PUBLICATION debezium_publication ADD TABLE public.new_table_name;
```

### Step 2: Update Source Connector
```bash
curl -X PUT -H "Content-Type: application/json" \
http://18.133.124.75:8083/connectors/postgres-source/config -d '{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "payments-live-db.c74eo0akc0v7.eu-west-2.rds.amazonaws.com",
  "database.port": "5432",
  "database.user": "cdc_user",
  "database.password": "cdc_password",
  "database.dbname": "payments_db",
  "topic.prefix": "payments-db",
  "table.include.list": "public.payments,public.new_table_name",
  "plugin.name": "pgoutput",
  "slot.name": "debezium_slot",
  "publication.name": "debezium_publication",
  "publication.autocreate.mode": "disabled",
  "tasks.max": "1"
}'
```

### Step 3: Create ClickHouse Table Structure
```sql
-- Connect to ClickHouse
CREATE TABLE default.new_table_name (
    id String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    -- Add your specific columns here
    column1 String,
    column2 Decimal(18,2),
    column3 DateTime64(3),
    -- Kafka metadata
    _kafka_topic String,
    _kafka_partition Int32,
    _kafka_offset Int64,
    _kafka_timestamp DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (created_at, id)
PARTITION BY toYYYYMM(created_at);
```

### Step 4: Create CDC Infrastructure
```sql
-- Create Kafka table in internal database
CREATE TABLE cdc_internal.kafka_new_table_name (
    after String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'payments-db.public.new_table_name',
    kafka_group_name = 'clickhouse_new_table_name',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Create materialized view for data transformation
CREATE MATERIALIZED VIEW cdc_internal.new_table_name_mv TO default.new_table_name AS
SELECT
    JSONExtractString(after, 'id') as id,
    fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'created_at'))) as created_at,
    fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'updated_at'))) as updated_at,
    -- Map your specific columns
    JSONExtractString(after, 'column1') as column1,
    toDecimal64OrZero(JSONExtractString(after, 'column2'), 2) as column2,
    fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'column3'))) as column3,
    -- Kafka metadata
    'payments-db.public.new_table_name' as _kafka_topic,
    0 as _kafka_partition,
    0 as _kafka_offset,
    now() as _kafka_timestamp
FROM cdc_internal.kafka_new_table_name
WHERE JSONExtractString(after, 'id') != '';
```

### Step 5: Test the Pipeline
```sql
-- Test with an update in PostgreSQL
UPDATE public.new_table_name 
SET updated_at = NOW(), column1 = 'test-value'
WHERE id = 'some-id';

-- Check data in ClickHouse (wait 10-15 seconds)
SELECT count() FROM default.new_table_name;
```

## Automation Script

Create `add_table_to_cdc.py`:

```python
#!/usr/bin/env python3
import requests
import sys

def add_table_to_cdc(table_name, columns_definition):
    """
    Add new table to CDC pipeline
    
    Args:
        table_name: Name of the table (e.g., 'users')
        columns_definition: ClickHouse column definitions
    """
    
    # ClickHouse connection
    CLICKHOUSE_HOST = "18.133.124.75"
    CLICKHOUSE_PORT = "8123"
    CLICKHOUSE_USER = "volume"
    CLICKHOUSE_PASSWORD = "msu&2GS%$*sf"
    BASE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
    
    def execute_query(query):
        response = requests.post(
            BASE_URL,
            params={'user': CLICKHOUSE_USER, 'password': CLICKHOUSE_PASSWORD, 'database': 'default'},
            data=query,
            timeout=30
        )
        if response.status_code == 200:
            print(f"✅ Success: {query[:50]}...")
            return True
        else:
            print(f"❌ Error: {response.text}")
            return False
    
    print(f"🚀 Adding table {table_name} to CDC pipeline...")
    
    # 1. Create main table
    main_table = f"""
    CREATE TABLE default.{table_name} (
        {columns_definition},
        _kafka_topic String,
        _kafka_partition Int32,
        _kafka_offset Int64,
        _kafka_timestamp DateTime64(3)
    ) ENGINE = MergeTree()
    ORDER BY (created_at, id)
    PARTITION BY toYYYYMM(created_at)
    """
    
    # 2. Create Kafka table
    kafka_table = f"""
    CREATE TABLE cdc_internal.kafka_{table_name} (
        after String
    ) ENGINE = Kafka()
    SETTINGS
        kafka_broker_list = 'kafka:9092',
        kafka_topic_list = 'payments-db.public.{table_name}',
        kafka_group_name = 'clickhouse_{table_name}',
        kafka_format = 'JSONEachRow',
        kafka_num_consumers = 1
    """
    
    # 3. Create materialized view (customize JSON extraction as needed)
    mv_query = f"""
    CREATE MATERIALIZED VIEW cdc_internal.{table_name}_mv TO default.{table_name} AS
    SELECT
        JSONExtractString(after, 'id') as id,
        fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'created_at'))) as created_at,
        fromUnixTimestamp64Micro(toUInt64OrZero(JSONExtractString(after, 'updated_at'))) as updated_at,
        -- Add your column mappings here
        'payments-db.public.{table_name}' as _kafka_topic,
        0 as _kafka_partition,
        0 as _kafka_offset,
        now() as _kafka_timestamp
    FROM cdc_internal.kafka_{table_name}
    WHERE JSONExtractString(after, 'id') != ''
    """
    
    execute_query(main_table)
    execute_query(kafka_table)
    execute_query(mv_query)
    
    print(f"✅ Table {table_name} added to CDC pipeline!")
    print(f"📝 Don't forget to:")
    print(f"   1. Add table to PostgreSQL publication")
    print(f"   2. Update Kafka Connect source connector")
    print(f"   3. Customize materialized view column mappings")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 add_table_to_cdc.py <table_name>")
        sys.exit(1)
    
    table_name = sys.argv[1]
    
    # Example columns - customize as needed
    columns = """
    id String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    name String,
    email String,
    status String
    """
    
    add_table_to_cdc(table_name, columns)
```

## Monitoring & Maintenance

### Check Connector Status
```bash
# List all connectors
curl -s http://18.133.124.75:8083/connectors

# Check specific connector status
curl -s http://18.133.124.75:8083/connectors/postgres-source/status
```

### Check Data Flow
```sql
-- Check record counts
SELECT count() FROM payments;

-- Check latest data
SELECT max(_kafka_timestamp) FROM payments;

-- Check for specific table
SELECT count() FROM your_table_name;
```

### Restart Connectors
```bash
# Restart source connector
curl -X POST http://18.133.124.75:8083/connectors/postgres-source/restart

# Restart specific task
curl -X POST http://18.133.124.75:8083/connectors/postgres-source/tasks/0/restart
```

### System Health
```bash
# Check disk space
ssh -i /path/to/clickhouse.pem ubuntu@18.133.124.75 "df -h"

# Check container status
ssh -i /path/to/clickhouse.pem ubuntu@18.133.124.75 "docker ps"

# Check system load
ssh -i /path/to/clickhouse.pem ubuntu@18.133.124.75 "uptime"
```

## Troubleshooting

### Common Issues

1. **No data flowing**
   - Check connector status
   - Verify PostgreSQL publication includes table
   - Check Kafka topic exists

2. **Connector failed**
   - Check PostgreSQL permissions for cdc_user
   - Verify logical replication is enabled
   - Restart connector

3. **Disk space full**
   - Clean up Docker: `docker system prune -f --volumes`
   - Monitor disk usage regularly

4. **Authentication errors**
   - Verify ClickHouse user credentials
   - Check PostgreSQL user permissions

### Log Locations
- **Kafka Connect**: Container logs via `docker logs connect`
- **ClickHouse**: Container logs via `docker logs clickhouse`
- **System**: `/var/log/` on EC2 instance

## Performance Optimization

### ClickHouse Tuning
- Use appropriate partitioning (currently by month)
- Optimize ORDER BY columns for query patterns
- Consider compression settings for large tables

### Kafka Tuning
- Monitor topic lag
- Adjust consumer groups if needed
- Consider increasing partitions for high-volume tables

## Security Notes

- CDC user has minimal permissions (SELECT + replication)
- ClickHouse user restricted to specific database
- All connections use encrypted passwords
- EC2 security groups limit access

## Backup Strategy

### ClickHouse Data
- Regular snapshots of data directory
- Export important tables periodically
- Monitor disk usage and retention policies

### Configuration Backup
- Save connector configurations
- Backup ClickHouse schema definitions
- Document any custom transformations

---

**Last Updated**: November 12, 2025
**Current Status**: ✅ Production Ready
**Tables in CDC**: 1 (payments)
**Total Records**: 862K+
