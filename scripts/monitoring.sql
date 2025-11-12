-- ClickHouse CDC Monitoring Queries

-- 1. Check table row counts
SELECT 
    table AS table_name,
    formatReadableSize(total_bytes) AS size,
    total_rows AS row_count,
    formatDateTime(max_time, '%Y-%m-%d %H:%M:%S') AS last_updated
FROM system.tables 
WHERE database = 'default' 
  AND table NOT LIKE '%_kafka' 
  AND table NOT LIKE '%_mv'
ORDER BY total_rows DESC;

-- 2. Monitor Kafka consumption lag
SELECT 
    table AS kafka_table,
    formatReadableSize(total_bytes) AS consumed_size,
    total_rows AS messages_consumed
FROM system.tables 
WHERE database = 'default' 
  AND table LIKE '%_kafka'
ORDER BY total_rows DESC;

-- 3. Check materialized view performance
SELECT 
    view_name,
    target_table,
    formatReadableSize(bytes_read) AS data_processed,
    rows_read AS rows_processed,
    formatDateTime(event_time, '%Y-%m-%d %H:%M:%S') AS last_activity
FROM system.query_log 
WHERE query_kind = 'Insert' 
  AND query LIKE '%MATERIALIZED VIEW%'
ORDER BY event_time DESC 
LIMIT 10;

-- 4. Real-time CDC latency check
SELECT 
    table_name,
    count(*) AS recent_changes,
    max(created_at) AS latest_change,
    dateDiff('second', max(created_at), now()) AS lag_seconds
FROM (
    -- Replace with your actual table names
    SELECT 'wide_table_100cols' AS table_name, created_at FROM wide_table_100cols WHERE created_at > now() - INTERVAL 1 HOUR
) 
GROUP BY table_name;

-- 5. Check for CDC errors or issues
SELECT 
    event_time,
    query_duration_ms,
    exception,
    query
FROM system.query_log 
WHERE exception != '' 
  AND (query LIKE '%kafka%' OR query LIKE '%_mv%')
ORDER BY event_time DESC 
LIMIT 10;

-- 6. Table partition and part information
SELECT 
    table,
    partition,
    name AS part_name,
    formatReadableSize(bytes_on_disk) AS size_on_disk,
    rows,
    modification_time
FROM system.parts 
WHERE database = 'default' 
  AND active = 1
ORDER BY modification_time DESC 
LIMIT 20;

-- 7. CDC throughput metrics (last 24 hours)
SELECT 
    toStartOfHour(event_time) AS hour,
    count(*) AS queries_executed,
    sum(read_rows) AS total_rows_processed,
    formatReadableSize(sum(read_bytes)) AS total_data_processed
FROM system.query_log 
WHERE event_time > now() - INTERVAL 24 HOUR
  AND query_kind = 'Insert'
  AND query LIKE '%MATERIALIZED VIEW%'
GROUP BY hour 
ORDER BY hour DESC;

-- 8. Current active Kafka consumers
SELECT 
    table,
    engine_full,
    create_table_query
FROM system.tables 
WHERE engine = 'Kafka' 
  AND database = 'default';

-- 9. Memory usage by tables
SELECT 
    table,
    formatReadableSize(sum(bytes)) AS memory_usage,
    sum(rows) AS rows_in_memory
FROM system.parts 
WHERE database = 'default' 
  AND active = 1
GROUP BY table 
ORDER BY sum(bytes) DESC;

-- 10. CDC health check summary
SELECT 
    'Total Tables' AS metric,
    toString(count(*)) AS value
FROM system.tables 
WHERE database = 'default' 
  AND table NOT LIKE '%_kafka' 
  AND table NOT LIKE '%_mv'

UNION ALL

SELECT 
    'Active Kafka Consumers' AS metric,
    toString(count(*)) AS value
FROM system.tables 
WHERE database = 'default' 
  AND engine = 'Kafka'

UNION ALL

SELECT 
    'Materialized Views' AS metric,
    toString(count(*)) AS value
FROM system.tables 
WHERE database = 'default' 
  AND engine = 'MaterializedView'

UNION ALL

SELECT 
    'Total Data Size' AS metric,
    formatReadableSize(sum(total_bytes)) AS value
FROM system.tables 
WHERE database = 'default';
