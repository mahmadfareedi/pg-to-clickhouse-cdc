# Orders → ClickHouse CDC

The repository now ships with everything needed to backfill the `orders` table
from PostgreSQL into ClickHouse and then keep it in sync from the CDC stream.

## Full load
- Bring the stack up with `docker compose up -d`. The
  `debezium-postgresql-connector` definition in
  `connectors/postgres-source.json` includes `public.orders` and has
  `snapshot.mode=initial`, so the connector performs a snapshot of the existing
  rows as soon as it starts.
- If the connector was already running before this change, push the updated
  config and restart it to trigger the snapshot:
  - `curl -X PUT -H 'Content-Type: application/json' --data @connectors/postgres-source.json \`
    `http://localhost:8083/connectors/debezium-postgresql-connector/config`
  - `curl -X POST http://localhost:8083/connectors/debezium-postgresql-connector/restart`
- Monitor the snapshot progress from the Connect logs:
  `docker compose logs -f connect | grep debezium-postgresql-connector`.

## CDC into ClickHouse
- The new sink definition `connectors/clickhouse-orders-sink.json` subscribes to
  `mydb-replication.public.orders` and writes directly into the ClickHouse
  `orders` table via the `jdbc:clickhouse+notx://...` URL so the shim intercepts
  `setAutoCommit(false)`. The connector is registered automatically by the
  `connect-init` container at startup; to register it manually run:
  `curl -X POST -H 'Content-Type: application/json' --data @connectors/clickhouse-orders-sink.json \`
  `http://localhost:8083/connectors`.
- Verify the sink is healthy with `curl
  http://localhost:8083/connectors/clickhouse-orders-sink/status`.
- Query ClickHouse to confirm both the snapshot rows and subsequent CDC events
  landed: `docker exec -it clickhouse clickhouse-client --query "SELECT * FROM
  orders ORDER BY created_at DESC LIMIT 5"` (adjust projections/columns per
  your schema).

## Troubleshooting tips
- Inspect the Kafka topic directly if ClickHouse stays empty:
  `docker exec -it kafka rpk topic consume mydb-replication.public.orders -n 5`.
- Ensure the ClickHouse table schema matches the fields Debezium emits. Update
  `create_tables.sql` or run custom DDL before starting the sink when new
  columns are added.
- Existing sink connectors created before the shim change need to be updated so
  their `connection.url` also uses `jdbc:clickhouse+notx://...`.
- If the ClickHouse sink errors with `setAutoCommit(false) is not supported`,
  rebuild the Connect image to pick up the new autocommit shim and redeploy the
  connectors: `docker compose build connect && docker compose up -d connect`,
  then `curl -X PUT ...connection.url=jdbc:clickhouse+notx://...` to refresh the
  sink configs.
