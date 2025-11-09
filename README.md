pg-to-clickhouse-cdc

What’s included
- Kafka-compatible broker powered by Redpanda (no ZooKeeper, Apple Silicon friendly)
- Kafka Connect with Debezium Postgres source + JDBC sink + ClickHouse JDBC driver
- Kafka UI (manage topics and Kafka Connect, including sink connectors), exposed via an auth proxy
- ClickHouse server

Quickstart
- Start the stack: `docker compose up -d --build`
- Kafka UI (with Basic Auth): http://localhost:8080 or http://localhost:8081
- Kafka Connect REST: http://localhost:8083
- ClickHouse HTTP: http://localhost:8123 (user `clickhouse`, password `clickhouse`)

Create connectors
- Debezium Postgres source (external Postgres on EC2)
- In Kafka UI, use Connect → New Connector → `io.debezium.connector.postgresql.PostgresConnector`, or copy `connectors/postgres-source.example.json`, fill `database.hostname`, `database.user`, `database.password`, `database.dbname`, then POST it to `http://localhost:8083/connectors`.
- Ensure your Postgres enables logical replication (wal_level=logical, suitable replication slots/publication). Open port 5432 from your Docker host to the EC2 instance and enable SSL if required.

2) ClickHouse sink (via JDBC Sink Connector)
- In Kafka UI, open the Connect tab (cluster: `local`) and create a new sink connector.
- Or POST this JSON to `http://localhost:8083/connectors`:
  - File: `connectors/clickhouse-sink.json`
- Important: set the `topics` value to the Debezium topic you want to sink. Debezium topic naming is `<serverName>.<schema>.<table>`. With the provided source example, a table `public.your_table` would produce `pgsrc.public.your_table`.

Notes and tips
- The JDBC sink here uses `insert.mode=insert` with Debezium’s `ExtractNewRecordState` transform to flatten the CDC envelope. This is append-only. For true upserts in ClickHouse, consider a ReplacingMergeTree table design and/or a dedicated ClickHouse sink connector.
- For an external Postgres, ensure logical replication is enabled and port 5432 is reachable from your Docker host.
- Basic Auth creds live in `nginx/htpasswd` (format: `user:hash`).
- This setup is for local/dev use. Add volumes, authentication, and proper replication factors before production.
