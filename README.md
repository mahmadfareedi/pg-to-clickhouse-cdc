pg-to-clickhouse-cdc

What’s included
- Kafka-compatible broker powered by Redpanda (no ZooKeeper, Apple Silicon friendly)
- Kafka Connect with Debezium Postgres source + JDBC sink + ClickHouse JDBC driver
- Debezium UI (build and manage source connectors)
- Debezium UI (runs as amd64 via Docker emulation on Apple Silicon)
- Kafka UI (manage topics and Kafka Connect, including sink connectors)
- ClickHouse server

Quickstart
- Start the stack: `docker compose up -d --build`
- Debezium UI: http://localhost:8080 (create the Postgres source connector)
- Kafka UI: http://localhost:8081 (manage topics and connectors)
- Kafka Connect REST: http://localhost:8083
- ClickHouse HTTP: http://localhost:8123 (user `clickhouse`, password `clickhouse`)

Create connectors
1) Debezium Postgres source (external Postgres on EC2)
- In Debezium UI, point to Connect at `http://connect:8083` and create a Postgres connector pointing to your EC2 Postgres host.
- Or copy `connectors/postgres-source.example.json`, fill `database.hostname`, `database.user`, `database.password`, `database.dbname`, then POST it to `http://localhost:8083/connectors`.
- Ensure your Postgres enables logical replication (wal_level=logical, suitable replication slots/publication). Open port 5432 from your Docker host to the EC2 instance and enable SSL if required.

2) ClickHouse sink (via JDBC Sink Connector)
- In Kafka UI, open the Connect tab (cluster: `local`) and create a new sink connector.
- Or POST this JSON to `http://localhost:8083/connectors`:
  - File: `connectors/clickhouse-sink.json`
- Important: set the `topics` value to the Debezium topic you want to sink. Debezium topic naming is `<serverName>.<schema>.<table>`. With the provided source example, a table `public.your_table` would produce `pgsrc.public.your_table`.

Notes and tips
- The JDBC sink here uses `insert.mode=insert` with Debezium’s `ExtractNewRecordState` transform to flatten the CDC envelope. This is append-only. For true upserts in ClickHouse, consider a ReplacingMergeTree table design and/or a dedicated ClickHouse sink connector.
- For a non-demo Postgres, change the Postgres service or point the Debezium connector at your external database.
- This setup is for local/dev use. Add volumes, authentication, and proper replication factors before production.
