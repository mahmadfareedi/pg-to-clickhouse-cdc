pg-to-clickhouse-cdc

What’s included
- Kafka-compatible broker powered by Redpanda (no ZooKeeper, Apple Silicon friendly)
- Kafka Connect with Debezium Postgres source + JDBC sink + ClickHouse JDBC driver
- Kafka UI (manage topics and Kafka Connect, including sink connectors)
- Redpanda Console (modern UI with full Kafka Connect management)
- ClickHouse server

Quickstart
- Start the stack: `docker compose up -d --build`
- UIs (Basic Auth: user `admin`, pass `admin`)
  - Kafka UI: http://localhost:8080
  - Redpanda Console: http://localhost:8081
  - ClickHouse Admin UI (Tabix): http://localhost:8082
- Kafka Connect REST: http://localhost:8083
- ClickHouse HTTP: http://localhost:8123 (user `clickhouse`, password `clickhouse`)
  - Use the credentials configured in `docker-compose.yml` under the `clickhouse` service.

Source DB (Postgres on EC2)
- Requirements on your Postgres
  - Enable logical replication (postgresql.conf: `wal_level=logical`, and allow sufficient `max_wal_senders`/`max_replication_slots`).
  - Network: open inbound 5432 from your Docker host. If using SSL, collect CA/client certs.
  - User must be allowed to create a publication and replication slot (superuser or proper privileges).
- Create the Debezium source connector (no JSON)
  - Kafka UI: Connect → New Connector → pick `io.debezium.connector.postgresql.PostgresConnector` and fill:
    - `database.hostname`: your EC2 host/IP
    - `database.port`: 5432
    - `database.user` / `database.password`
    - `database.dbname`
    - `plugin.name`: pgoutput
    - `topic.prefix`: pgsrc (or any prefix you choose)
    - `publication.autocreate.mode`: all_tables
    - `snapshot.mode`: initial
  - Redpanda Console: Connect → Kafka Connect → New Connector → same fields as above.
- Or via REST (JSON)
  - Copy `connectors/postgres-source.example.json` → `connectors/postgres-source.json` and set your values.
  - POST: `curl -s -X POST -H 'Content-Type: application/json' --data @connectors/postgres-source.json http://localhost:8083/connectors`

Target (ClickHouse sink via JDBC)
- Kafka UI or Redpanda Console: Connect → New Connector → pick `io.confluent.connect.jdbc.JdbcSinkConnector` and set:
  - `connection.url`: `jdbc:clickhouse://clickhouse:8123/default`
  - `connection.user`: `clickhouse`
  - `connection.password`: `clickhouse`
  - `auto.create`: `true`
  - `transforms`: `unwrap`
  - `transforms.unwrap.type`: `io.debezium.transforms.ExtractNewRecordState`
  - `transforms.unwrap.drop.tombstones`: `true`
  - `transforms.unwrap.delete.handling.mode`: `none`
  - Choose one:
    - `topics`: `pgsrc.public.your_table`
    - or `topics.regex`: `pgsrc.public.*` (dev: sink all tables)
- Or via REST: `curl -s -X POST -H 'Content-Type: application/json' --data @connectors/clickhouse-sink.json http://localhost:8083/connectors`
  - Edit the `topics` or `topics.regex` field first.

ClickHouse admin UI (Tabix)
- Open http://localhost:8082 (Basic Auth)
- Add a new server connection pointing to `http://clickhouse:8123` (HTTP interface)
  - User / Password: the values you configured in `docker-compose.yml` (see `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`)
- Use the SQL editor to browse schemas and run queries (SELECT, DESCRIBE, etc.).

Start CDC and verify
- After both connectors show RUNNING, Debezium will stream changes to topics like `pgsrc.public.<table>`.
- Test quickly:
  1) Insert in Postgres (on EC2): `INSERT INTO public.your_table(id,name) VALUES (1,'hello');`
  2) Query ClickHouse: `SELECT * FROM your_table;` (auto-created if `auto.create=true`).

Notes and tips
- The JDBC sink here uses `insert.mode=insert` with Debezium’s `ExtractNewRecordState` transform to flatten the CDC envelope. This is append-only. For true upserts in ClickHouse, consider a ReplacingMergeTree table design and/or a dedicated ClickHouse sink connector.
- For an external Postgres, ensure logical replication is enabled and port 5432 is reachable from your Docker host.
- Basic Auth creds live in `nginx/htpasswd` (format: `user:hash`). Change with: `openssl passwd -apr1 'NEW_PASSWORD'` and replace the hash.
- This setup is for local/dev use. Add volumes, authentication, and proper replication factors before production.

Automation (optional)
- The `connect-init` job auto-POSTs any `connectors/*.json` on startup, skipping `*.example.json`. Keep your finalized configs as `.json` in that folder to auto-create them when the stack starts.
