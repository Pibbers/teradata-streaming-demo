# Demo 7 — Kafka Connect JDBC Sink → Teradata

## What it demonstrates

A Kafka Connect **JDBC Sink connector** (Confluent Community License) inserts rows from a Kafka topic directly into a Teradata table using standard JDBC. No TPT, no NOS, no Flink — just a connector JAR and the Teradata JDBC driver from Maven Central.

This completes the "three ways to get data into Teradata" story:

| Demo | Method | Latency | Use case |
|------|--------|---------|----------|
| 2 | TPT STREAM + Kafka Access Module | Seconds | High-throughput native Teradata path |
| 4/5 | Flink → Iceberg → OTF | Minutes | Stream transformations, open table format |
| **7** | **Kafka Connect JDBC Sink** | **Seconds** | **Simple, licence-free, connector-native** |

## Architecture

```
adsb_producer.py          Kafka                  Kafka Connect           Teradata
  (--format json)    →  adsb-positions-json  →  JDBC Sink connector  →  adsb_positions
```

Key points:
- The producer encodes each ADS-B position as **Kafka Connect JSON** (`--format connect-json`):
  a `{"schema": {...}, "payload": {...}}` envelope so the JDBC Sink receives a typed Struct
  rather than a schemaless HashMap.  No Schema Registry is needed.
- The connector uses `GenericDatabaseDialect` — standard ANSI `INSERT INTO ... VALUES (?)`.
- Credentials are never hard-coded in the connector JSON; they are read at runtime from a
  bind-mounted properties file via Kafka Connect's built-in **FileConfigProvider**.
- `TMODE=ANSI` in the JDBC URL is required: Teradata defaults to TERA mode, which uses
  session-level transaction semantics incompatible with JDBC Sink's per-statement error handling.

## Connector design decisions

| Setting | Value | Reason |
|---------|-------|--------|
| `connector.class` | `io.confluent.connect.jdbc.JdbcSinkConnector` | Confluent Community Licence — free, no registration |
| `insert.mode` | `insert` | `upsert` requires a custom Teradata dialect (not available in `kafka-connect-jdbc`) |
| `auto.create` | `false` | Table DDL uses PPI (`PARTITION BY RANGE_N`) which `auto.create` cannot replicate |
| `auto.evolve` | `false` | Schema evolution is handled in code, not at the connector layer |
| `value.converter` | `JsonConverter` with `schemas.enable=true` | Producer sends `{"schema":{...},"payload":{...}}` envelope; connector scope only — Demo 03 S3 Sink is unaffected |
| `fields.whitelist` | All columns except `ingest_ts` | `ingest_ts` has `DEFAULT CURRENT_TIMESTAMP(0)` server-side; it must be absent from the INSERT |
| `TMODE=ANSI` | In JDBC URL | Required for per-statement error semantics |
| `consumer.override.auto.offset.reset` | `earliest` | Ensures replay from offset 0 if the connector starts after messages are published |

### Teradata JDBC driver

`terajdbc-20.00.00.58.jar` is downloaded from Maven Central during the Docker image build:

```
https://repo1.maven.org/maven2/com/teradata/jdbc/terajdbc/20.00.00.58/terajdbc-20.00.00.58.jar
```

It is placed in `/usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/`
so it shares the connector's classloader. If you see `No suitable driver found for
jdbc:teradata://`, the JAR is on the wrong classloader — see Troubleshooting below.

### Credential handling

`kafka/connect/td-credentials.properties` is **gitignored** and written at demo runtime by
`run.sh` from the values in `.env`. The file is bind-mounted into the container at
`/etc/kafka/connect-configs/`. The connector config references it via Kafka Connect's
`FileConfigProvider`:

```json
"connection.url": "jdbc:teradata://${file:/etc/kafka/connect-configs/td-credentials.properties:td_host}/..."
```

The `FileConfigProvider` is enabled by two environment variables on the `kafka-connect` service
in `docker-compose.yml`:

```yaml
CONNECT_CONFIG_PROVIDERS: file
CONNECT_CONFIG_PROVIDERS_FILE_CLASS: org.apache.kafka.common.config.provider.FileConfigProvider
```

## Prerequisites

```bash
# Stack must include kafka, kafka-connect, schema-registry, tpt
docker compose up -d

# One-time Teradata setup (creates adsb_positions table)
bash tpt/scripts/run_setup.sh

# Python producer dependency
pip install confluent-kafka
```

The `kafka-connect` image must be rebuilt after any Dockerfile change:

```bash
docker build -t td-demo-kafka-connect:latest kafka-connect/
```

## Running the demo

```bash
# Bounded: 200 Connect-JSON messages, then verify row count
bash demos/07-kafka-connect-td/run.sh

# Continuous: produce until Ctrl+C, then print final count
bash demos/07-kafka-connect-td/run.sh --continuous
```

The script:
1. Writes `kafka/connect/td-credentials.properties` from `.env`
2. Deletes any existing `demo07-td-jdbc-sink` connector
3. Drops and recreates the `adsb-positions-json` Kafka topic
4. Clears `adsb_positions` (DELETE ALL)
5. Registers the connector via the Connect REST API
6. Waits for connector + task to reach `RUNNING`
7. Starts `adsb_producer.py --format json`
8. Polls Teradata until all expected rows arrive (bounded) or waits for Ctrl+C (continuous)
9. Prints final row count and per-aircraft breakdown

## Expected output (bounded)

```
── 3/4  Registering JDBC Sink connector ──────────────────────
      Config: kafka/connect/td-jdbc-sink.json
      Credentials: FileConfigProvider → /etc/kafka/connect-configs/td-credentials.properties
      JDBC URL: jdbc:teradata://172.28.x.x/DATABASE=demo_db,TMODE=ANSI
      Waiting for connector+task to start........... RUNNING

── 4/4  Publishing ADS-B positions as JSON ───────────────────
      Publishing 200 messages (10 aircraft × 20 cycles, 1s interval)...
[2026-06-25T...] 50 ADS-B messages sent to adsb-positions-json
...
      Producer done. Waiting for JDBC Sink to flush to Teradata...
      [3s] Rows in adsb_positions: 200 / 200

  positions_received  earliest_ts  latest_ts  aircraft_seen
  ------------------  -----------  ---------  -------------
                 200  2026-06-25…  2026-06-25…           10
```

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `No suitable driver found for jdbc:teradata://` | Teradata JAR on wrong classloader | Move JAR to its own plugin dir and add to `CONNECT_PLUGIN_PATH` |
| `Authentication failed` | Wrong credentials | Check `.env` values; re-run `run.sh` to rewrite credentials file |
| `Table 'adsb_positions' not found` | Setup not run | `bash tpt/scripts/run_setup.sh` |
| `TASK FAILED` immediately | FileConfigProvider not enabled | Ensure `CONNECT_CONFIG_PROVIDERS=file` env var is set and container restarted |
| `Unknown schema type: float64` | Wrong Connect JSON type name | Use `"double"` (not `"float64"`) in the schema envelope; JsonConverter maps `"double"` → FLOAT64 |
| Connector stuck in `UNASSIGNED` | Connect cluster still starting | Wait 30 s and retry; check `docker compose logs kafka-connect` |

```bash
# Inspect connector status
curl -s http://localhost:8083/connectors/demo07-td-jdbc-sink/status | python3 -m json.tool

# Live connector logs
docker compose logs -f kafka-connect
```

## Known limitations

- **No upsert**: `kafka-connect-jdbc` has no `TeradataDialect`, so only `insert.mode=insert`
  is supported without writing a custom dialect.
- **Type coercion**: With `schemas.enable=false`, the connector submits all values as strings.
  Teradata's JDBC driver accepts this for DATE and TIMESTAMP columns via `TMODE=ANSI` implicit
  casting, but exotic types may need schema-enabled mode or a custom converter.
- **No DLQ**: Failed rows are retried and then the task fails. A dead-letter queue pattern
  would require additional configuration not covered by this demo.
