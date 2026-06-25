# Demo 6: Kafka Avro → Flink → TPT STREAM

**Topic:** Avro messages published with Confluent wire format (schema registered in Schema Registry) are deserialised by Flink with full type awareness, transformed, and re-emitted as pipe-delimited text to an intermediate Kafka topic. TPT STREAM then ingests that topic into Teradata in real time. Shows where Flink adds value *before* Teradata ingestion, rather than replacing it.

## What it demonstrates

- Producer publishing Confluent wire-format Avro: `\x00` + 4-byte schema ID + schemaless Avro payload, with the schema auto-registered in Schema Registry on first run
- Flink `avro-confluent` format fetching the writer schema from Schema Registry per message (schema ID embedded in each record) — no manual schema configuration in Flink SQL
- Avro-to-SQL type mapping: `boolean` → `BOOLEAN`, `long/timestamp-millis` → `BIGINT` (epoch ms), `double` → `DOUBLE`
- Type transformations that are impossible for TPT's text-only `DELIMITED` format: `BOOLEAN` → `"0"`/`"1"` string, `BIGINT` epoch ms → formatted timestamp string via `FROM_UNIXTIME` + `MOD`, `pos_date` derived from the same epoch value
- Flink Kafka-to-Kafka: streaming job with no stateful sink (no checkpointing required)
- TPT STREAM consuming the Flink output topic identically to Demo 2 — same tbuild pattern, same `-l 5` latency flush, same graceful drain on Ctrl+C

## How to run

```bash
# Rebuild the Flink image (required once — adds flink-sql-avro-confluent-registry JAR)
docker compose build flink-jobmanager flink-taskmanager

# Start the full stack
docker compose up -d

# Continuous streaming (default); press Ctrl+C to stop
bash demos/06-flink-avro-tpt/run.sh

# Bounded mode — 100 messages; exits automatically
bash demos/06-flink-avro-tpt/run.sh --bounded
```

## Files

| File | Purpose |
|------|---------|
| [kafka/producers/adsb_producer.py](../kafka/producers/adsb_producer.py) | Synthetic ADS-B producer; `--format sr-avro` registers schema + publishes Confluent wire Avro; `--registry` sets Schema Registry URL |
| [kafka/schemas/adsb_position.avsc](../kafka/schemas/adsb_position.avsc) | Avro schema — registered automatically on first produce |
| [flink/jobs/demo06/stream.sql](../flink/jobs/demo06/stream.sql) | Flink SQL: `avro-confluent` source → `raw`-format pipe-delimited Kafka sink |
| [flink/jobs/demo06/batch.sql](../flink/jobs/demo06/batch.sql) | Same as stream.sql with `earliest-offset` for bounded mode |
| [tpt/tbuild/kafka_stream_06.tbuild](../tpt/tbuild/kafka_stream_06.tbuild) | TPT job: DataConnector (Kafka Access Module) → STREAM operator → `adsb_positions_06` |
| [tpt/scripts/demo06/prepare.bteq](../tpt/scripts/demo06/prepare.bteq) | Creates `adsb_positions_06` if needed; clears table and LT/ET work tables |
| [tpt/scripts/demo06/status.bteq](../tpt/scripts/demo06/status.bteq) | Single-line heartbeat: `STATUS rows=N latest=TIMESTAMP` |
| [tpt/scripts/demo06/verify.bteq](../tpt/scripts/demo06/verify.bteq) | Post-run row count and per-aircraft summary |
| [demos/06-flink-avro-tpt/run.sh](../demos/06-flink-avro-tpt/run.sh) | Orchestration — dual-mode |

## What to expect

```
======================================================
  Demo 6: Kafka Avro → Flink → TPT STREAM (ADS-B)
  Avro topic:               adsb-avro
  Flink output topic:       adsb-positions-flink
  Schema Registry (host):   http://localhost:8082
  Mode:                     CONTINUOUS (Ctrl+C to stop)
======================================================

── 1/4  Resetting prior state
── 2/4  Starting Flink streaming job
[Schema Registry] Registered schema ID 1 for adsb-avro-value
      Flink job ID: a1b2c3d4e5f6...

── 3/4  Starting TPT STREAM and producer

  [12:01:15 UTC]  STATUS rows=80 latest=2026-06-25 12:01:10.443
  [12:01:25 UTC]  STATUS rows=180 latest=2026-06-25 12:01:20.512
  ...

^C
── Stopping producer...
── Waiting for TPT to drain (up to 35s after Flink goes quiet)...
── Cancelling Flink job a1b2c3...

── 4/4  Final row count
  positions_received  earliest_ts              latest_ts              aircraft_seen
  ------------------  -------------------      -------------------    -------------
                 630  2026-06-25 12:01:05.210  2026-06-25 12:02:08    10
```

## How it works

```
1. BTEQ creates adsb_positions_06 (if absent) and clears prior data + LT/ET work tables
   Kafka topics adsb-avro and adsb-positions-flink deleted and recreated

2. Flink streaming job submitted (stream.sql)
   └─ Source: adsb-avro, format=avro-confluent, SR at http://schema-registry:8081
      Each message: \x00 + 4-byte schema ID → Flink fetches schema from SR → typed record
   └─ INSERT: boolean on_ground → CAST(CASE WHEN ... THEN 1 ELSE 0 END AS STRING)
              BIGINT ts (epoch ms) → FROM_UNIXTIME(ts/1000, ...) + LPAD(MOD(ts,1000)) for ts and pos_date
   └─ Sink: adsb-positions-flink, format=raw — full pipe-delimited line built in SQL with || + CHR(10)
      No checkpointing — Kafka-to-Kafka append requires no stateful commit protocol

3. TPT STREAM starts (background) consuming adsb-positions-flink with -l 5
   Same pattern as Demo 2: DataConnector + Kafka Access Module → STREAM operator

4. Producer starts (background) publishing to adsb-avro, 1s interval, 10 aircraft

5. On Ctrl+C:
   a. Producer killed → no more messages to adsb-avro
   b. Flink sees empty input → stops writing to adsb-positions-flink
   c. TPT idle timeout (30s) fires → STREAM flushes and exits
   d. Flink job cancelled via REST API
   e. Final verify query run
```

## Avro schema and Schema Registry

The Avro schema ([kafka/schemas/adsb_position.avsc](../kafka/schemas/adsb_position.avsc)) is registered automatically by the producer on first publish using the Schema Registry REST API. The schema is stored under the subject `adsb-avro-value`.

Flink's `avro-confluent` format fetches the writer schema by schema ID on each message — old and new schema versions can coexist in the topic. However, adding a new field to the Avro schema also requires updating:

1. The Flink SQL source table columns in `stream.sql`
2. The Flink INSERT column list
3. The pipe-delimited sink table in `stream.sql`
4. `ADSB_SCHEMA` in `kafka_stream_06.tbuild`
5. `ALTER TABLE adsb_positions_06 ADD ...` in Teradata

Schema Registry makes that migration *safe* (no message corruption); it does not make it *automatic*.

## Key technical constraints

| Constraint | Detail |
|---|---|
| Flink image rebuild required | `flink-sql-avro-confluent-registry-1.20.1.jar` must be in `/opt/flink/lib/` — added to `flink/Dockerfile`, requires `docker compose build` |
| Schema Registry URL inside vs outside Docker | Producer uses `http://localhost:${SCHEMA_REGISTRY_PORT}` (host network); Flink SQL uses `http://schema-registry:8081` (Docker network) |
| `avro-confluent` format, not `avro` | `avro` format expects raw schemaless Avro; `avro-confluent` expects the 5-byte Confluent wire prefix — mismatch causes silent decode failure |
| `ts` declared as `BIGINT`, not `TIMESTAMP_LTZ(3)` | `AvroSchemaConverter.convertToSchema()` in `flink-sql-avro-confluent-registry` 1.20.1 throws `UnsupportedOperationException` for `TIMESTAMP_LTZ`. Declaring `BIGINT` reads the raw epoch-ms long directly; `FROM_UNIXTIME` + `MOD` format the value. `TIMESTAMP(3)` would also avoid the error but introduces implicit timezone handling. |
| Flink sink uses `raw` format, not `csv` | Flink's CSV format quotes all STRING fields by default (e.g., `"4ca87a"`) and `csv.disable-quote-character` is silently ignored in Flink 1.20.1. The `raw` format writes a single STRING column as unquoted UTF-8 bytes. The full pipe-delimited line is assembled in SQL with `\|\|` concatenation + `CHR(10)` newline terminator. |
| No checkpointing in stream.sql | Kafka→Kafka append does not require checkpointing; adding it would delay the first visible rows |
| TPT target is `adsb_positions_06` | Named separately from Demo 2's `adsb_positions` so both demos are fully independent |

## Table

```sql
CREATE MULTISET TABLE adsb_positions_06 (
  icao24        CHAR(6)       NOT NULL,
  callsign      VARCHAR(8),
  pos_date      DATE          NOT NULL,
  latitude      DECIMAL(9,6)  NOT NULL,
  longitude     DECIMAL(10,6) NOT NULL,
  altitude      INTEGER,
  velocity      DECIMAL(7,2),
  heading       DECIMAL(6,2),
  vertical_rate INTEGER,
  on_ground     BYTEINT       DEFAULT 0,
  squawk        CHAR(4),
  ts            TIMESTAMP(3)  NOT NULL,
  ingest_ts     TIMESTAMP(0)  DEFAULT CURRENT_TIMESTAMP(0)
)
PRIMARY INDEX (icao24)
PARTITION BY RANGE_N(pos_date BETWEEN DATE '2024-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY);
```

## Sample queries

```sql
-- Recent position fixes
SELECT icao24, callsign, latitude, longitude, altitude, ts
FROM demo_db.adsb_positions_06
WHERE pos_date = CURRENT_DATE
ORDER BY ts DESC;

-- Ingest latency (producer → Teradata, via Flink + TPT)
SELECT icao24,
       AVG( (CAST(ingest_ts AS TIMESTAMP(3)) - ts) SECOND(4) ) AS avg_lag_secs
FROM demo_db.adsb_positions_06
GROUP BY 1
ORDER BY 1;
```
