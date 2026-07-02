# Demo 4: Kafka → Flink → Iceberg → Teradata OTF

**Topic:** Flink SQL continuously streams ADS-B messages from Kafka into an Apache Iceberg table (Parquet on MinIO, metadata in Hive Metastore). Teradata queries the live Iceberg table via a **DATALAKE** object (OTF) using 3-part dot notation — no ETL, no COPY, no TPT. New rows become visible within ~30 seconds of ingestion.

## What it demonstrates

- Flink SQL checkpointing as the Iceberg commit mechanism: each 30-second checkpoint atomically commits a new Iceberg snapshot, which Teradata reads on the very next SELECT
- The **DATALAKE** object — Teradata's OTF interface to an Iceberg catalog; connects to Hive Metastore + MinIO with a single DDL object; tables queried via `<datalake>.<namespace>.<table>` 3-part notation
- `TD_SNAPSHOTS()` table function: lists committed Iceberg snapshots — demonstrates the growing snapshot history as streaming continues
- Open-lakehouse pattern: Flink writes Parquet data files; Teradata, Spark, Trino can all query the same files simultaneously via the Iceberg catalog
- Iceberg partitioning by `pos_date` (daily) — enables partition pruning in Teradata OTF queries

## How to run

```bash
# Continuous streaming (default); press Ctrl+C to stop
bash demos/04-kafka-flink-iceberg/run.sh

# Bounded mode — 200 messages; exits automatically
bash demos/04-kafka-flink-iceberg/run.sh --bounded
```

## Files

| File | Purpose |
|------|---------|
| [kafka/producers/adsb_producer.py](../kafka/producers/adsb_producer.py) | ADS-B producer — `--continuous` for streaming, `--count 200` for bounded |
| [flink/jobs/demo04/stream.sql](../flink/jobs/demo04/stream.sql) | Flink SQL: register Hive catalog, create Iceberg table, streaming INSERT (checkpoint 30s) |
| [flink/jobs/demo04/batch.sql](../flink/jobs/demo04/batch.sql) | Flink SQL: same table + batch INSERT (earliest→latest-offset) |
| [flink/jobs/demo04/drop.sql](../flink/jobs/demo04/drop.sql) | Reset: DROP TABLE + DROP DATABASE |
| [tpt/scripts/demo04/datalake_create.bteq](../tpt/scripts/demo04/datalake_create.bteq) | CREATE DATALAKE connecting Teradata to HMS + MinIO |
| [tpt/scripts/demo04/datalake_drop.bteq](../tpt/scripts/demo04/datalake_drop.bteq) | DROP DATALAKE |
| [tpt/scripts/demo04/otf_query.bteq](../tpt/scripts/demo04/otf_query.bteq) | Heartbeat: `STATUS rows=N latest=TIMESTAMP` |
| [tpt/scripts/demo04/otf_verify.bteq](../tpt/scripts/demo04/otf_verify.bteq) | Final: HELP TABLE, row count, per-aircraft, TD_SNAPSHOTS() |
| [demos/04-kafka-flink-iceberg/run.sh](../demos/04-kafka-flink-iceberg/run.sh) | Orchestration — dual-mode |

## How it works

```
1. Reset
   └─ Flink SQL client drops Iceberg table (removes Parquet + metadata from MinIO)
   └─ DATALAKE dropped from Teradata
   └─ Kafka topic deleted and recreated (clean offset)

2. DATALAKE setup (one-time per session)
   └─ CREATE DATALAKE demo_iceberg: CATALOG_TYPE='hive', HMS at HOST_IP:9083
   └─ STORAGE_ENDPOINT=http://HOST_IP:9000 (MinIO, path-style, no SSL)

3. Flink streaming INSERT (continuous mode)
   └─ Creates hive_catalog (Iceberg Flink connector → Hive Metastore)
   └─ INSERT: CASTs + TO_TIMESTAMP() → Iceberg Parquet files on MinIO
   └─ Checkpoint every 30s (EXACTLY_ONCE) → atomic Iceberg snapshot per checkpoint
   └─ SQL client exits after submitting; job continues running in Flink cluster

4. Monitoring loop
   └─ Every 30s: SELECT COUNT(*) + MAX(ts) from demo_iceberg.demo.adsb_positions

5. Graceful shutdown (Ctrl+C)
   └─ Producer killed → 35s wait → final checkpoint commits remaining rows
   └─ Flink job cancelled via REST (PATCH /jobs/{id}?mode=cancel)
```

## Key OTF design rules

| Rule | Detail |
|---|---|
| DATALAKE, not FOREIGN TABLE | Iceberg tables are queried via `CREATE DATALAKE` with `CATALOG_TYPE('hive')` |
| `HOST_IP` in CATALOG_LOCATION and STORAGE_ENDPOINT | Teradata is external to Docker; use `HOST_IP` from `.env`, not container hostnames |
| Auth objects must live in `TD_SERVER_DB` | DATALAKE objects are stored in `TD_SERVER_DB`, so `EXTERNAL SECURITY DEFINER TRUSTED` (not `INVOKER TRUSTED`) must be used, and the auth objects must be created in `TD_SERVER_DB` before the `CREATE DATALAKE` — `INVOKER TRUSTED` always resolves against the connecting user's home DB (DBC for user dbc) and fails with error 6938 |
| `S3_PATH_STYLE_ACCESS('true')` required for MinIO | MinIO uses path-style S3 URLs |
| `S3_SSL_ENABLED('false')` required | MinIO in this stack runs HTTP |
| Namespace created by Flink, not Teradata | `CREATE DATABASE via DATALAKE` from Teradata → error 7825; Flink's `CREATE DATABASE IF NOT EXISTS` handles this |
| Rows visible at checkpoint boundaries | Queries between checkpoints return the previous snapshot's data |

## DATALAKE DDL reference

```sql
DATABASE demo_db;

CREATE DATALAKE demo_iceberg
  EXTERNAL SECURITY DEFINER TRUSTED CATALOG hms_catalog_auth,
  EXTERNAL SECURITY DEFINER TRUSTED STORAGE minio_storage_auth
USING
  CATALOG_TYPE('hive')
  CATALOG_LOCATION('thrift://<HOST_IP>:9083')
  STORAGE_LOCATION('s3://iceberg/warehouse/')
  STORAGE_ENDPOINT('http://<HOST_IP>:9000')
  S3_PATH_STYLE_ACCESS('true')
  STORAGE_REGION('us-east-1')
  S3_SSL_ENABLED('false')
  S3_MAX_TASK('1000')
  S3_MAX_THREADS('1000')
  S3_MAX_CONNECTIONS('5000')
TABLE FORMAT iceberg;
```

`hms_catalog_auth` and `minio_storage_auth` must be created as `DEFINER TRUSTED` in `TD_SERVER_DB` (the DATALAKE's own database) before this statement — see [tpt/scripts/demo04/datalake_create.bteq](../tpt/scripts/demo04/datalake_create.bteq).

## Sample queries

```sql
-- Explore the catalog
HELP DATALAKE demo_iceberg;
HELP TABLE demo_iceberg.demo.adsb_positions;

-- Current snapshot (latest committed data)
SELECT icao24, callsign, latitude, longitude, altitude, ts
FROM demo_iceberg.demo.adsb_positions
WHERE pos_date = '2026-06-22'
ORDER BY ts DESC
SAMPLE 20;

-- Snapshot history (one row per Flink checkpoint that committed data)
SELECT * FROM TD_SNAPSHOTS(ON demo_iceberg.demo.adsb_positions) AS snap;

-- Time travel
SELECT COUNT(*) AS rows_at_snapshot
FROM demo_iceberg.demo.adsb_positions
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-22 12:01:00';
```
