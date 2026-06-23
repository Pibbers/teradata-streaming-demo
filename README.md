# Teradata Streaming Demo

A hands-on demonstration of Teradata's real-time and near-real-time data ingestion capabilities using Apache Kafka, MinIO, Apache Flink, and Teradata Parallel Transporter (TPT).

Each demo is self-contained and runnable with a single `bash demos/<n>-<name>/run.sh` command.

---

## Demos

| # | Name | Ingestion method | Key feature shown |
|---|------|-----------------|-------------------|
| 1 | [avro-blob](#demo-1-avro-blob) | BTEQ + AvroContainerSplit | Avro container files → `DATASET AVRO` column; schema evolution |
| 2 | kafka-tpt-stream | TPT STREAM + Kafka Access Module | Continuous near-real-time INSERT from Kafka |
| 3 | kafka-csv-minio | Kafka Connect S3 Sink + NOS | Object store → Teradata Native Object Store foreign table |
| 4 | kafka-flink-iceberg | Flink → Iceberg → OTF | Open Table Format query from Teradata |
| 5 | flink-td-enrich | Flink Lookup Join → Teradata | In-stream enrichment from Teradata reference table |

---

## Architecture

```
                           ┌─────────────────────────────────────────┐
                           │          Docker Compose (demo-net)       │
                           │                                          │
  Python producers ──────► │  Kafka (KRaft)                           │
                           │    │                                     │
                           │    ├─► TPT container ──────────────────► │──► Teradata (external)
                           │    │    (BTEQ / tbuild)                  │
                           │    │                                     │
                           │    ├─► Kafka Connect (S3 Sink) ────────► │──► MinIO
                           │    │                                     │      │
                           │    └─► Flink (JobManager/TaskManager)    │      └─► NOS / OTF ──► Teradata
                           │                                          │
                           │  MinIO (S3-compatible object store)      │
                           │  Hive Metastore + MySQL (Iceberg catalog)│
                           └─────────────────────────────────────────┘
```

Teradata runs externally — on-premises, VantageCloud, or a VM reachable from the Docker host.

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Docker Desktop / Docker Engine 24+ | With Compose v2 (`docker compose`) |
| Python 3.9+ | `pip install confluent-kafka fastavro` |
| Teradata instance | Vantage 20.x recommended; `dbc` user with CREATE DATABASE rights |
| `teradata/tpt:latest` Docker image | Teradata Parallel Transporter container (requires Teradata Developer registration) |

---

## Environment Setup

### 1. Clone and configure

```bash
git clone <repo-url>
cd teradata-streaming-demo
cp .env.example .env
```

Edit `.env` and fill in:

```env
TD_HOST=<your-teradata-ip>
TD_USER=dbc
TD_PASSWORD=<your-dbc-password>
TD_DATABASE=demo_db
HOST_IP=<your-docker-host-ip>          # run: ip addr | grep '192.168'
KAFKA_CLUSTER_ID=<uuid>                # run: docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid
```

The remaining defaults (`MINIO_*`, `HIVE_*`, `MYSQL_*`) work out of the box.

### 2. Build custom images

Two services use custom Dockerfiles that must be built before first use:

```bash
# Kafka Connect with Confluent S3 Sink connector pre-installed
docker build -t td-demo-kafka-connect:latest kafka-connect/

# Flink with Iceberg + Hive catalog JARs
docker build -t flink-demo:latest flink/
```

> The `hive-metastore` service also needs a custom image built separately — see `hive-metastore/` if present, or pull `apache/hive:4.0.0`.

### 3. Start the infrastructure

```bash
# Foundation only (needed for Demo 1)
docker compose up -d tpt

# Full stack (needed for Demos 2–5)
docker compose up -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

### 4. One-time Teradata setup

Creates the `demo_db` database, all tables, NOS authorization objects, and seeds the aircraft registry. Safe to re-run — drops and recreates all objects.

```bash
docker compose exec -T tpt bash /tpt/scripts/run_setup.sh
```

Expected output ends with `RC (return code) = 0`.

---

## Demo 1: Avro BLOB

**Topic:** Loading Avro container files into a Teradata `DATASET STORAGE FORMAT AVRO` column, with schema evolution across two file versions.

### What it demonstrates

- The correct Teradata workflow for loading Avro Object Container Files (`.avro`) — direct `CAST(VARBYTE → DATASET AVRO)` is **not** supported; the documented path uses BTEQ `DEFERRED BY NAME` staging followed by `AvroContainerSplit`
- **Schema evolution** without ETL rewrites: v1 records (5 fields) and v2 records (8 fields) coexist in the same `DATASET AVRO` column; querying a v2 field on a v1 row returns `NULL`
- Dot-notation access (`avro.."field_name"`) on `DATASET` columns
- `AVRO_CHECK()` for inline validity verification

### Files

| File | Purpose |
|------|---------|
| `kafka/schemas/product_v1.avsc` | Avro schema: 5 fields (product_id, name, price, category, ts) |
| `kafka/schemas/product_v2.avsc` | Evolved schema: adds description, subcategory, discount_pct (nullable) |
| `kafka/producers/generate_product_avro.py` | Generates `product_v1.avro` / `product_v2.avro` + BTEQ index files |
| `tpt/scripts/setup_demo_tables.bteq` | DDL for `avro_product_stage` (BLOB) and `avro_product` (DATASET AVRO) |
| `tpt/scripts/demo01_prepare.bteq` | Truncates both tables before each run |
| `tpt/scripts/demo01_load_v1.bteq` | BTEQ DEFERRED BY NAME: loads `product_v1.avro` as a single BLOB row |
| `tpt/scripts/demo01_load_v2.bteq` | Same for `product_v2.avro` |
| `tpt/scripts/demo01_decode.bteq` | AvroContainerSplit INSERT-SELECT + verification queries |
| `demos/01-avro-blob/run.sh` | Orchestrates all four steps end-to-end |

### How to run

```bash
# From project root
bash demos/01-avro-blob/run.sh
```

### What to expect

```
══════════════════════════════════════════════
  Demo 1: Avro BLOB → Teradata DATASET column
══════════════════════════════════════════════

── 1/4  Generating Avro container files
Written 30 records → data/sample/product_v1.avro
Written 30 records → data/sample/product_v2.avro
...

── 2/4  Clearing staging tables

── 3/4  Loading Avro files into Teradata (BTEQ DEFERRED BY NAME)
      → product_v1.avro (schema v1: 5 fields)
      *** Insert completed. One row added.
      → product_v2.avro (schema v2: 8 fields)
      *** Insert completed. One row added.

── 4/4  Decoding BLOBs into DATASET column and verifying

  -- staging: 2 rows (one BLOB per container file)
  stage_id
  --------
         1
         2

  -- AvroContainerSplit: 60 rows inserted
  container_id    records
  ------------    -------
             1         30
             2         30

  -- AVRO_CHECK: all OK
  validity    cnt
  --------    ---
  OK           60

  -- schema evolution: v1 rows return NULL for v2-only fields
  ver   product_id                             price   description   discount_pct
  ---   ------------------------------------   ------  -----------   ------------
    1   08a63ba7-...                           19.99   (null)        (null)
    1   ...
    2   f3a1c204-...                           149.99  Premium...    12.5
```

### How it works

Teradata's `DATASET AVRO` type stores native Avro records but **cannot** be populated by a direct `CAST` from `VARBYTE` or `BLOB`. The correct four-step workflow is:

```
1. Generate Avro container files (.avro)
   └─ fastavro writes binary Avro Object Container File format
   └─ Schema: no namespace (avoids dotted record names that break Teradata's Avro 1.7.7 parser)

2. Stage as BLOB via BTEQ DEFERRED BY NAME
   └─ An index file contains the path to the .avro file
   └─ BTEQ reads the binary and inserts it as a single BLOB row
   └─ avro_product_stage: one row per container file

3. Split with AvroContainerSplit
   └─ Table operator (FROM-clause): ON (key_col, blob_col)
   └─ Returns: (out_container_id, avro_object_id, avro_value)
   └─ One output row per Avro record in the container

4. Query with dot notation
   └─ avro.."field_name"  — recursive field access
   └─ AVRO_CHECK(avro)    — validate individual rows
```

**Schema design rules** (required for Teradata's Avro 1.7.7 parser):
- No `"namespace"` field — a namespace causes fastavro to write a fully-qualified dotted name (e.g. `com.teradata.demo.Product`) which the parser cannot handle
- `logicalType` must be nested inside the type object: `{"type": "long", "logicalType": "timestamp-millis"}` — not at field level

### Tables

```sql
-- Staging: one row per .avro file (BLOB)
CREATE TABLE avro_product_stage (
  stage_id  INTEGER NOT NULL,
  container BLOB
) PRIMARY INDEX (stage_id);

-- Target: one row per Avro record (DATASET AVRO)
CREATE MULTISET TABLE avro_product (
  container_id  INTEGER,
  avro_obj_id   INTEGER,
  avro          DATASET(2097088000) INLINE LENGTH 64000 STORAGE FORMAT AVRO
) PRIMARY INDEX (container_id);
```

### Sample queries

After running the demo, connect to Teradata and try:

```sql
-- All records with dot-notation field access
SELECT
  container_id                AS schema_version,
  avro.."product_id"          AS product_id,
  avro.."name"                AS name,
  avro.."price"               AS price,
  avro.."category"            AS category,
  avro.."description"         AS description,   -- NULL for v1 rows
  avro.."discount_pct"        AS discount_pct   -- NULL for v1 rows
FROM demo_db.avro_product
ORDER BY 1, 2;

-- Validate all rows
SELECT AVRO_CHECK(avro), COUNT(*) FROM demo_db.avro_product GROUP BY 1;

-- Records above a price threshold (v2 only, since v1 rows return NULL for discount_pct)
SELECT avro.."product_id", avro.."price", avro.."discount_pct"
FROM demo_db.avro_product
WHERE avro.."discount_pct" > 20.0;
```

---

## Demo 2: Kafka → TPT STREAM

**Topic:** Near-real-time INSERT into Teradata directly from a live Kafka topic using the TPT STREAM operator and the Kafka Access Module (`libkafkaaxsmod.so`), with rows visible in Teradata every ~5 seconds throughout the run.

### What it demonstrates

- The TPT Kafka Access Module connecting to a live Kafka topic and streaming pipe-delimited messages into Teradata with no intermediary file
- `tbuild -l 5` (latency interval) — the key flag that forces the STREAM operator to flush its internal buffers and commit rows to Teradata every 5 seconds, even with a continuously-producing Kafka feed. Without `-l`, the STREAM operator only flushes at end-of-source; rows would never be visible while the producer is running
- `Format='DELIMITED'` with all-VARCHAR schema — the correct approach when source messages are plain text (not TPT binary format)
- Typed column conversion: all VARCHAR schema fields cast to their target types (`FLOAT`, `INTEGER`, `BYTEINT`, `DATE`, `TIMESTAMP(3)`) inside the `APPLY INSERT`
- Partitioned target table (`PPI` on `pos_date`) — the STREAM operator inserts directly into the correct partition
- Explicit `LogTable` / `ErrorTable` in the user database so the STREAM operator has CREATE TABLE rights
- Graceful shutdown: Ctrl+C kills the producer; TPT drains remaining buffered messages (controlled by the Kafka idle timeout `-W 30`) then exits cleanly with a final row count

### Files

| File | Purpose |
|------|---------|
| `kafka/schemas/adsb_position.avsc` | Avro schema for the ADS-B message (reference; not used by this demo's TPT path) |
| `kafka/producers/adsb_producer.py` | Synthetic ADS-B producer; `--format delimited` writes pipe-delimited text with `\n` terminator per message; `--continuous` runs indefinitely |
| `tpt/tbuild/kafka_stream.tbuild` | TPT job: DataConnector (Kafka Access Module) → STREAM operator; idle timeout parameterised via `$(KAFKA_IDLE_TIMEOUT)` |
| `tpt/scripts/demo02_prepare.bteq` | Truncates `adsb_positions`; drops `adsb_positions_LT` / `adsb_positions_ET` from any prior run |
| `tpt/scripts/demo02_status.bteq` | Single-line heartbeat query: `STATUS rows=N latest=TIMESTAMP` — used by the monitoring loop |
| `tpt/scripts/demo02_verify.bteq` | Post-run row count and per-aircraft summary query |
| `demos/02-kafka-tpt-stream/run.sh` | Orchestrates all steps; dual-mode (continuous default / `--bounded`) |

### How to run

```bash
# From project root — continuous streaming (default); press Ctrl+C to stop
bash demos/02-kafka-tpt-stream/run.sh

# Bounded mode — original 100-message behaviour; exits automatically
bash demos/02-kafka-tpt-stream/run.sh --bounded
```

### What to expect

**Continuous mode (default):**

```
======================================================
  Demo 2: Kafka → Teradata TPT STREAM (ADS-B)
  Broker (host):      localhost:29092
  Broker (container): kafka:9092
  Topic:              adsb-positions
  Mode:               CONTINUOUS (Ctrl+C to stop)
======================================================

── 1/3  Clearing TPT and Kafka state from any prior run
      Topic recreated.

── 2/3  Starting TPT STREAM job and producer
      Starting TPT (KAFKA_IDLE_TIMEOUT=30, latency flush -l 5)...
      Waiting 5 seconds for TPT to connect...
      Starting producer (continuous, 1 s interval, 10 aircraft)...

  Streaming — press Ctrl+C to stop.

  [12:01:15 UTC]  STATUS rows=80 latest=2026-06-22 12:01:10.443
  [12:01:25 UTC]  STATUS rows=180 latest=2026-06-22 12:01:20.512
  [12:01:35 UTC]  STATUS rows=280 latest=2026-06-22 12:01:30.447
  ...

^C
── Stopping producer...
── Waiting for TPT to drain (up to 30 s)...
   TD_INSERTER: Rows Inserted: 630
   Job ttuuser completed successfully

── 3/3  Final row count

  positions_received   earliest_ts              latest_ts              aircraft_seen
  ------------------   -------------------      -------------------    -------------
                 630   2026-06-22 12:01:05.210  2026-06-22 12:02:08    10
```

**Bounded mode (`--bounded`):** publishes 100 messages, waits for the 15-second Kafka idle timeout, then exits — same behaviour as the original implementation.

### How it works

```
1. Prepare
   └─ BTEQ truncates adsb_positions + drops STREAM operator work tables
   └─ Kafka topic deleted and recreated (clean partition offset)
   └─ TPT checkpoint file cleared (twbrmcp)

2. TPT STREAM job starts (background) with -l 5
   └─ DataConnector PRODUCER + Kafka Access Module (libkafkaaxsmod.so)
   └─ AccessModuleInitStr: -M C (Consumer) -T <topic> -B <broker> -P 0 (partition 0) -W 30 (drain window)
   └─ tbuild -l 5: latency interval — STREAM operator flushes internal buffers every 5 s
      ↑ This is the critical flag for continuous streaming.
      Without -l, flush only happens at end-of-source (when -W idle timeout fires).
      With a live producer, -W never fires → rows buffer forever → zero rows visible.
      Reference: Teradata PT Reference Guide 20.00, B035-2436-103K §tbuild.
   └─ Format=DELIMITED, TextDelimiter='|' — parses pipe-separated lines, one Kafka message = one row

3. Python producer runs continuously (--continuous)
   └─ 10 synthetic aircraft, 1 position update per aircraft per second
   └─ Each message: icao24|callsign|lat|lon|alt|vel|hdg|vrate|on_ground|squawk|pos_date|ts\n
   └─ The \n is the DELIMITED record terminator — required so the parser treats each message as one row

4. Every 5 seconds: STREAM operator flushes its buffer
   └─ ~50 rows (10 aircraft × ~5 s × 1 row/s) committed to adsb_positions
   └─ Rows visible in Teradata immediately after each flush
   └─ Monitoring loop polls every 10 s: STATUS rows=N latest=TIMESTAMP

5. On Ctrl+C: graceful shutdown
   └─ Producer killed; no new messages published
   └─ Kafka Access Module waits up to -W 30 s for the topic to go idle, then signals end-of-stream
   └─ STREAM operator flushes final buffer and exits
   └─ verify BTEQ prints the complete final row count
```

**Key technical constraints:**

| Constraint | Detail |
|---|---|
| `tbuild -l <seconds>` is required for continuous streaming | The STREAM operator only commits when it flushes buffers. Without `-l`, that only happens at end-of-source. For a live Kafka topic, end-of-source means the Kafka idle timeout (`-W`) firing — which never happens with a continuous producer. Add `-l 5` to `tbuild` to force periodic flushes. Source: B035-2436-103K |
| Two different `-W` flags — do not confuse | `tbuild -W <seconds>` is the subprocess spawn timeout (1–900, default 120) — **not** the Kafka idle timeout. The Kafka idle timeout is `-W <seconds>` inside `AccessModuleInitStr`. These are completely different options |
| `AccessModuleName` / `AccessModuleInitStr` required | Named attributes like `ConnectorName`, `TopicName`, `BootstrapServers` are **not** valid DataConnector attributes — they are silently ignored. The correct API is `AccessModuleName = 'libkafkaaxsmod.so'` with CLI-style `-M C -T -B -P -W` flags |
| All schema columns must be VARCHAR | `Format='DELIMITED'` requires every schema column to be VARCHAR/CLOB; typed conversion is done via `CAST` in the `APPLY INSERT` |
| `\n` record terminator required | DELIMITED format uses newlines as record separators; messages without `\n` are concatenated by the access module into one giant "record", causing field overflow |
| Kafka Access Module reads from offset 0 | With `PRESERVE_RESTART_INFO=YES` and a cleared checkpoint, the module reads from the start of the partition; topic is recreated each run so offset 0 = first new message |
| LogTable / ErrorTable must be explicit | STREAM operator defaults to creating work tables in DBC; specify `LogTable` and `ErrorTable` in the user database |
| No `-S y` flag | The `-S` flag causes "Failed to open Properties file" on this version; omit it |

### Table

```sql
CREATE MULTISET TABLE adsb_positions (
  icao24        CHAR(6)      NOT NULL,
  callsign      VARCHAR(8),
  pos_date      DATE         NOT NULL,    -- stored for PPI; populated by CAST(:pos_date AS DATE)
  latitude      DECIMAL(9,6) NOT NULL,
  longitude     DECIMAL(10,6) NOT NULL,
  altitude      INTEGER,
  velocity      DECIMAL(7,2),
  heading       DECIMAL(6,2),
  vertical_rate INTEGER,
  on_ground     BYTEINT      DEFAULT 0,
  squawk        CHAR(4),
  ts            TIMESTAMP(3) NOT NULL,
  ingest_ts     TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP(0)
)
PRIMARY INDEX (icao24)
PARTITION BY RANGE_N(pos_date BETWEEN DATE '2024-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY);
```

### Sample queries

```sql
-- Recent position fixes
SELECT icao24, callsign, latitude, longitude, altitude, ts
FROM demo_db.adsb_positions
WHERE pos_date = CURRENT_DATE
ORDER BY ts DESC;

-- Aircraft at cruise altitude (> 30,000 ft)
SELECT icao24, callsign, AVG(altitude) AS avg_alt_ft, COUNT(*) AS fixes
FROM demo_db.adsb_positions
GROUP BY 1, 2
HAVING AVG(altitude) > 30000
ORDER BY avg_alt_ft DESC;

-- Ingest latency (time between event and landing in Teradata)
SELECT icao24,
       AVG( (CAST(ingest_ts AS TIMESTAMP(3)) - ts) SECOND(4) ) AS avg_lag_secs
FROM demo_db.adsb_positions
GROUP BY 1
ORDER BY 1;
```

---

## Demo 3: Kafka → Kafka Connect → MinIO CSV → NOS

**Topic:** Streaming weather observations through a Kafka Connect S3 Sink into plain-date MinIO folders, then querying and incrementally loading the data via Teradata Native Object Store (NOS) with typed partition pruning.

### What it demonstrates

- Kafka Connect S3 Sink (`ByteArrayFormat`, `flush.size=1`, `TimeBasedPartitioner`) landing one CSV file per Kafka message under a plain time-partitioned path: `YYYY-MM-DD/HH/`
- `READ_NOS` with `RETURNTYPE('NOSREAD_KEYS')` to enumerate exactly which partition folders and files Kafka Connect wrote
- `CREATE FOREIGN TABLE` with `PATHPATTERN` enabling NOS **partition pruning** — the equivalent of PPI elimination on internal tables; 10–100× faster on large datasets because object listing and fetching are skipped for non-matching partitions
- **Incremental load pattern**: the FOREIGN TABLE covers the full MinIO path (all history); each run's `INSERT INTO weather_obs` uses a scoped LOCATION pointing at the current hour's folder
- `--fresh` flag for a full clean-slate reset (purges MinIO + clears all `weather_obs` rows)

### Files

| File | Purpose |
|------|---------|
| `kafka/producers/weather_kafka.py` | Publishes full CSV batches (header + 30 rows) every 5 minutes to the `weather-csv` topic |
| `kafka/connect/s3-sink.json` | S3 Sink: ByteArrayFormat, flush.size=1, TimeBasedPartitioner → `YYYY-MM-DD/HH/` |
| `tpt/scripts/demo03_nos_create.bteq` | NOSREAD_KEYS + CREATE FOREIGN TABLE with PATHPATTERN |
| `tpt/scripts/demo03_nos_load.bteq` | Incremental INSERT (current-hour partition only) + summary |
| `tpt/scripts/demo03_nos_prepare.bteq` | Pre-run: drop FOREIGN TABLE only (weather_obs untouched) |
| `tpt/scripts/demo03_nos_fresh.bteq` | Full reset: drop FOREIGN TABLE + DELETE FROM weather_obs ALL |
| `demos/03-kafka-csv-minio/run.sh` | Orchestrates all six steps end-to-end |

### How to run

```bash
# From project root — accumulates this hour's data into weather_obs
bash demos/03-kafka-csv-minio/run.sh

# Full reset: purge MinIO + clear all weather_obs rows, then run fresh
bash demos/03-kafka-csv-minio/run.sh --fresh
```

> **Runtime:** ~11 minutes for the default 3 batches at 5-minute intervals. The demo is designed to be left running while presenting — each batch confirms data flowing through the pipeline.

### What to expect

```
======================================================
  Demo 3: Kafka → Kafka Connect → MinIO → NOS
  Current partition:  2026-06-19/14
  Weather batches:    3  (interval: 300s)
  Mode:               FRESH (purge all)
======================================================

── 1/6  Preparing — clean up prior connector and topic
── 2/6  Registering Kafka Connect S3 Sink connector
      Partitioner: TimeBasedPartitioner → 2026-06-19/14/
      Waiting for connector to start. RUNNING

── 3/6  Publishing weather batches to Kafka
      [2026-06-19T14:50:27]  Batch 1/3: 30 rows → weather-csv
      [2026-06-19T14:55:27]  Batch 2/3: 30 rows → weather-csv
      [2026-06-19T15:00:27]  Batch 3/3: 30 rows → weather-csv

── 4/6  Waiting for Kafka Connect to flush files to MinIO
      [15s] Files in MinIO: 3 / 3

── 5/6  NOS: list objects, create FOREIGN TABLE with PATHPATTERN, count rows

  -- NOSREAD_KEYS: paths written by Kafka Connect
  location
  -----------------------------------------------------------------------
  /S3/.../raw/weather-csv/2026-06-19/14/weather-csv+0+0000000000.bin
  /S3/.../raw/weather-csv/2026-06-19/14/weather-csv+0+0000000001.bin
  /S3/.../raw/weather-csv/2026-06-19/14/weather-csv+0+0000000002.bin

  -- FOREIGN TABLE created with PATHPATTERN (partition-aware)
  nos_row_count: 93   (90 data rows + 3 header rows)

── 6/6  Incremental load: partition 2026-06-19/14 → weather_obs
      WHERE $var3 = '2026-06-19' AND $var4 = '14'
      Prior hours in weather_obs are untouched.

  *** Insert completed. 90 rows added.

  station_id   total_obs   avg_temp_c   max_wind_kts
  ----------   ---------   ----------   ------------
  EGLL                18       12.40             32
  EHAM                18       15.10             28
  KATL                18        9.80             35
  KJFK                18       11.20             30
  KLAX                18       18.60             19
```

Running again at a different hour accumulates a second block of 90 rows:
```
  station_id   total_obs   avg_temp_c   max_wind_kts
  ----------   ---------   ----------   ------------
  EGLL                36       13.10             34
  ...
```

### How it works

```
1. Producer (weather_kafka.py)
   └─ Generates one CSV batch every 5 minutes: header + 5 stations × 6 offsets (30 rows)
   └─ Encodes as UTF-8 bytes, produces to Kafka topic weather-csv
   └─ One Kafka message = one complete CSV file (including header row)

2. Kafka Connect S3 Sink — TimeBasedPartitioner
   └─ ByteArrayFormat: writes message bytes as-is, no schema needed
   └─ flush.size=1: a new file is written after every single Kafka message
   └─ TimeBasedPartitioner + WallClock: derives folder path from wall-clock time
   └─ path.format "yyyy-MM-dd/HH"
      → s3://demo-csv/raw/weather-csv/2026-06-19/14/weather-csv+0+NNN.bin
   └─ Files from the same hour land in the same folder; a new folder opens on the hour

3. NOS FOREIGN TABLE (partition-aware)
   └─ LOCATION covers the root path — the table spans all historical partitions
   └─ PATHPATTERN ('$var1/$var2/$var3/$var4/$var5') names each segment from bucket root
      $var1=raw, $var2=weather-csv, $var3=date (DATE), $var4=hour (BYTEINT), $var5=file
   └─ WHERE $var3 = '2026-06-19' AND $var4 = '14' prunes all other partitions before any I/O
      (string comparisons — PARTITION BY COLUMN causes error 3706 on CSV foreign tables in 20.x)

4. Incremental INSERT into weather_obs
   └─ DELETE WHERE observation_ts within current hour (idempotent re-run safety)
   └─ Scoped FOREIGN TABLE with LOCATION pointing at exactly one hour folder (no PATHPATTERN WHERE needed)
   └─ Header rows rejected by type-cast failure (CAST('station_id' AS TIMESTAMP) fails)
   └─ Prior hours in weather_obs are untouched — data accumulates across runs
```

**Key NOS design rules:**

| Rule | Detail |
|---|---|
| Plain date/hour partitioning | `YYYY-MM-DD/HH/` is cleaner than Hive `key=value` naming; WHERE clause uses `$var3 = '2026-06-22'` and `$var4 = '08'` (string comparisons) |
| `PATHPATTERN` — the most important NOS performance lever | Eliminates file listing and fetching for non-matching partitions before any I/O; 10–100× faster than unfiltered scans on large datasets |
| `PARTITION BY COLUMN` is Parquet/JSON-only on 20.x | Adding `PARTITION BY COLUMN` to a CSV foreign table triggers error 3706 ("not allowed with JSON/PARQUET table") — omit it; string PATHPATTERN WHERE clauses still prune correctly |
| PATHPATTERN is matched from bucket root, not from LOCATION | All path segments from the bucket root must be named in PATHPATTERN; LOCATION only scopes which objects are listed |
| `$var` vs `${VAR}` in BTEQ scripts | `${CURRENT_YEAR}` (curly braces) is replaced by the perl preprocessor; `$var3` (no braces) is a Teradata NOS path variable resolved at query time |
| `HEADER('TRUE')` maps column names but does not suppress header rows from `COUNT(*)` | Header rows from each file appear in raw `SELECT *`; they are rejected during typed INSERT via cast failure |
| CSV is the default FOREIGN TABLE format on 20.x | `STOREDAS` is not accepted for CSV in `CREATE FOREIGN TABLE USING`; omit it |
| DEFINER TRUSTED auth for FOREIGN TABLE | `EXTERNAL SECURITY DEFINER TRUSTED` resolves the auth object in the table's own database (`demo_db`); `INVOKER TRUSTED` would look in the session user's database and fail for user `dbc` |

### Tables

```sql
-- NOS foreign table (partition-aware, spans full MinIO path)
-- PATHPATTERN segments from bucket root: $var1=raw, $var2=weather-csv,
--   $var3=date (e.g. 2026-06-22), $var4=hour (e.g. 14), $var5=filename
CREATE MULTISET FOREIGN TABLE weather_nos_ft ,FALLBACK ,
  EXTERNAL SECURITY DEFINER TRUSTED minio_nos_auth
(
  Location          VARCHAR(2048) CHARACTER SET UNICODE CASESPECIFIC,
  station_id        VARCHAR(4),
  observation_ts    VARCHAR(30),
  temperature_c     VARCHAR(10),
  wind_speed_kts    VARCHAR(5),
  wind_direction    VARCHAR(5),
  visibility_m      VARCHAR(7),
  precipitation_mm  VARCHAR(7),
  pressure_hpa      VARCHAR(8),
  conditions        VARCHAR(10)
)
USING (
  LOCATION    ('/S3/<host>:9000/demo-csv/raw/weather-csv/')
  HEADER      ('TRUE')
  PATHPATTERN ('$var1/$var2/$var3/$var4/$var5')
)
NO PRIMARY INDEX
NO PRIMARY INDEX;

-- Relational target (typed columns; accumulates across runs)
CREATE MULTISET TABLE weather_obs (
  station_id       CHAR(4)       NOT NULL,
  observation_ts   TIMESTAMP(0)  NOT NULL,
  temperature_c    DECIMAL(5,1),
  wind_speed_kts   SMALLINT,
  wind_direction   SMALLINT,
  visibility_m     INTEGER,
  precipitation_mm DECIMAL(6,2),
  pressure_hpa     DECIMAL(7,1),
  conditions       VARCHAR(10),
  ingest_ts        TIMESTAMP(0)  DEFAULT CURRENT_TIMESTAMP(0)
) PRIMARY INDEX (station_id, observation_ts);
```

### Sample queries

After running the demo, connect to Teradata and try:

```sql
-- Path-filtered NOS query: reads only the current-hour partition (pruning demo)
-- $var3 and $var4 are PATHPATTERN string variables — compare as string literals
-- (PARTITION BY COLUMN not used: causes error 3706 on CSV tables in 20.x)
SELECT station_id,
       CAST(TRIM(observation_ts) AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') AS obs_ts,
       CAST(TRIM(temperature_c) AS DECIMAL(5,1)) AS temp_c,
       conditions
FROM demo_db.weather_nos_ft
WHERE $var3 = '2026-06-19'
  AND $var4 = '14'
ORDER BY obs_ts DESC;

-- Full NOS scan across all partitions (no path filter — reads every file)
SELECT COUNT(*) AS total_nos_rows FROM demo_db.weather_nos_ft;

-- Aggregates from the relational table across all accumulated hours
SELECT station_id,
       COUNT(*)             AS obs_count,
       AVG(temperature_c)  AS avg_temp_c,
       MAX(wind_speed_kts) AS max_wind_kts,
       MIN(pressure_hpa)   AS min_pressure
FROM demo_db.weather_obs
GROUP BY station_id
ORDER BY station_id;

-- IFR / LIFR conditions across all loaded partitions
SELECT station_id, observation_ts, visibility_m, conditions
FROM demo_db.weather_obs
WHERE conditions IN ('IFR', 'LIFR')
ORDER BY observation_ts DESC;
```

---

## Demo 4: Kafka → Flink → Iceberg → Teradata OTF

**Topic:** Flink SQL continuously streams ADS-B messages from Kafka into an Apache Iceberg table (Parquet on MinIO, metadata in Hive Metastore). Teradata queries the live Iceberg table via a **DATALAKE** object (OTF) using 3-part dot notation — no ETL, no COPY, no TPT. New rows become visible within ~30 seconds of ingestion.

### What it demonstrates

- Flink SQL checkpointing as the Iceberg commit mechanism: each 30-second checkpoint atomically commits a new Iceberg snapshot, which Teradata reads on the very next SELECT
- The **DATALAKE** object — Teradata's OTF interface to an Iceberg catalog; connects to Hive Metastore (catalog) + MinIO (storage) with a single DDL object; tables queried via `<datalake>.<namespace>.<table>` 3-part notation
- `TD_SNAPSHOTS()` table function: lists committed Iceberg snapshots — demonstrates the growing snapshot history as streaming continues
- Open-lakehouse pattern: Flink writes Parquet data files; Teradata, Spark, Trino can all query the same files simultaneously via the Iceberg catalog
- Iceberg partitioning by `pos_date` (daily) — enables partition pruning in Teradata OTF queries

### Files

| File | Purpose |
|------|---------|
| `kafka/producers/adsb_producer.py` | Same ADS-B producer as Demo 2; `--continuous` for streaming, `--count 200` for bounded |
| `flink/jobs/demo04_stream.sql` | Flink SQL: register Hive catalog, create Iceberg table, streaming INSERT from Kafka (checkpoint 30s) |
| `flink/jobs/demo04_batch.sql` | Flink SQL: same table + batch INSERT (earliest→latest-offset); exits naturally when all messages processed |
| `flink/jobs/demo04_drop.sql` | Reset: DROP TABLE + DROP DATABASE — removes Parquet data files from MinIO |
| `tpt/scripts/demo04_datalake_create.bteq` | CREATE DATALAKE connecting Teradata to HMS + MinIO; HELP DATALAKE to verify |
| `tpt/scripts/demo04_datalake_drop.bteq` | DROP DATALAKE (reset) |
| `tpt/scripts/demo04_otf_query.bteq` | Single-line heartbeat: `STATUS rows=N latest=TIMESTAMP` for monitoring loop |
| `tpt/scripts/demo04_otf_verify.bteq` | Final: HELP TABLE (type mapping), row count, per-aircraft summary, TD_SNAPSHOTS() |
| `demos/04-kafka-flink-iceberg/run.sh` | Orchestration — dual-mode |

### How to run

```bash
# From project root — continuous streaming (default); press Ctrl+C to stop
bash demos/04-kafka-flink-iceberg/run.sh

# Bounded mode — 200 messages; exits automatically
bash demos/04-kafka-flink-iceberg/run.sh --bounded
```

### What to expect

**Continuous mode (default):**

```
======================================================
  Demo 4: Kafka → Flink → Iceberg → Teradata OTF
  Flink REST:               http://localhost:8081
  HMS (host-facing):        192.168.1.x:9083
  MinIO (host-facing):      192.168.1.x:9000
  Mode:                     CONTINUOUS (Ctrl+C to stop)
======================================================

── 1/4  Resetting prior state
      Dropping Iceberg table via Flink SQL...
      Topic recreated.

── 2/4  Creating DATALAKE object in Teradata
      DatabaseName        DatabaseProperties
      default             'hive.metastore.database.owner'='public'...

── 3/4  Starting Flink job and producer
      Flink job ID: a3b2c1d4e5f6...
      Starting producer (continuous, 1s interval, 10 aircraft)...

  Streaming — rows visible in Teradata every ~30s (one Iceberg checkpoint).
  Press Ctrl+C to stop.

  [12:01:05 UTC]  STATUS rows=0    latest=none
  [12:01:35 UTC]  STATUS rows=290  latest=2026-06-22 12:01:33.412
  [12:02:05 UTC]  STATUS rows=580  latest=2026-06-22 12:02:03.891
  [12:02:35 UTC]  STATUS rows=870  latest=2026-06-22 12:02:33.107
  ...

^C
── Stopping producer...
── Waiting 35s for final Iceberg checkpoint to commit...
── Cancelling Flink job a3b2c1d4e5f6...

── 4/4  Final snapshot

  ColumnName      IcebergType   TeradataType
  icao24          STRING        VARCHAR(2000)
  ts              TIMESTAMP     TIMESTAMP
  ...

  total_rows   aircraft_seen   first_ts                  last_ts
  ----------   -------------   ------                    ------
        1450              10   2026-06-22 12:00:35.121   2026-06-22 12:03:08.447

  icao24   callsign   positions   avg_alt_ft
  ------   --------   ---------   ----------
  3950f2   AFR674           145       38000
  ...

  snapshot_id    committed_at              operation   added_files
  -----------    -------------------------  ---------  -----------
  8392847362...  2026-06-22 12:01:05.000   append      1
  7461928374...  2026-06-22 12:01:35.000   append      1
  ...
```

**Bounded mode (`--bounded`):** runs producer (200 messages), Flink batch job reads all messages and commits one snapshot, verify query runs automatically.

### How it works

```
1. Reset
   └─ Flink SQL client drops Iceberg table (removes Parquet + metadata from MinIO)
   └─ DATALAKE dropped from Teradata (safe to recreate)
   └─ Kafka topic deleted and recreated (clean offset)

2. DATALAKE setup (one-time per session)
   └─ CREATE DATALAKE demo_iceberg: CATALOG_TYPE='hive', HMS at HOST_IP:9083
   └─ STORAGE_ENDPOINT=http://HOST_IP:9000 (MinIO, path-style, no SSL)
   └─ Auth objects (hms_catalog_auth, minio_storage_auth) resolved from demo_db
   └─ HELP DATALAKE confirms Hive namespaces visible from Teradata

3. Flink streaming INSERT (continuous mode)
   └─ Creates hive_catalog (Iceberg Flink connector → Hive Metastore)
   └─ CREATE TABLE IF NOT EXISTS demo.adsb_positions (partitioned by pos_date)
   └─ CREATE TEMPORARY TABLE kafka_adsb (all-STRING schema, csv '|' format, latest-offset)
   └─ INSERT: CASTs + TO_TIMESTAMP() on all columns → Iceberg Parquet files on MinIO
   └─ Checkpoint every 30s (EXACTLY_ONCE) → atomic Iceberg snapshot per checkpoint
   └─ SQL client exits after submitting; job continues running in Flink cluster

4. Monitoring loop
   └─ Every 30s: SELECT COUNT(*) + MAX(ts) from demo_iceberg.demo.adsb_positions
   └─ Row count grows by ~300/interval (10 aircraft × 30s × 1 row/s)
   └─ Each new count represents a freshly committed Iceberg snapshot

5. Graceful shutdown (Ctrl+C)
   └─ Producer killed (no new Kafka messages)
   └─ 35s wait → final checkpoint commits remaining buffered rows
   └─ Flink job cancelled via REST (PATCH /jobs/{id}?mode=cancel)
   └─ Final verify: row count, per-aircraft, TD_SNAPSHOTS() history
```

**Key OTF design rules (from validated vault examples):**

| Rule | Detail |
|---|---|
| DATALAKE, not FOREIGN TABLE | Iceberg tables are queried via `CREATE DATALAKE` with `CATALOG_TYPE('hive')`; do not use `CREATE FOREIGN TABLE` for Iceberg |
| `HOST_IP` in CATALOG_LOCATION and STORAGE_ENDPOINT | Teradata is external to Docker; `hive-metastore:9083` and `minio:9000` are not reachable from Teradata — use `HOST_IP` (set in `.env`) which is exposed via docker-compose port bindings |
| Auth objects must be in the session database | `INVOKER TRUSTED` resolves auth objects from the active session database; `DATABASE ${TD_DATABASE}` must be set before `CREATE DATALAKE` |
| `S3_PATH_STYLE_ACCESS('true')` required for MinIO | MinIO uses path-style S3 URLs; virtual-hosted-style (the AWS default) does not work |
| `S3_SSL_ENABLED('false')` required for HTTP | MinIO in this stack runs HTTP; TLS is not configured |
| Namespace created by Flink, not Teradata | `CREATE DATABASE via DATALAKE from Teradata → error 7825`; Flink's `CREATE DATABASE IF NOT EXISTS demo` handles namespace creation |
| Rows visible at checkpoint boundaries | Flink Iceberg sink commits per checkpoint (30s); queries between checkpoints return the previous snapshot's data — this is the expected behaviour |
| `TD_SNAPSHOTS()` shows commit history | Each checkpoint that added rows creates one snapshot entry; time-travel via `FOR TIMESTAMP AS OF TIMESTAMP '...'` works across these snapshots |

### DATALAKE DDL reference

```sql
-- Auth objects already created by run_setup.sh in demo_db:
--   hms_catalog_auth  (INVOKER TRUSTED, USER '', PASSWORD '' — HMS needs no auth)
--   minio_storage_auth (INVOKER TRUSTED, USER 'minioadmin', PASSWORD 'minioadmin')

DATABASE demo_db;

CREATE DATALAKE demo_iceberg
  EXTERNAL SECURITY INVOKER TRUSTED CATALOG hms_catalog_auth,
  EXTERNAL SECURITY INVOKER TRUSTED STORAGE minio_storage_auth
USING
  CATALOG_TYPE('hive')
  CATALOG_LOCATION('thrift://<HOST_IP>:9083')
  STORAGE_LOCATION('s3://iceberg/warehouse/')
  STORAGE_ENDPOINT('http://<HOST_IP>:9000')
  S3_PATH_STYLE_ACCESS('true')
  STORAGE_REGION('us-east-1')
  S3_SSL_ENABLED('false')
TABLE FORMAT iceberg;
```

### Sample queries

After running the demo, connect to Teradata and try:

```sql
-- Explore what's in the catalog
HELP DATALAKE demo_iceberg;
HELP TABLE demo_iceberg.demo.adsb_positions;

-- Current snapshot (latest committed data)
SELECT icao24, callsign, latitude, longitude, altitude, ts
FROM demo_iceberg.demo.adsb_positions
WHERE pos_date = '2026-06-22'
ORDER BY ts DESC
SAMPLE 20;

-- Aggregate across all partitions
SELECT icao24, TRIM(callsign) AS callsign,
       COUNT(*) AS fixes,
       AVG(CAST(altitude AS FLOAT)) AS avg_alt_ft
FROM demo_iceberg.demo.adsb_positions
GROUP BY 1, 2
ORDER BY fixes DESC;

-- Snapshot history (one row per Flink checkpoint that committed data)
SELECT * FROM TD_SNAPSHOTS('demo_iceberg.demo.adsb_positions');

-- Time travel: read the table as it was at a specific past snapshot
SELECT COUNT(*) AS rows_at_snapshot
FROM demo_iceberg.demo.adsb_positions
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-22 12:01:00';
```

---

## Demo 5: Flink Lookup Join → Teradata enrichment

**Topic:** A live ADS-B stream carries only ICAO24 aircraft identifiers. Teradata holds the master `aircraft_ref` reference table (registration, aircraft type, operator, country). Flink enriches each position message in-flight via a **JDBC Lookup Join** — querying Teradata per-message with a 30-second cache — then writes enriched rows to Iceberg. Teradata queries its own enrichment back via OTF.

This reverses the usual data-flow: instead of pushing data *into* Teradata, Flink pulls reference data *from* Teradata to enhance a real-time stream.

### What it demonstrates

- **Teradata as an enrichment source** — the JDBC Lookup Join connector executes `SELECT … WHERE icao24 = ?` against Teradata using the `terajdbc` driver; results are cached per the configured TTL, keeping Teradata load minimal
- **Cache TTL and live invalidation** — with a 30-second TTL, an `UPDATE` to `aircraft_ref` in Teradata propagates to newly enriched Iceberg rows within one checkpoint window; visible without restarting Flink
- **LEFT JOIN graceful degradation** — ICAO24 codes with no matching reference row receive `UNKNOWN` defaults rather than being dropped; the enriched stream never stalls
- **Teradata as both source and destination** — `aircraft_ref` is the lookup source; `enriched_positions` in Iceberg is the OTF query target; both accessed from the same Teradata session

### Files

| File | Purpose |
|------|---------|
| `kafka/producers/adsb_producer.py` | Same ADS-B producer as Demos 2 and 4 — reused without modification |
| `flink/jobs/demo05_stream.sql` | Flink SQL template: Kafka source + JDBC Lookup Join + Iceberg sink (checkpoint 30s) |
| `flink/jobs/demo05_batch.sql` | Bounded variant: earliest→latest-offset Kafka scan; exits when all messages processed |
| `flink/jobs/demo05_drop.sql` | Reset: DROP TABLE enriched_positions (leaves Demo 4's adsb_positions intact) |
| `tpt/scripts/demo05_setup.bteq` | CREATE TABLE aircraft_ref + INSERT 10 fleet rows; safe to re-run |
| `tpt/scripts/demo05_teardown.bteq` | DROP TABLE aircraft_ref (full reset) |
| `tpt/scripts/demo05_otf_query.bteq` | Single-line heartbeat: `STATUS rows=N aircraft=N latest=TIMESTAMP` |
| `tpt/scripts/demo05_otf_verify.bteq` | Final: HELP TABLE, row/aircraft summary, per-aircraft enrichment, TD_SNAPSHOTS() |
| `demos/05-flink-td-enrich/run.sh` | Orchestration — dual-mode (continuous / bounded) |

### How to run

```bash
# Continuous streaming (default) — press Ctrl+C to stop
bash demos/05-flink-td-enrich/run.sh

# Bounded mode — 200 messages; exits automatically
bash demos/05-flink-td-enrich/run.sh --bounded
```

### What to expect

```
======================================================
  Demo 5: Flink Lookup Join → Teradata enrichment
  Teradata host:            192.168.1.199
  Reference table:          demo_db.aircraft_ref
  Mode:                     CONTINUOUS (Ctrl+C to stop)
======================================================

── 1/6  Resetting prior state
      Dropping enriched_positions Iceberg table...
      Topic recreated.

── 2/6  Setting up Teradata reference table
      icao24   registration   aircraft_type   operator              country
      ------   ------------   -------------   --------------------  -----------
      34618a   EC-MXV         A330-200        Iberia                Spain
      3950f2   F-HTYB         A350-900        Air France            France
      3c4b0f   D-AIHE         A340-600        Lufthansa             Germany
      3f7062   EI-FTP         B737-800        Ryanair               Ireland
      ...

── 3/6  Ensuring DATALAKE object is present

── 4/6  Starting Flink lookup-join streaming job
      Flink job ID: f1e2d3c4b5a6...
      Starting producer (continuous, 1s interval, 10 aircraft)...

  Enriched rows visible in Teradata every ~30s (one Iceberg checkpoint).
  Each row includes registration, aircraft_type, operator, country
  sourced from Teradata aircraft_ref via JDBC Lookup Join.

  DEMO TIP — live cache invalidation:
    UPDATE demo_db.aircraft_ref WHERE icao24='3f7062'
    SET operator='Ryanair DAC';
    The change appears in enriched_positions after the 30s cache expires.

── 5/6  Streaming — press Ctrl+C to stop

  [12:01:05 UTC]  STATUS rows=0    aircraft=0  latest=none
  [12:01:35 UTC]  STATUS rows=291  aircraft=10 latest=2026-06-22 12:01:33.882
  [12:02:05 UTC]  STATUS rows=582  aircraft=10 latest=2026-06-22 12:02:04.215
  ...

^C
── Stopping producer...
── Waiting 35s for final Iceberg checkpoint to commit...
── Cancelling Flink job f1e2d3c4b5a6...

── 6/6  Final verification

  icao24   callsign   positions   registration   type       operator          country
  ------   --------   ---------   ------------   --------   ---------------   -----------
  34618a   IBE601           145   EC-MXV         A330-200   Iberia            Spain
  3950f2   AFR674           145   F-HTYB         A350-900   Air France        France
  3c4b0f   DLH463           145   D-AIHE         A340-600   Lufthansa         Germany
  3f7062   RYR4421          145   EI-FTP         B737-800   Ryanair DAC       Ireland  ← updated
  ...
```

### How it works

```
1. Reset
   └─ Flink SQL client drops enriched_positions (leaves adsb_positions intact)
   └─ Kafka topic deleted and recreated (clean offset)

2. Reference table setup
   └─ BTEQ: DROP + CREATE aircraft_ref in demo_db
   └─ INSERT 10 rows matching the adsb_producer.py fleet
   └─ SELECT confirms all rows visible in Teradata

3. DATALAKE verification
   └─ demo04_datalake_create.bteq: idempotent CREATE DATALAKE demo_iceberg
   └─ Required for OTF queries in steps 5/6

4. Flink job submission
   └─ demo05_stream.sql substituted (${TD_HOST} etc. → real values via perl)
   └─ Written to flink/jobs/demo05_stream_sub.sql (bind-mounted volume)
   └─ Flink SQL client submits streaming INSERT; exits after job accepted
   └─ Job continues running in cluster; ID captured for cleanup

5. Streaming with monitoring
   └─ adsb_producer.py publishes 10 aircraft positions at 1s interval
   └─ Flink reads each row from Kafka, looks up icao24 in aircraft_ref:
        └─ Cache hit (within 30s TTL): uses cached registration/operator
        └─ Cache miss: issues SELECT FROM aircraft_ref WHERE icao24 = ? to Teradata
   └─ Enriched rows written to Iceberg; committed every 30s checkpoint
   └─ Monitor loop queries Teradata OTF every 30s for row count

6. Graceful shutdown (Ctrl+C)
   └─ Producer killed → wait 35s → final Iceberg snapshot committed
   └─ Flink job cancelled via REST PATCH /jobs/{id}?mode=cancel
   └─ Final verify: per-aircraft table with enriched fields + TD_SNAPSHOTS()
```

### Live cache invalidation walkthrough

The 30-second cache TTL makes Teradata's role as a live reference store visible during a demo:

```sql
-- 1. Observe Ryanair rows in Teradata OTF showing operator='Ryanair'
SELECT icao24, operator FROM demo_iceberg.demo.enriched_positions
WHERE icao24 = '3f7062' ORDER BY ts DESC SAMPLE 3;

-- 2. Update the reference table in Teradata
UPDATE demo_db.aircraft_ref WHERE icao24 = '3f7062'
SET operator = 'Ryanair DAC';

-- 3. Wait ~35s (cache TTL expires; next Kafka messages re-query Teradata)

-- 4. New enriched rows show the updated operator
SELECT icao24, operator, ts FROM demo_iceberg.demo.enriched_positions
WHERE icao24 = '3f7062' ORDER BY ts DESC SAMPLE 5;
-- operator transitions from 'Ryanair' → 'Ryanair DAC' at the cache boundary
```

### aircraft_ref reference table

| icao24 | registration | aircraft_type | operator | country |
|--------|-------------|--------------|---------|---------|
| 34618a | EC-MXV | A330-200 | Iberia | Spain |
| 3950f2 | F-HTYB | A350-900 | Air France | France |
| 3c4b0f | D-AIHE | A340-600 | Lufthansa | Germany |
| 3f7062 | EI-FTP | B737-800 | Ryanair | Ireland |
| 400a5b | G-YMML | B777-200 | British Airways | UK |
| 4073d6 | G-TUIM | B787-8 | TUI Airways | UK |
| 440a45 | G-VPOP | A350-1000 | Virgin Atlantic | UK |
| 4841d8 | PH-BVI | B777-200 | KLM | Netherlands |
| 4b1803 | HB-JCF | A220-300 | Swiss Int'l Air Lines | Switzerland |
| 4ca87a | EI-GEK | A330-300 | Aer Lingus | Ireland |

### Flink SQL key excerpts

```sql
-- Processing-time attribute — required for lookup join temporal syntax
CREATE TEMPORARY TABLE kafka_adsb (
    icao24    STRING,
    ...
    proc_time AS PROCTIME()   -- not written to Iceberg
) WITH ('connector' = 'kafka', ...);

-- Teradata JDBC lookup table — Flink issues parameterised SELECT per cache miss
CREATE TEMPORARY TABLE aircraft_ref (
    icao24        STRING,
    registration  STRING,
    aircraft_type STRING,
    operator      STRING,
    country       STRING,
    PRIMARY KEY (icao24) NOT ENFORCED  -- declares lookup key; no uniqueness check
) WITH (
    'connector'                               = 'jdbc',
    'url'                                     = 'jdbc:teradata://<TD_HOST>/DATABASE=demo_db,...',
    'table-name'                              = 'aircraft_ref',
    'lookup.cache'                            = 'PARTIAL',
    'lookup.partial-cache.max-rows'           = '10000',
    'lookup.partial-cache.expire-after-write' = '30s'
);

-- Temporal lookup join
INSERT INTO enriched_positions
SELECT a.icao24, ..., r.registration, r.aircraft_type, r.operator, r.country
FROM kafka_adsb a
LEFT JOIN aircraft_ref FOR SYSTEM_TIME AS OF a.proc_time AS r
    ON a.icao24 = r.icao24;
```

---

## Further reading

- Teradata DATASET Data Type — B035-1198 (Avro Object Container File loading)
- Teradata Parallel Transporter Reference Guide — B035-2436
- TPT Kafka Access Module — B035-2447 (Access Module Reference)
- Teradata Native Object Store Getting Started Guide — B035-2198
