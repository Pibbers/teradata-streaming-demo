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

**Topic:** Continuous near-real-time INSERT into Teradata directly from a Kafka topic using the TPT STREAM operator and the Kafka Access Module (`libkafkaaxsmod.so`).

### What it demonstrates

- The TPT Kafka Access Module connecting to a live Kafka topic and streaming pipe-delimited messages into Teradata with no intermediary file
- `Format='DELIMITED'` with all-VARCHAR schema — the correct approach when source messages are plain text (not TPT binary format)
- Typed column conversion: all VARCHAR schema fields cast to their target types (`FLOAT`, `INTEGER`, `BYTEINT`, `DATE`, `TIMESTAMP(3)`) inside the `APPLY INSERT`
- Partitioned target table (`PPI` on `pos_date`) — the STREAM operator inserts directly into the correct partition
- Explicit `LogTable` / `ErrorTable` in the user database so the STREAM operator has CREATE TABLE rights

### Files

| File | Purpose |
|------|---------|
| `kafka/schemas/adsb_position.avsc` | Avro schema for the ADS-B message (reference; not used by this demo's TPT path) |
| `kafka/producers/adsb_producer.py` | Synthetic ADS-B producer; `--format delimited` writes pipe-delimited text with `\n` terminator per message |
| `tpt/tbuild/kafka_stream.tbuild` | TPT job: DataConnector (Kafka Access Module) → STREAM operator |
| `tpt/scripts/demo02_prepare.bteq` | Truncates `adsb_positions`; drops `adsb_positions_LT` / `adsb_positions_ET` from any prior run |
| `tpt/scripts/demo02_verify.bteq` | Post-run row count and per-aircraft summary query |
| `demos/02-kafka-tpt-stream/run.sh` | Orchestrates all steps end-to-end |

### How to run

```bash
# From project root
bash demos/02-kafka-tpt-stream/run.sh
```

### What to expect

```
══════════════════════════════════════════════
  Demo 2: Kafka → Teradata TPT STREAM (ADS-B)
══════════════════════════════════════════════

── 1/3  Clearing TPT and Kafka state from any prior run
      Delete completed. 0 rows removed.        ← or 100 if re-run
      Topic recreated.

── 2/3  Starting TPT STREAM job (background) then producing messages
      TPT connects to kafka:9092 inside the demo-net network.
      Waiting 5 seconds for TPT to connect...

      Publishing 100 ADS-B messages (10 aircraft × 10 cycles × 1s interval)...
      [timestamp] 50 ADS-B messages sent to adsb-positions
      [timestamp] 100 ADS-B messages sent to adsb-positions
      Producer done. TPT will exit ~15 seconds after the last message.

      KAFKA_AXSMOD: Ending due to -rwait 15 timeout
      TD_INSERTER: Rows Inserted: 100
      TD_INSERTER: Total Rows in Error Table: 0
      Job ttuuser completed successfully

── 3/3  Verifying rows in adsb_positions

  positions_received   earliest_ts              latest_ts              aircraft_seen
  ------------------   -------------------      -------------------    -------------
                 100   2026-06-19 12:46:47.621  2026-06-19 12:46:55    10

  icao24  callsign  position_count  avg_altitude_ft
  ------  --------  --------------  ---------------
  4073d6  TOM3XT                10  3.69E+004
  3950f2  AFR674                10  3.80E+004
  ...
```

### How it works

```
1. Prepare
   └─ BTEQ truncates adsb_positions + drops STREAM operator work tables
   └─ Kafka topic deleted and recreated (clean partition offset)
   └─ TPT checkpoint file cleared (twbrmcp)

2. TPT STREAM job starts (background)
   └─ DataConnector PRODUCER + Kafka Access Module (libkafkaaxsmod.so)
   └─ AccessModuleInitStr: -M C (Consumer) -T <topic> -B <broker> -P 0 (partition 0) -W 15 (15s idle timeout)
   └─ Format=DELIMITED, TextDelimiter='|' — parses pipe-separated lines, one Kafka message = one row

3. Python producer sends 100 messages while TPT is connected
   └─ 10 synthetic aircraft × 10 position updates × 1s interval ≈ 10 seconds
   └─ Each message: icao24|callsign|lat|lon|alt|vel|hdg|vrate|on_ground|squawk|pos_date|ts\n
   └─ The \n is the DELIMITED record terminator — required so the parser treats each message as one row

4. TPT idle timeout fires (15s after last message)
   └─ STREAM operator flushes and commits all rows
   └─ All 100 rows land in adsb_positions in one load phase (4 seconds)
   └─ Job exits 0
```

**Key technical constraints** (discovered during implementation):

| Constraint | Detail |
|---|---|
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

**Topic:** Streaming weather observations through a Kafka Connect S3 Sink into MinIO, then querying the CSV files in-place via Teradata Native Object Store (NOS).

### What it demonstrates

- The Kafka Connect S3 Sink connector (`ByteArrayFormat`, `flush.size=1`) landing one CSV file per Kafka message in MinIO — no code beyond the connector config
- `READ_NOS` with `RETURNTYPE('NOSREAD_KEYS')` to enumerate exactly which files Kafka Connect wrote, before touching the schema
- `READ_NOS` with `RETURNTYPE('NOSREAD_SCHEMA')` for automatic column inference from a CSV header row — useful before committing to a `CREATE FOREIGN TABLE`
- `CREATE FOREIGN TABLE` as a thin, persistent read layer over the MinIO path; `STOREDAS CSV` with `HEADER('TRUE')` strips the header from each file
- Typed `INSERT … SELECT` that casts the all-VARCHAR foreign table schema into the typed `weather_obs` columns

### Files

| File | Purpose |
|------|---------|
| `kafka/producers/weather_kafka.py` | Publishes full CSV batches (header + 30 rows) to the `weather-csv` Kafka topic |
| `kafka/connect/s3-sink.json` | S3 Sink connector: ByteArrayFormat, flush.size=1, path `raw/weather-csv/` |
| `tpt/scripts/demo03_nos_create.bteq` | NOSREAD_KEYS → NOSREAD_SCHEMA → CREATE FOREIGN TABLE + row count |
| `tpt/scripts/demo03_nos_load.bteq` | Typed INSERT from NOS foreign table into `weather_obs` + summary |
| `demos/03-kafka-csv-minio/run.sh` | Orchestrates all six steps end-to-end |

### How to run

```bash
# From project root
bash demos/03-kafka-csv-minio/run.sh
```

### What to expect

```
══════════════════════════════════════════════════════
  Demo 3: Kafka → Kafka Connect → MinIO → NOS
══════════════════════════════════════════════════════

── 1/6  Preparing — clean up any previous run
      Recreating Kafka topic: weather-csv
      Purging MinIO prefix: demo-csv/raw/weather-csv/
      Deleted 0 object(s) from MinIO.
      Clearing Teradata: weather_nos_ft, weather_obs

── 2/6  Registering Kafka Connect S3 Sink connector
      Waiting for connector to start......... RUNNING

── 3/6  Publishing weather batches to Kafka
      Each batch: 5 stations × 6 hourly offsets = 30 rows + CSV header

      [2026-06-19T12:00:01]  Batch 1/3: 30 rows → weather-csv
      [2026-06-19T12:00:06]  Batch 2/3: 30 rows → weather-csv
      [2026-06-19T12:00:11]  Batch 3/3: 30 rows → weather-csv
      Done. 3 CSV batch(es) published to weather-csv.

── 4/6  Waiting for Kafka Connect to flush files to MinIO
      Files found in MinIO: 3 (expected: 3)

── 5/6  NOS: list objects, discover schema, create FOREIGN TABLE
      A) NOSREAD_KEYS  — list files written by Kafka Connect
      B) NOSREAD_SCHEMA — infer column names from CSV header
      C) CREATE FOREIGN TABLE weather_nos_ft
      D) COUNT(*) sanity check (expect 90 rows)

  -- A: files in MinIO
  location                                                     objectlength
  ----------------------------------------------------------   ------------
  /S3/.../raw/weather-csv/partition=0/weather-csv+0+00000000            961
  /S3/.../raw/weather-csv/partition=0/weather-csv+0+00000001            961
  /S3/.../raw/weather-csv/partition=0/weather-csv+0+00000002            961

  -- B: schema discovered from CSV header
  columnid   columnname        typename
  --------   ---------------   -------
         1   station_id        VARCHAR
         2   observation_ts    VARCHAR
         3   temperature_c     VARCHAR
         4   wind_speed_kts    VARCHAR
         5   wind_direction    VARCHAR
         6   visibility_m      VARCHAR
         7   precipitation_mm  VARCHAR
         8   pressure_hpa      VARCHAR
         9   conditions        VARCHAR

  -- D: row count
  nos_row_count
  -------------
             90

── 6/6  Loading weather_obs from NOS and showing summary

  station_id   obs_count   avg_temp_c   max_wind_kts
  ----------   ---------   ----------   ------------
  EGLL                18       12.40             32
  EHAM                18       15.10             28
  KATL                18        9.80             35
  KJFK                18       11.20             30
  KLAX                18       18.60             19
```

### How it works

```
1. Producer (weather_kafka.py)
   └─ Generates a complete CSV batch per call: header + 5 stations × 6 offsets
   └─ Encodes as UTF-8 bytes, produces to Kafka topic weather-csv
   └─ One Kafka message = one complete CSV file (including header row)

2. Kafka Connect S3 Sink
   └─ ByteArrayFormat: writes message bytes as-is — no schema, no transformation
   └─ flush.size=1: a new S3 file is written after every single Kafka message
   └─ Path: s3://demo-csv/raw/weather-csv/partition=0/weather-csv+0+NNNNNNNNNN
   └─ No file extension (ByteArrayFormat omits it — STOREDAS overrides detection)

3. NOS exploration
   └─ NOSREAD_KEYS: enumerate files before committing to a schema
   └─ NOSREAD_SCHEMA: infer column names from the CSV header row
      (STOREDAS('CSV') required to override extension-based detection)

4. FOREIGN TABLE
   └─ LOCATION recurses into partition=0/ subdirectory automatically
   └─ HEADER('TRUE') strips the header from each individual file
   └─ All columns VARCHAR — NOS does no type conversion at the read layer

5. INSERT … SELECT with explicit CASTs
   └─ observation_ts: CAST(TRIM(…) AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS')
   └─ Numeric fields: CAST(TRIM(…) AS DECIMAL / SMALLINT / INTEGER)
```

**Key NOS design rules:**

| Rule | Detail |
|---|---|
| `STOREDAS` required for extensionless files | `ByteArrayFormat` produces files with no `.csv` extension; `STOREDAS ('CSV')` forces CSV parsing regardless |
| `HEADER('TRUE')` applies per file | Each file has its own header row; NOS strips it from each file independently |
| NOS recurses into subdirectories | `partition=0/` created by Kafka Connect's DefaultPartitioner is traversed automatically |
| All-VARCHAR FOREIGN TABLE | NOS performs no type conversion; do all CAST operations in the consuming query |
| Two authorization forms | `EXTERNAL SECURITY INVOKER TRUSTED minio_nos_auth` for FOREIGN TABLE; inline JSON `'{"access_id":"...","access_key":"..."}'` for READ_NOS (READ_NOS AUTHORIZATION NVP requires Simplified auth, not INVOKER TRUSTED) |

### Tables

```sql
-- NOS foreign table (read-only, no physical storage in Teradata)
CREATE FOREIGN TABLE weather_nos_ft,
  EXTERNAL SECURITY INVOKER TRUSTED minio_nos_auth
(
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
  LOCATION ('/S3/<host>:9000/demo-csv/raw/weather-csv/')
  STOREDAS  CSV
  HEADER   ('TRUE')
)
NO PRIMARY INDEX;

-- Relational target (typed columns, loaded from NOS)
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
-- Live NOS query (reads MinIO files directly on every execution)
SELECT station_id,
       CAST(TRIM(observation_ts) AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') AS obs_ts,
       CAST(TRIM(temperature_c) AS DECIMAL(5,1))  AS temp_c,
       CAST(TRIM(wind_speed_kts) AS SMALLINT)     AS wind_kts,
       conditions
FROM demo_db.weather_nos_ft
WHERE station_id = 'EGLL'
ORDER BY obs_ts DESC;

-- Aggregate from the relational table (post-INSERT)
SELECT station_id,
       COUNT(*)              AS obs_count,
       AVG(temperature_c)   AS avg_temp_c,
       MAX(wind_speed_kts)  AS max_wind_kts,
       MIN(pressure_hpa)    AS min_pressure
FROM demo_db.weather_obs
GROUP BY station_id
ORDER BY station_id;

-- IFR / LIFR conditions (low visibility) across all stations
SELECT station_id, observation_ts, visibility_m, conditions
FROM demo_db.weather_obs
WHERE conditions IN ('IFR', 'LIFR')
ORDER BY observation_ts DESC;
```

---

## Further reading

- Teradata DATASET Data Type — B035-1198 (Avro Object Container File loading)
- Teradata Parallel Transporter Reference Guide — B035-2436
- TPT Kafka Access Module — B035-2447 (Access Module Reference)
- Teradata Native Object Store Getting Started Guide — B035-2198
