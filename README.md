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

## Further reading

- Teradata DATASET Data Type — B035-1198 (Avro Object Container File loading)
- Teradata Parallel Transporter Reference Guide — B035-2436
- TPT Kafka Access Module — B035-2447 (Access Module Reference)
