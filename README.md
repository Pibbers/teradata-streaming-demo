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

## Further reading

- Teradata DATASET Data Type — B035-1198 (Avro Object Container File loading)
- Teradata Parallel Transporter Reference Guide — B035-2436
- TPT Kafka Access Module documentation
