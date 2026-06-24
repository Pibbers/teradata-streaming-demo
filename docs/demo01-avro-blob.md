# Demo 1: Avro BLOB → Teradata DATASET column

**Topic:** Loading Avro container files into a Teradata `DATASET STORAGE FORMAT AVRO` column using TPT DataConnector + STREAM, with schema evolution across two file versions.

## What it demonstrates

- **TPT STREAM is the only high-throughput TPT operator that supports BLOB/CLOB columns** — LOAD (FastLoad) and MLoad both reject LOB columns at job start; STREAM accepts them
- **`BLOB(size) AS DEFERRED BY NAME` schema type** — the manifest column value is a file path; DataConnector reads the binary bytes at that path and passes them to STREAM. This is the TPT equivalent of BTEQ's `AS DEFERRED BY NAME`. **Important: this is not the same as `BLOB BY NAME`** (see syntax notes below)
- **Manifest-based multi-file loading** — a single TPT job processes a pipe-delimited manifest of N files; both container files are loaded in one job, scaling to thousands of files with no code changes
- **Schema evolution without ETL rewrites**: v1 records (5 fields) and v2 records (8 fields) coexist in the same `DATASET AVRO` column; querying a v2 field on a v1 row returns `NULL`
- Dot-notation access (`avro.."field_name"`) on `DATASET` columns
- `AVRO_CHECK()` for inline validity verification

## How to run

```bash
# From project root
bash demos/01-avro-blob/run.sh
```

## Files

| File | Purpose |
|------|---------|
| [kafka/schemas/product_v1.avsc](../kafka/schemas/product_v1.avsc) | Avro schema: 5 fields (product_id, name, price, category, ts) |
| [kafka/schemas/product_v2.avsc](../kafka/schemas/product_v2.avsc) | Evolved schema: adds description, subcategory, discount_pct (nullable) |
| [kafka/producers/generate_product_avro.py](../kafka/producers/generate_product_avro.py) | Generates `product_v1.avro` / `product_v2.avro` + TPT manifest |
| [tpt/scripts/infra/setup_demo_tables.bteq](../tpt/scripts/infra/setup_demo_tables.bteq) | DDL for `avro_product_stage` (BLOB) and `avro_product` (DATASET AVRO) |
| [tpt/scripts/demo01/prepare.bteq](../tpt/scripts/demo01/prepare.bteq) | Truncates staging tables and drops TPT work tables before each run |
| [tpt/scripts/demo01/decode.bteq](../tpt/scripts/demo01/decode.bteq) | AvroContainerSplit INSERT-SELECT + verification queries |
| [tpt/tbuild/avro_blob_load.tbuild](../tpt/tbuild/avro_blob_load.tbuild) | TPT job: DataConnector `BLOB(size) AS DEFERRED BY NAME` → STREAM → `avro_product_stage` |
| [demos/01-avro-blob/run.sh](../demos/01-avro-blob/run.sh) | Orchestrates all four steps end-to-end |

## What to expect

```
======================================================
  Demo 1: Avro BLOB → Teradata DATASET column
======================================================

── 1/4  Generating Avro container files + TPT manifest
Written 30 records → data/sample/product_v1.avro
Written 30 records → data/sample/product_v2.avro
Written TPT manifest → data/sample/avro_manifest.txt  (2 files)

── 2/4  Clearing staging tables and TPT work tables

── 3/4  Loading Avro files into Teradata (TPT STREAM, BLOB AS DEFERRED BY NAME)
      manifest: /tpt/data/sample/avro_manifest.txt (2 container files)
      Job ttuuser completed successfully
      stage_id 1 = product_v1.avro (schema v1: 5 fields)
      stage_id 2 = product_v2.avro (schema v2: 8 fields)

── 4/4  Decoding BLOBs into DATASET column and verifying

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
  ver   product_id       price   description   discount_pct
  ---   ------------     ------  -----------   ------------
    1   08a63ba7-...     19.99   (null)        (null)
    2   f3a1c204-...    149.99   Premium...    12.5
```

## How it works

```
1. Generate Avro container files (.avro) + manifest
   └─ fastavro writes binary Avro Object Container File format
   └─ Schema: no namespace (avoids dotted record names that break Teradata's Avro 1.7.7 parser)
   └─ avro_manifest.txt: pipe-delimited list of stage_id|file_path

2. TPT STREAM load (avro_blob_load.tbuild)
   └─ twbrmcp ttuuser: clears checkpoint so tbuild is a fresh job, not a restart
   └─ DataConnector PRODUCER reads avro_manifest.txt
        Column 1 (stage_id VARCHAR(10)):     "1" or "2"
        Column 2 (container BLOB AS DEFERRED BY NAME):
          DataConnector opens each file path, reads all bytes, passes as BLOB
   └─ STREAM operator INSERTs both rows into avro_product_stage in one job
   └─ Two rows: stage_id=1 (v1, ~3 KB) and stage_id=2 (v2, ~5 KB)

3. Split with AvroContainerSplit
   └─ FROM AvroContainerSplit(ON (SELECT stage_id, container FROM avro_product_stage))
   └─ INSERT INTO avro_product: 30 rows from stage_id=1, 30 from stage_id=2

4. Query with dot notation
   └─ avro.."field_name"  — recursive field access
   └─ AVRO_CHECK(avro)    — validate individual rows
```

**Schema design rules** (required for Teradata's Avro 1.7.7 parser):
- No `"namespace"` field — a namespace causes fastavro to write a fully-qualified dotted name which the parser cannot handle
- `logicalType` must be nested inside the type object: `{"type": "long", "logicalType": "timestamp-millis"}` — not at field level

## Critical TPT syntax note — `BLOB(size) AS DEFERRED BY NAME`

This syntax was determined by testing, not assumed. Three approaches were tried:

| Syntax tried | Result | Reason |
|---|---|---|
| `container BLOB BY NAME` | **Compile error** — "At 'BY' missing RPAREN_" | `BLOB BY NAME` is BTEQ notation; TPT schema parser does not recognise it |
| `container BLOB` + `LobCols='container'` attribute | **Runtime error** TPT19108 — DELIMITED format rejects plain `BLOB` | DataConnector DELIMITED requires all columns to be VARCHAR or a `BY NAME` LOB type |
| `container BLOB(10000000) AS DEFERRED BY NAME` | **Works** | Correct TPT schema syntax; confirmed by Teradata-supplied sample PTS00025 |

The correct syntax source is the Teradata-installed TPT sample at  
`/opt/teradata/client/20.00/tbuild/sample/userguide/PTS00025`.

## Tables

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

## Manifest format

```
stage_id|container_path
1|/tpt/data/sample/product_v1.avro
2|/tpt/data/sample/product_v2.avro
```

- Pipe (`|`) delimiter matches `TextDelimiter = '|'` in the tbuild script
- `stage_id` must be unique — it maps to the PRIMARY INDEX of `avro_product_stage`
- `stage_id` is read as VARCHAR in the schema (DELIMITED format rejects INTEGER) and `CAST` to INTEGER in the INSERT
- Container paths are paths **inside the tpt container** (`/tpt/...` bind-mount)

## TPT job design notes

### Why STREAM and not LOAD?

`LOAD` (FastLoad) explicitly rejects BLOB, CLOB, and DATASET columns. `STREAM` uses the Teradata Protocol Interface and accepts all column types. For two small Avro files the performance difference is irrelevant; for bulk LOB loading at scale, STREAM is the right choice.

### LogTable and ErrorTable

```
VARCHAR LogTable   = '$(TD_DATABASE).avro_blob_stream_log_tbl',
VARCHAR ErrorTable = '$(TD_DATABASE).avro_blob_stream_err_tbl'
```

The prepare step drops these before each run so `tbuild` can recreate them fresh. Without explicit names, STREAM defaults to `DBC.`-prefixed names which require DBC privileges.

### Checkpoint and re-runs

STREAM automatically writes a checkpoint file (`ttuuserLVCP`). On the next `tbuild` invocation, it detects the file and treats the run as a restart of the previous job — loading nothing if the previous job completed. The run.sh calls `twbrmcp ttuuser` before each tbuild to clear this, ensuring every run starts fresh.

### Scaling to many files

For a production scenario (e.g., 500 nightly Avro dumps):
1. Build the manifest: `find /output -name "*.avro" | awk '{print NR"|"$0}' > manifest.txt`
2. Tune `MaxSessions` in the tbuild STREAM operator to match AMP count (8–32 typical)
3. TPT distributes file loading across sessions automatically

## Sample queries

```sql
-- All records with dot-notation field access
SELECT
  container_id                AS schema_version,
  avro.."product_id"          AS product_id,
  avro.."name"                AS name,
  avro.."price"               AS price,
  avro.."description"         AS description,   -- NULL for v1 rows
  avro.."discount_pct"        AS discount_pct   -- NULL for v1 rows
FROM demo_db.avro_product
ORDER BY 1, 2;

-- Validate all rows
SELECT AVRO_CHECK(avro), COUNT(*) FROM demo_db.avro_product GROUP BY 1;

-- Records above a price threshold
SELECT avro.."product_id", avro.."price", avro.."discount_pct"
FROM demo_db.avro_product
WHERE avro.."discount_pct" > 20.0;
```
