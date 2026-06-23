# Demo 1A: Avro BLOB → Teradata DATASET column (TPT variant)

**Topic:** The same Avro container file story as [Demo 1](demo01-avro-blob.md) — load two Avro files as BLOBs, split with `AvroContainerSplit`, query with dot notation — but the BLOB staging step uses **TPT DataConnector + STREAM** instead of BTEQ DEFERRED BY NAME.

The two demos produce identical output. The difference is the loader: BTEQ is an interactive SQL client that handles one file per session; TPT is a parallel ETL framework that loads N files in a single restartable job.

## What it demonstrates

- **TPT STREAM is the only high-throughput TPT operator that supports BLOB/CLOB columns** — LOAD (FastLoad) and MLoad both reject LOB columns at job start; STREAM accepts them
- **`BLOB(size) AS DEFERRED BY NAME` schema type** — the manifest column value is a file path; DataConnector reads the binary bytes at that path and passes them to STREAM; this is the TPT equivalent of BTEQ's `AS DEFERRED BY NAME`. **Important: this is not the same as `BLOB BY NAME`** (see syntax notes below)
- **Manifest-based multi-file loading** — a single TPT job processes a pipe-delimited manifest of N files; Demo 1 required two separate BTEQ sessions (one per file); TPT scales to thousands of files with no code changes
- **TPT job infrastructure** — explicit `LogTable` and `ErrorTable`; checkpoint/restart via `PRESERVE_RESTART_INFO`; formal job definition vs BTEQ's imperative script

## How to run

```bash
bash demos/01a-avro-blob-tpt/run.sh
```

## Files

| File | Purpose |
|------|---------|
| [kafka/producers/generate_product_avro.py](../kafka/producers/generate_product_avro.py) | Generates `.avro` files + BTEQ index files (Demo 1) + TPT manifest (Demo 1A) |
| [tpt/tbuild/avro_blob_load.tbuild](../tpt/tbuild/avro_blob_load.tbuild) | TPT job: DataConnector `BLOB(size) AS DEFERRED BY NAME` → STREAM → `avro_product_stage` |
| [tpt/scripts/demo01a/prepare.bteq](../tpt/scripts/demo01a/prepare.bteq) | Truncates staging tables + drops TPT work tables before each run |
| [tpt/scripts/demo01/decode.bteq](../tpt/scripts/demo01/decode.bteq) | AvroContainerSplit INSERT-SELECT + verification queries (shared with Demo 1) |
| [demos/01a-avro-blob-tpt/run.sh](../demos/01a-avro-blob-tpt/run.sh) | Orchestration |

## How it works

```
1. Generate
   └─ fastavro writes product_v1.avro (30 records, 5 fields)
   └─ fastavro writes product_v2.avro (30 records, 8 fields)
   └─ avro_manifest.txt written:
        1|/tpt/data/sample/product_v1.avro
        2|/tpt/data/sample/product_v2.avro

2. Prepare
   └─ DELETE FROM avro_product_stage ALL
   └─ DELETE FROM avro_product ALL
   └─ DROP TABLE avro_blob_stream_log_tbl / avro_blob_stream_err_tbl (clean job state)

3. TPT STREAM load (avro_blob_load.tbuild)
   └─ twbrmcp ttuuser: clears checkpoint so tbuild is a fresh job, not a restart
   └─ DataConnector PRODUCER reads avro_manifest.txt
        Column 1 (stage_id VARCHAR(10)):     "1" or "2"
        Column 2 (container BLOB AS DEFERRED BY NAME):
          DataConnector opens each file path, reads all bytes, passes as BLOB
   └─ STREAM operator INSERTs both rows into avro_product_stage in one job
        CAST(:stage_id AS INTEGER) → stage_id column
        :container                 → container BLOB column
   └─ Two rows: stage_id=1 (v1, ~3 KB) and stage_id=2 (v2, ~5 KB)

4. AvroContainerSplit (shared with Demo 1)
   └─ FROM AvroContainerSplit(ON (SELECT stage_id, container FROM avro_product_stage))
   └─ INSERT INTO avro_product: 30 rows from stage_id=1, 30 from stage_id=2

5. Verify
   └─ Row counts per container_id
   └─ AVRO_CHECK — all rows valid
   └─ Schema evolution: v1 rows return NULL for description, subcategory, discount_pct
```

## Critical TPT syntax note — `BLOB(size) AS DEFERRED BY NAME`

This syntax was determined by testing, not assumed. Three approaches were tried:

| Syntax tried | Result | Reason |
|---|---|---|
| `container BLOB BY NAME` | **Compile error** — "At 'BY' missing RPAREN_" | `BLOB BY NAME` is BTEQ notation; TPT schema parser does not recognise it |
| `container BLOB` + `LobCols='container'` attribute | **Runtime error** TPT19108 — DELIMITED format rejects plain `BLOB` | DataConnector DELIMITED requires all columns to be VARCHAR or a `BY NAME` LOB type |
| `container BLOB(10000000) AS DEFERRED BY NAME` | **Works** | Correct TPT schema syntax; confirmed by Teradata-supplied sample PTS00025 |

The correct syntax source is the Teradata-installed TPT sample at  
`/opt/teradata/client/20.00/tbuild/sample/userguide/PTS00025`.

## BTEQ vs TPT comparison

| Attribute | Demo 1 — BTEQ DEFERRED BY NAME | Demo 1A — TPT STREAM + DataConnector |
|---|---|---|
| Files per invocation | 1 (one BTEQ session per file) | N (all files in manifest, one job) |
| Sessions | 1 | Configurable; 1 for small files |
| BLOB support | Yes | Yes (STREAM only among high-throughput operators) |
| Restart / checkpoint | No | Yes (`PRESERVE_RESTART_INFO` auto-set by STREAM) |
| Error tables | Yes (defined in tbuild) | Yes |
| Job definition | Imperative `.IMPORT` in BTEQ script | Declarative TPT job file (`.tbuild`) |
| Checkpoint must be cleared before re-run | N/A | Yes — `twbrmcp ttuuser` clears it; otherwise tbuild treats next run as a restart |
| Typical use case | Interactive, ad-hoc, 1–2 files | Automated, bulk, SLA-bound, N files |

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
-- After Demo 1A runs, these are identical to Demo 1:

-- Records per container (expect 30 each)
SELECT container_id, COUNT(*) AS records FROM demo_db.avro_product GROUP BY 1 ORDER BY 1;

-- Validate all rows
SELECT AVRO_CHECK(avro) AS validity, COUNT(*) AS cnt FROM demo_db.avro_product GROUP BY 1;

-- Schema evolution: v1 rows return NULL for the three v2-only fields
SELECT TOP 5
  container_id                  AS schema_version,
  avro.."product_id"            AS product_id,
  avro.."price"                 AS price,
  avro.."description"           AS description,
  avro.."discount_pct"          AS discount_pct
FROM demo_db.avro_product ORDER BY 1, 2;
```
