# Demo 1: Avro BLOB → Teradata DATASET column

**Topic:** Loading Avro container files into a Teradata `DATASET STORAGE FORMAT AVRO` column, with schema evolution across two file versions.

## What it demonstrates

- The correct Teradata workflow for loading Avro Object Container Files (`.avro`) — direct `CAST(VARBYTE → DATASET AVRO)` is **not** supported; the documented path uses BTEQ `DEFERRED BY NAME` staging followed by `AvroContainerSplit`
- **Schema evolution** without ETL rewrites: v1 records (5 fields) and v2 records (8 fields) coexist in the same `DATASET AVRO` column; querying a v2 field on a v1 row returns `NULL`
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
| [kafka/producers/generate_product_avro.py](../kafka/producers/generate_product_avro.py) | Generates `product_v1.avro` / `product_v2.avro` + BTEQ index files |
| [tpt/scripts/infra/setup_demo_tables.bteq](../tpt/scripts/infra/setup_demo_tables.bteq) | DDL for `avro_product_stage` (BLOB) and `avro_product` (DATASET AVRO) |
| [tpt/scripts/demo01/prepare.bteq](../tpt/scripts/demo01/prepare.bteq) | Truncates both tables before each run |
| [tpt/scripts/demo01/load_v1.bteq](../tpt/scripts/demo01/load_v1.bteq) | BTEQ DEFERRED BY NAME: loads `product_v1.avro` as a single BLOB row |
| [tpt/scripts/demo01/load_v2.bteq](../tpt/scripts/demo01/load_v2.bteq) | Same for `product_v2.avro` |
| [tpt/scripts/demo01/decode.bteq](../tpt/scripts/demo01/decode.bteq) | AvroContainerSplit INSERT-SELECT + verification queries |
| [demos/01-avro-blob/run.sh](../demos/01-avro-blob/run.sh) | Orchestrates all four steps end-to-end |

## What to expect

```
══════════════════════════════════════════════
  Demo 1: Avro BLOB → Teradata DATASET column
══════════════════════════════════════════════

── 1/4  Generating Avro container files
Written 30 records → data/sample/product_v1.avro
Written 30 records → data/sample/product_v2.avro

── 2/4  Clearing staging tables

── 3/4  Loading Avro files into Teradata (BTEQ DEFERRED BY NAME)
      → product_v1.avro (schema v1: 5 fields)
      *** Insert completed. One row added.
      → product_v2.avro (schema v2: 8 fields)
      *** Insert completed. One row added.

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
- No `"namespace"` field — a namespace causes fastavro to write a fully-qualified dotted name which the parser cannot handle
- `logicalType` must be nested inside the type object: `{"type": "long", "logicalType": "timestamp-millis"}` — not at field level

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
