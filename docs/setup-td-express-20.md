# Teradata Express v20 – Enabling Open Table Format (OTF) Support

## Overview

This guide describes how to configure [**Teradata Express (TDExpress v20)**](https://downloads.teradata.com/download/database/teradata-express/vmware) to support **Open Table Formats (OTF)**, including:

* Apache Iceberg (read/write)
* Delta Lake (read/write)
* NOS-based access (MinIO / S3-compatible storage)

## Prerequisites

* TDExpress v20 running (VMware-based) — tested on Teradata v20.00.28.81
* Root or sudo access to the node
* Network connectivity to object storage (e.g., MinIO or S3)
* DNS working (or manually configured — see [DNS section](#configure-dns-if-required))

---

## Configuration Reference

All DBS Control flags are set via `dbscontrol`. The sections below explain every flag, why it is needed, and what happens if it is left at its default value.

### OTF Feature Flags

| Flag | Category | Name | Value | Default |
|------|----------|------|-------|---------|
| 732 | Internal | JavaOTFFlags | 0 | — |
| 733 | Internal | NativeOTFFlags | 1 | — |
| 282 | NOS | DisableMOTF | FALSE | TRUE |
| 245 | Internal | ColumnarPurchased | TRUE | FALSE |

**Flag 732 — JavaOTFFlags = 0**
Enables the Java-based Open Table Format engine. Setting this to `0` (all sub-bits active) turns on both Iceberg read/write and Delta Lake read/write. The flag is a bitmask: bit 0 = disable Iceberg reads, bit 1 = disable Iceberg writes, bit 2 = disable Delta reads, bit 3 = disable Delta writes (value 15 = everything off). Without this flag, queries against `DATALAKE` objects — the OTF access mechanism — will fail at the engine level. Note: Iceberg tables are accessed via `CREATE DATALAKE` + 3-level dot notation (`my_datalake.my_db.my_table`), not via `CREATE FOREIGN TABLE`.

**Flag 733 — NativeOTFFlags = 1**
Enables the native (C++) OTF reader used for Iceberg scan operations. Setting to `0` activates Iceberg read support in the native execution path. This flag works alongside 732; disabling it forces all reads through the slower Java path.  On TDExpress v20.00.28.x, the native OTF reader is not fully supported; it is recommended to leave this at `1` (disabled) for now. Future releases will improve native OTF support.

**Flag 282 — DisableMOTF = FALSE**
Controls Managed Open Table Format — the layer that handles OTF metadata and write coordination. `FALSE` means "do NOT disable MOTF", i.e., MOTF is active. Without this, Iceberg write operations (INSERT, UPDATE, DELETE on DATALAKE tables) are rejected. Note: MERGE/UPSERT are not supported on OTF tables regardless of this flag.

**Flag 245 — ColumnarPurchased = TRUE**
Declares that the platform has columnar capability licensed and enabled. TDExpress ships with this `FALSE`. Iceberg tables (accessed via DATALAKE) and partitioned NOS foreign tables (Parquet format) use columnar storage internally; if this flag is `FALSE`, attempts to use these will fail with a licensing error. This is the most common reason OTF fails on TDExpress.

---

### NOS / Object Storage Flags

| Flag | Category | Name | Value | Default |
|------|----------|------|-------|---------|
| 101 | NOS | NOS HTTPS disable | TRUE | FALSE |
| 134 | NOS | AllowToForceS3pathstyle | TRUE | FALSE |
| 198 | NOS | EnableOFSSpool | FALSE | TRUE |
| 199 | NOS | DBSOFSSpoolThr | 10 | — |
| 205 | NOS | DisavlePartOFSSpool | 0 | — |
| 256 | NOS | EnablePHBRAggr | TRUE | FALSE |

**Flag 101 — NOS HTTPS disable = TRUE**
Forces NOS to use plain HTTP when connecting to object storage. MinIO in a lab/Docker environment typically runs on HTTP (port 9000) without TLS. Leaving this `FALSE` causes every NOS read/write to attempt an HTTPS handshake that will time out or be refused.

**Flag 134 — AllowToForceS3pathstyle = TRUE**
Allows NOS to use path-style S3 URLs (`http://host:port/bucket/key`) instead of virtual-hosted style (`http://bucket.host/key`). MinIO requires path-style addressing. Without this, bucket references in `LOCATION` clauses resolve incorrectly and return 403/404 errors.

**Flag 198 — EnableOFSSpool = FALSE**
Disables writing OTF intermediate results to OFS (Object File System) spool. Keeping OFS spool disabled avoids unexpected I/O to object storage during query planning and reduces latency for short queries. Set to `FALSE` for lab/demo environments.

**Flag 199 — DBSOFSSpoolThr = 10**
Threshold (in MB) at which OFS spool would be triggered if it were enabled. Setting this to `10` ensures that even if OFS spool is inadvertently re-enabled, only very large intermediate results are spooled, not routine query data.

**Flag 205 — DisavlePartOFSSpool = 0**
Controls partition-level OFS spooling. Value `0` disables partition-based OFS spool. Works in conjunction with flag 198 to fully suppress OFS spool usage during partitioned scans.

**Flag 256 — EnablePHBRAggr = TRUE**
Enables Partial Hash-Based Row Aggregation for NOS queries. Activating this flag allows the optimizer to push aggregation steps closer to the scan, reducing the volume of data transported through the pipeline before grouping. Significant throughput improvement for NOS aggregation queries.

---

### Optimizer / Performance Flags

These flags tune the internal query planner and pipeline execution for workloads that scan external (OTF/NOS) tables. The defaults are set for traditional relational workloads and perform poorly on wide columnar scans.

| Flag | Category | Name | Value |
|------|----------|------|-------|
| 47 | Internal | HTMemAllocBase | 128 |
| 54 | Internal | SynDiagFlags (Decimals) | 3=55 |
| 403 | Internal | EnableInMem | TRUE |
| 404 | Internal | InMemHTSize | 256 |
| 502 | Internal | DupedToAROAHTLimit | 256 |
| 582 | Internal | DisableIMHashOuterJoin | FALSE |
| 685 | Internal | SemiHashJoinRewrite | 2 |
| 693 | Internal | IMHJMaxNumHashBits | 23 |
| 700 | Internal | BFMaxNumHashBits | 30 |
| 713 | Internal | PipelineFunctionalityControl | 0 |
| 720 | Internal | DisableNewCVE | FALSE |
| 746 | Internal | EnableLargeAggrCache | TRUE |
| 763 | Internal | EnablePartitionAggregation | TRUE |
| 788 | Internal | CgiGenSumStep | 2 |
| 43 | Performance | PipelineLimit4Block | 8 |
| 44 | Performance | PipelineMemLimit4Block | 2048 |
| 45 | Performance | PipelineChanLimit4Block | 64 |
| 49 | Performance | PipelineSpoolThreshold | 1024 |
| 50 | Performance | PipelineFavMP2SPHJ | 0 |
| 57 | Performance | PipelineMultiHashDupControl | 0 |
| 58 | Performance | PipelineJoin2SumIpeControl | 1 |

**Flag 47 — HTMemAllocBase = 128**
Sets the base memory allocation (MB) for hash table operations. Increasing from the default improves performance for hash joins and aggregations that arise in OTF/NOS scans with many distinct keys.

**Flag 54 — SynDiagFlags (Decimals) = 3=55**
Enables a specific set of synopsis diagnostic bits (sub-field 3, value 55). These bits activate additional planner statistics collection paths used during external table optimization, giving the optimizer better cardinality estimates for foreign table scans.

**Flag 403 — EnableInMem = TRUE**
Enables in-memory hash table execution. When active, the engine builds hash tables in memory rather than spooling to disk, dramatically reducing I/O for join-heavy OTF queries. Required for the InMemHTSize flag (404) to have any effect.

**Flag 404 — InMemHTSize = 256**
Maximum size (MB) of an in-memory hash table per AMP. Set to 256 MB to give each AMP enough headroom for joins against typical dimension tables when scanning large Iceberg fact tables.

**Flag 502 — DupedToAROAHTLimit = 256**
Controls when the optimizer switches from a duplication-based join to an all-rows-on-all-AMPs hash join. Setting to 256 encourages the optimizer to use the more efficient AROAHT strategy for medium-to-large foreign table joins.

**Flag 582 — DisableIMHashOuterJoin = FALSE**
`FALSE` means in-memory hash outer joins are **enabled**. This is needed for LEFT/RIGHT OUTER JOIN queries against Iceberg/NOS tables to use the in-memory path established by flag 403, rather than falling back to spool-based execution.

**Flag 685 — SemiHashJoinRewrite = 2**
Controls how aggressively the optimizer rewrites semi-joins (EXISTS / IN subqueries). Value `2` enables the most aggressive rewrite, converting correlated subqueries into hash semi-joins — important for analytic queries over Iceberg that filter on subquery results.

**Flag 693 — IMHJMaxNumHashBits = 23**
Sets the maximum number of hash bits (i.e., maximum hash table partitions = 2^23) for in-memory hash joins. Higher values reduce collision rates for large hash tables at the cost of slightly more memory overhead. 23 is the recommended value for OTF workloads.

**Flag 700 — BFMaxNumHashBits = 30**
Maximum hash bits for Bloom filter operations used in partition pruning. Setting to 30 allows finer-grained Bloom filters, improving partition elimination during Iceberg scans and reducing unnecessary data read from object storage.

**Flag 713 — PipelineFunctionalityControl = 0**
Enables all pipeline execution features (value `0` = no restrictions). Pipelined execution is critical for streaming data through OTF scan → transform → aggregate chains without materialising intermediate results to spool.

**Flag 720 — DisableNewCVE = FALSE**
`FALSE` means the new Compile-time Value Estimation is **enabled**. CVE improves cardinality estimates at plan compilation time, leading to better join order and parallelism decisions for queries involving foreign tables with limited local statistics.

**Flag 746 — EnableLargeAggrCache = TRUE**
Activates a larger aggregation result cache. Beneficial for GROUP BY queries over Iceberg/NOS data where many partial aggregates are produced by each AMP before the final merge step.

**Flag 763 — EnablePartitionAggregation = TRUE**
Allows the optimizer to push aggregation inside partition scans. For partitioned Iceberg tables, this means each partition's data is pre-aggregated before being sent across AMPs, reducing network traffic significantly.

**Flag 788 — CgiGenSumStep = 2**
Enables LLVM Code Generation for SUM step operations (value `2` = LLVM path active). LLVM-compiled SUM steps run substantially faster than interpreted code for wide-column aggregations common in OTF analytics.

**Flag 43 — PipelineLimit4Block = 8**
Maximum number of pipeline stages per block. Set to `8` to allow deeper pipelines for multi-step OTF transformations without forcing intermediate spool writes.

**Flag 44 — PipelineMemLimit4Block = 2048**
Memory limit (MB) per pipeline block. `2048` MB per block prevents OOM errors during large Iceberg scans that buffer columnar data in pipeline stages.

**Flag 45 — PipelineChanLimit4Block = 64**
Maximum parallel channels per pipeline block. `64` channels enables fine-grained parallelism across AMPs when reading from partitioned Iceberg tables.

**Flag 49 — PipelineSpoolThreshold = 1024**
Size threshold (MB) above which pipeline data is spooled to disk rather than kept in memory. `1024` MB keeps most OTF intermediate results in memory for lab-scale data volumes.

**Flag 50 — PipelineFavMP2SPHJ = 0**
Disables the preference for MP2SPHJ (Multi-Partition 2-Step Physical Hash Join) in the pipeline. Value `0` allows the optimizer to freely choose the best join strategy for each OTF query rather than being biased toward MP2SPHJ.

**Flag 57 — PipelineMultiHashDupControl = 0**
Controls duplicate elimination in multi-hash pipeline steps. `0` disables overly aggressive deduplication that can incorrectly eliminate rows during OTF partition scans.

**Flag 58 — PipelineJoin2SumIpeControl = 1**
Enables the join-to-sum IPE (In-Pipeline Execution) optimisation. When active, qualifying join results are summed inside the pipeline stage rather than materialised first, reducing memory pressure for aggregated OTF joins.

---

## Setup Script

Run this script **as root on the Teradata node** after completing the OTF component installation. It applies all flags in a single `dbscontrol` session and then restarts the database.

### Step 1 — Install OTF Components

```bash
cd /opt/teradata/tdotf/lib/scripts

./install.sh \
  --teradata_host dbccop1 \
  --db_admin_user dbc \
  --db_admin_pass dbc
```

### Step 2 — Apply All DBS Control Flags

```bash
dbscontrol

# OTF feature flags
modify internal 732=0
modify internal 733=1
modify nos 282=FALSE
modify internal 245=TRUE

# NOS / object storage flags
modify nos 101=TRUE
modify nos 134=TRUE
modify nos 198=FALSE
modify nos 199=10
modify nos 205=0
modify nos 256=TRUE

# Optimizer flags
modify internal 47=128
modify internal 54 3=55
modify internal 403=TRUE
modify internal 404=256
modify internal 415=2
modify internal 502=256
modify internal 582=FALSE
modify internal 685=2
modify internal 693=23
modify internal 694=1
modify internal 700=30
modify internal 713=0
modify internal 720=FALSE
modify internal 746=TRUE
modify internal 763=TRUE
modify internal 788=2

# Performance / pipeline flags
modify performance 43=8
modify performance 44=2048
modify performance 45=64
modify performance 49=1024
modify performance 50=0
modify performance 57=0
modify performance 58=1

write
quit
```

### Step 3 — Restart Teradata

```bash
tpareset -f now
```

### Step 4 — Configure DNS (if required)

If NOS cannot resolve object storage hostnames, add public nameservers:

```bash
cat >> /etc/resolv.conf <<'EOF'
nameserver 8.8.8.8
nameserver 1.1.1.1
EOF
```

---

## Validation

### Verify OTF flags

```bash
dbscontrol -i | grep -E "OTF|MOTF"
```

### Verify columnar enabled

```bash
dbscontrol -i | grep Columnar
```

Expected output:

```
ColumnarPurchased = TRUE
ColumnarTable is enabled
```

### Test NOS raw-file connectivity (CSV/Parquet via READ_NOS)

```sql
SELECT TOP 1 * FROM (
  LOCATION = 's3://your-bucket/test/'
  STOREDAS PARQUET
  AUTHORIZATION = TD_Authorization_Object
) AS t;
```

### Test OTF / Iceberg connectivity (via DATALAKE)

Iceberg tables are accessed through a `DATALAKE` object using 3-level dot notation. First create the DATALAKE (see your catalog setup), then validate:

```sql
-- Inspect the registered DATALAKE
SHOW DATALAKE my_iceberg_lake;

-- List databases inside the catalog
HELP DATALAKE my_iceberg_lake;

-- List tables in a database
HELP DATABASE my_iceberg_lake.my_db;

-- Query an Iceberg table
SELECT TOP 5 * FROM my_iceberg_lake.my_db.my_table;
```

---

## Diagnostics

These checks isolate the most common failure modes before and after applying the setup script. Run them as `dbc` in BTEQ or SQL Assistant.

### 1 — Verify OTF functions are installed in TD_OTFDB

```sql
SELECT FunctionName, DatabaseName
FROM   DBC.FunctionsV
WHERE  DatabaseName = 'TD_OTFDB'
ORDER BY FunctionName;
```

Expected: **9 rows** — TD_OTF_BeginCommit, TD_OTF_CommitStagingData, TD_OTF_GetDataFiles, TD_OTF_GetDeleteFiles, TD_OTF_GetManifest, TD_OTF_GetMetadata, TD_OTF_GetPartitions, TD_OTF_GetStagingInfo, TD_OTF_RollbackStaging.

If fewer than 9 rows, or zero rows, `install.sh` did not complete successfully. Re-run it and check `/opt/teradata/tdotf/lib/scripts/install.log` for errors (look for `TD_OTF_USER logon failed`).

> **Note**: HELP TABLE, HELP DATALAKE, and TD_SNAPSHOTS all operate on catalog metadata and succeed even if the OTF functions are absent or partially installed. A `SELECT` that reads actual Parquet data always goes through the Java OTF engine (`TD_OTF_GetDataFiles` → `TD_OTF_GetManifest`). Error 6301 during SELECT while metadata queries succeed is the canonical symptom of missing or disabled OTF functions.

### 2 — Restart the Java OTF server (if SELECT still fails after flag + function checks)

The JVM process may be in a bad state after dbscontrol changes or a partial restart. Cycle it without a full `tpareset`:

```sql
DECLARE a VARCHAR(200);
CALL SQLJ.ServerControl('JAVAOTF', 'disable',  a);
CALL SQLJ.ServerControl('JAVAOTF', 'shutdown', a);
CALL SQLJ.ServerControl('JAVAOTF', 'status',   a);  -- wait until result shows INACTIVE
CALL SQLJ.ServerControl('JAVAOTF', 'enable',   a);
```

The JVM will restart automatically on the next OTF query. Run a simple `SELECT COUNT(*) FROM my_lake.my_db.my_table;` to trigger the restart and confirm the error clears.

### 3 — Check install.sh log for silent failures

```bash
tail -50 /opt/teradata/tdotf/lib/scripts/install.log
```

Look for: `TD_OTF_USER logon failed` (credentials wrong), `object already exists` (ok — re-run is safe), or Java exception stack traces (indicate a real installation failure).

---

## Common Issues

| Symptom | Root Cause | Fix |
|---------|-----------|-----|
| Error 6301 on `SELECT FROM datalake.db.tbl` while HELP/TD_SNAPSHOTS work | OTF Java engine disabled or functions not installed | Verify 9 functions in TD_OTFDB (Diagnostic 1); cycle the JVM (Diagnostic 2); check install.log (Diagnostic 3) |
| Cannot create partitioned NOS/Iceberg table | Columnar disabled | Enable flag 245 |
| S3 access fails with TLS error | HTTPS not disabled | Set flag 101 = TRUE |
| MinIO returns 403/404 | Path-style not enabled | Set flag 134 = TRUE |
| Iceberg INSERT/UPDATE/DELETE rejected | MOTF disabled | Set flag 282 = FALSE |
| DATALAKE queries fail at engine level | JavaOTFFlags / NativeOTFFlags not set | Set flags 732 = 0 and 733 = 1 (see Step 2) |
| `[Error 5589] TD_ICEBERG_READ does not exist` | OTF install incomplete | Re-run `install.sh`; check install.log |
| Error 7583 — JVM unresponsive | OTF JVM hung | Use SQLJ.ServerControl cycle (Diagnostic 3) |
| `install.sh` fails with TD_OTF_USER logon error | B/G upgrade or re-run with stale credentials | Check install.log; ensure `dbc` password is correct and TD is reachable from node |
| Poor NOS/OTF query performance | Optimizer flags missing | Apply full optimizer block |
