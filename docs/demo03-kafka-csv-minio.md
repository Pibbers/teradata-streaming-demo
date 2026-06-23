# Demo 3: Kafka → Kafka Connect → MinIO CSV → NOS

**Topic:** Streaming weather observations through a Kafka Connect S3 Sink into plain-date MinIO folders, then querying and incrementally loading the data via Teradata Native Object Store (NOS) with typed partition pruning.

## What it demonstrates

- Kafka Connect S3 Sink (`ByteArrayFormat`, `flush.size=1`, `TimeBasedPartitioner`) landing one CSV file per Kafka message under a plain time-partitioned path: `YYYY-MM-DD/HH/`
- `READ_NOS` with `RETURNTYPE('NOSREAD_KEYS')` to enumerate exactly which partition folders and files Kafka Connect wrote
- `CREATE FOREIGN TABLE` with `PATHPATTERN` enabling NOS **partition pruning** — 10–100× faster on large datasets because object listing and fetching are skipped for non-matching partitions
- **Incremental load pattern**: the FOREIGN TABLE covers the full MinIO path (all history); each run's `INSERT INTO weather_obs` uses a scoped LOCATION pointing at the current hour's folder
- `--fresh` flag for a full clean-slate reset (purges MinIO + clears all `weather_obs` rows)

## How to run

```bash
# Accumulates this hour's data into weather_obs
bash demos/03-kafka-csv-minio/run.sh

# Full reset: purge MinIO + clear all weather_obs rows, then run fresh
bash demos/03-kafka-csv-minio/run.sh --fresh
```

> **Runtime:** ~11 minutes for the default 3 batches at 5-minute intervals.

## Files

| File | Purpose |
|------|---------|
| [kafka/producers/weather_kafka.py](../kafka/producers/weather_kafka.py) | Publishes full CSV batches (header + 30 rows) every 5 minutes to `weather-csv` topic |
| [kafka/connect/s3-sink.json](../kafka/connect/s3-sink.json) | S3 Sink: ByteArrayFormat, flush.size=1, TimeBasedPartitioner → `YYYY-MM-DD/HH/` |
| [tpt/scripts/demo03/nos_create.bteq](../tpt/scripts/demo03/nos_create.bteq) | NOSREAD_KEYS + CREATE FOREIGN TABLE with PATHPATTERN |
| [tpt/scripts/demo03/nos_load.bteq](../tpt/scripts/demo03/nos_load.bteq) | Incremental INSERT (current-hour partition only) + summary |
| [tpt/scripts/demo03/nos_prepare.bteq](../tpt/scripts/demo03/nos_prepare.bteq) | Pre-run: drop FOREIGN TABLE only (weather_obs untouched) |
| [tpt/scripts/demo03/nos_fresh.bteq](../tpt/scripts/demo03/nos_fresh.bteq) | Full reset: drop FOREIGN TABLE + DELETE FROM weather_obs ALL |
| [demos/03-kafka-csv-minio/run.sh](../demos/03-kafka-csv-minio/run.sh) | Orchestration |

## How it works

```
1. Producer (weather_kafka.py)
   └─ Generates one CSV batch every 5 minutes: header + 5 stations × 6 offsets (30 rows)
   └─ One Kafka message = one complete CSV file (including header row)

2. Kafka Connect S3 Sink — TimeBasedPartitioner
   └─ ByteArrayFormat: writes message bytes as-is
   └─ flush.size=1: a new file is written after every single Kafka message
   └─ path.format "yyyy-MM-dd/HH" → s3://demo-csv/raw/weather-csv/2026-06-19/14/...

3. NOS FOREIGN TABLE (partition-aware)
   └─ PATHPATTERN ('$var1/$var2/$var3/$var4/$var5') names each path segment
   └─ WHERE $var3 = '2026-06-19' AND $var4 = '14' prunes all other partitions before any I/O

4. Incremental INSERT into weather_obs
   └─ Header rows rejected by type-cast failure (CAST('station_id' AS TIMESTAMP) fails)
   └─ Prior hours in weather_obs are untouched — data accumulates across runs
```

## Key NOS design rules

| Rule | Detail |
|---|---|
| `PATHPATTERN` | Most important NOS performance lever — eliminates file listing for non-matching partitions |
| `PARTITION BY COLUMN` is Parquet/JSON-only on 20.x | Adding it to a CSV foreign table triggers error 3706; omit it; string WHERE clauses still prune correctly |
| `$var` vs `${VAR}` | `${CURRENT_YEAR}` (curly braces) is replaced by the perl preprocessor; `$var3` is a Teradata NOS path variable resolved at query time |
| `HEADER('TRUE')` does not suppress header rows from `COUNT(*)` | Header rows from each file appear in raw SELECT; they are rejected during typed INSERT via cast failure |
| `STOREDAS` not accepted for CSV in `CREATE FOREIGN TABLE USING` | Omit it; CSV is the default on 20.x |
| `DEFINER TRUSTED` auth for FOREIGN TABLE | `EXTERNAL SECURITY DEFINER TRUSTED` resolves the auth object in the table's own database; `INVOKER TRUSTED` would look in the session user's database |

## Tables

```sql
-- NOS foreign table (partition-aware, spans full MinIO path)
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
NO PRIMARY INDEX;

-- Relational target (accumulates across runs)
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

## Sample queries

```sql
-- Path-filtered NOS query: reads only the current-hour partition
SELECT station_id,
       CAST(TRIM(observation_ts) AS TIMESTAMP(0) FORMAT 'YYYY-MM-DDBHH:MI:SS') AS obs_ts,
       CAST(TRIM(temperature_c) AS DECIMAL(5,1)) AS temp_c,
       conditions
FROM demo_db.weather_nos_ft
WHERE $var3 = '2026-06-19'
  AND $var4 = '14'
ORDER BY obs_ts DESC;

-- Aggregates from the relational table across all accumulated hours
SELECT station_id,
       COUNT(*)             AS obs_count,
       AVG(temperature_c)  AS avg_temp_c,
       MAX(wind_speed_kts) AS max_wind_kts
FROM demo_db.weather_obs
GROUP BY station_id
ORDER BY station_id;
```
