# Demo 2: Kafka → TPT STREAM

**Topic:** Near-real-time INSERT into Teradata directly from a live Kafka topic using the TPT STREAM operator and the Kafka Access Module (`libkafkaaxsmod.so`), with rows visible in Teradata every ~5 seconds throughout the run.

## What it demonstrates

- The TPT Kafka Access Module connecting to a live Kafka topic and streaming pipe-delimited messages into Teradata with no intermediary file
- `tbuild -l 5` (latency interval) — the key flag that forces the STREAM operator to flush its internal buffers and commit rows to Teradata every 5 seconds, even with a continuously-producing Kafka feed
- `Format='DELIMITED'` with all-VARCHAR schema — the correct approach when source messages are plain text
- Typed column conversion: all VARCHAR schema fields cast to their target types (`FLOAT`, `INTEGER`, `BYTEINT`, `DATE`, `TIMESTAMP(3)`) inside the `APPLY INSERT`
- Partitioned target table (PPI on `pos_date`) — the STREAM operator inserts directly into the correct partition
- Graceful shutdown: Ctrl+C kills the producer; TPT drains remaining buffered messages then exits cleanly

## How to run

```bash
# Continuous streaming (default); press Ctrl+C to stop
bash demos/02-kafka-tpt-stream/run.sh

# Bounded mode — 100 messages; exits automatically
bash demos/02-kafka-tpt-stream/run.sh --bounded
```

## Files

| File | Purpose |
|------|---------|
| [kafka/producers/adsb_producer.py](../kafka/producers/adsb_producer.py) | Synthetic ADS-B producer; `--format delimited` writes pipe-delimited text; `--continuous` runs indefinitely |
| [tpt/tbuild/kafka_stream.tbuild](../tpt/tbuild/kafka_stream.tbuild) | TPT job: DataConnector (Kafka Access Module) → STREAM operator |
| [tpt/scripts/demo02/prepare.bteq](../tpt/scripts/demo02/prepare.bteq) | Truncates `adsb_positions`; drops STREAM operator work tables |
| [tpt/scripts/demo02/status.bteq](../tpt/scripts/demo02/status.bteq) | Single-line heartbeat: `STATUS rows=N latest=TIMESTAMP` |
| [tpt/scripts/demo02/verify.bteq](../tpt/scripts/demo02/verify.bteq) | Post-run row count and per-aircraft summary |
| [demos/02-kafka-tpt-stream/run.sh](../demos/02-kafka-tpt-stream/run.sh) | Orchestration — dual-mode |

## What to expect

```
======================================================
  Demo 2: Kafka → Teradata TPT STREAM (ADS-B)
  Mode:               CONTINUOUS (Ctrl+C to stop)
======================================================

── 1/3  Clearing TPT and Kafka state from any prior run
── 2/3  Starting TPT STREAM job and producer

  [12:01:15 UTC]  STATUS rows=80 latest=2026-06-22 12:01:10.443
  [12:01:25 UTC]  STATUS rows=180 latest=2026-06-22 12:01:20.512
  ...

^C
── Stopping producer...
── Waiting for TPT to drain (up to 30 s)...
   Job ttuuser completed successfully

── 3/3  Final row count
  positions_received  earliest_ts              latest_ts              aircraft_seen
  ------------------  -------------------      -------------------    -------------
                 630  2026-06-22 12:01:05.210  2026-06-22 12:02:08    10
```

## How it works

```
1. BTEQ truncates adsb_positions + drops STREAM operator work tables
   Kafka topic deleted and recreated (clean partition offset)

2. TPT STREAM job starts (background) with -l 5
   └─ DataConnector PRODUCER + Kafka Access Module (libkafkaaxsmod.so)
   └─ AccessModuleInitStr: -M C -T <topic> -B <broker> -P 0 -W 30 (drain window)
   └─ tbuild -l 5: STREAM operator flushes internal buffers every 5s
      Without -l, flush only happens at end-of-source → rows never visible during live run.

3. Python producer runs continuously
   └─ 10 synthetic aircraft, 1 position update per aircraft per second
   └─ Each message: icao24|callsign|lat|lon|...|ts\n
   └─ \n is the DELIMITED record terminator

4. Every 5 seconds: STREAM operator flushes → ~50 rows committed to adsb_positions

5. On Ctrl+C: producer killed → Kafka idle timeout (-W 30) fires → STREAM flushes and exits
```

## Key technical constraints

| Constraint | Detail |
|---|---|
| `tbuild -l <seconds>` is required for continuous streaming | Without `-l`, STREAM only flushes at end-of-source (Kafka idle timeout) which never fires with a live producer. Source: B035-2436-103K |
| Two different `-W` flags | `tbuild -W` is the subprocess spawn timeout; `-W` inside `AccessModuleInitStr` is the Kafka idle timeout — completely different options |
| `AccessModuleName` / `AccessModuleInitStr` required | Named attributes like `TopicName` are silently ignored; use `AccessModuleName = 'libkafkaaxsmod.so'` with CLI-style flags |
| All schema columns must be VARCHAR | `Format='DELIMITED'` requires VARCHAR/CLOB; typed conversion done via `CAST` in `APPLY INSERT` |
| `\n` record terminator required | Messages without `\n` are concatenated into one giant record, causing field overflow |
| LogTable / ErrorTable must be explicit | STREAM operator defaults to DBC; specify in the user database |

## Table

```sql
CREATE MULTISET TABLE adsb_positions (
  icao24        CHAR(6)       NOT NULL,
  callsign      VARCHAR(8),
  pos_date      DATE          NOT NULL,
  latitude      DECIMAL(9,6)  NOT NULL,
  longitude     DECIMAL(10,6) NOT NULL,
  altitude      INTEGER,
  velocity      DECIMAL(7,2),
  heading       DECIMAL(6,2),
  vertical_rate INTEGER,
  on_ground     BYTEINT       DEFAULT 0,
  squawk        CHAR(4),
  ts            TIMESTAMP(3)  NOT NULL,
  ingest_ts     TIMESTAMP(0)  DEFAULT CURRENT_TIMESTAMP(0)
)
PRIMARY INDEX (icao24)
PARTITION BY RANGE_N(pos_date BETWEEN DATE '2024-01-01' AND DATE '2030-12-31' EACH INTERVAL '1' DAY);
```

## Sample queries

```sql
-- Recent position fixes
SELECT icao24, callsign, latitude, longitude, altitude, ts
FROM demo_db.adsb_positions
WHERE pos_date = CURRENT_DATE
ORDER BY ts DESC;

-- Ingest latency
SELECT icao24,
       AVG( (CAST(ingest_ts AS TIMESTAMP(3)) - ts) SECOND(4) ) AS avg_lag_secs
FROM demo_db.adsb_positions
GROUP BY 1
ORDER BY 1;
```
