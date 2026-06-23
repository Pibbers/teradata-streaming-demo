# Demo 5: Flink Lookup Join → Teradata enrichment

**Topic:** A live ADS-B stream carries only ICAO24 aircraft identifiers. Teradata holds the master `aircraft_ref` reference table (registration, aircraft type, operator, country). Flink enriches each position message in-flight via a **JDBC Lookup Join** — querying Teradata per-message with a 30-second cache — then writes enriched rows to Iceberg. Teradata queries its own enrichment back via OTF.

This reverses the usual data-flow: instead of pushing data *into* Teradata, Flink pulls reference data *from* Teradata to enhance a real-time stream.

## What it demonstrates

- **Teradata as an enrichment source** — the JDBC Lookup Join executes `SELECT … WHERE icao24 = ?` against Teradata using the `terajdbc` driver; results are cached per the configured TTL, keeping Teradata load minimal
- **Cache TTL and live invalidation** — with a 30-second TTL, an `UPDATE` to `aircraft_ref` in Teradata propagates to newly enriched Iceberg rows within one checkpoint window
- **LEFT JOIN graceful degradation** — ICAO24 codes with no matching reference row receive `UNKNOWN` defaults rather than being dropped
- **Teradata as both source and destination** — `aircraft_ref` is the lookup source; `enriched_positions` in Iceberg is the OTF query target

## How to run

```bash
# Continuous streaming (default) — press Ctrl+C to stop
bash demos/05-flink-td-enrich/run.sh

# Bounded mode — 200 messages; exits automatically
bash demos/05-flink-td-enrich/run.sh --bounded
```

## Files

| File | Purpose |
|------|---------|
| [kafka/producers/adsb_producer.py](../kafka/producers/adsb_producer.py) | Same ADS-B producer as Demos 2 and 4 — reused without modification |
| [flink/jobs/demo05/stream.sql](../flink/jobs/demo05/stream.sql) | Flink SQL template: Kafka source + JDBC Lookup Join + Iceberg sink (checkpoint 30s) |
| [flink/jobs/demo05/batch.sql](../flink/jobs/demo05/batch.sql) | Bounded variant: earliest→latest-offset Kafka scan |
| [flink/jobs/demo05/drop.sql](../flink/jobs/demo05/drop.sql) | Reset: DROP TABLE enriched_positions (leaves Demo 4's adsb_positions intact) |
| [flink/dialect/TeradataFactory.java](../flink/dialect/TeradataFactory.java) | Registers Teradata JDBC dialect via Java SPI (`flink-connector-jdbc 3.3.0-1.20`) |
| [flink/dialect/TeradataDialect.java](../flink/dialect/TeradataDialect.java) | Implements `AbstractDialect` for Teradata URLs |
| [flink/dialect/TeradataDialectConverter.java](../flink/dialect/TeradataDialectConverter.java) | Implements `AbstractDialectConverter` for row type conversion |
| [tpt/scripts/demo05/setup.bteq](../tpt/scripts/demo05/setup.bteq) | CREATE TABLE aircraft_ref + INSERT 10 fleet rows |
| [tpt/scripts/demo05/teardown.bteq](../tpt/scripts/demo05/teardown.bteq) | DROP TABLE aircraft_ref |
| [tpt/scripts/demo05/otf_query.bteq](../tpt/scripts/demo05/otf_query.bteq) | Heartbeat: `STATUS rows=N aircraft=N latest=TIMESTAMP` |
| [tpt/scripts/demo05/otf_verify.bteq](../tpt/scripts/demo05/otf_verify.bteq) | Final: HELP TABLE, row/aircraft summary, per-aircraft enrichment, TD_SNAPSHOTS() |
| [demos/05-flink-td-enrich/run.sh](../demos/05-flink-td-enrich/run.sh) | Orchestration — dual-mode |

## How it works

```
1. Reset
   └─ Flink SQL client drops enriched_positions (leaves adsb_positions intact)
   └─ Kafka topic deleted and recreated

2. Reference table setup
   └─ BTEQ: DROP + CREATE aircraft_ref in demo_db
   └─ INSERT 10 rows matching the adsb_producer.py fleet

3. DATALAKE verification
   └─ demo04/datalake_create.bteq: idempotent CREATE DATALAKE demo_iceberg

4. Flink job submission
   └─ Template substituted (${TD_HOST} etc. → real values via perl)
   └─ Written to flink/jobs/demo05/stream_sub.sql (bind-mounted volume)
   └─ Flink SQL client submits streaming INSERT; exits after job accepted

5. Streaming with monitoring
   └─ adsb_producer.py publishes 10 aircraft positions at 1s interval
   └─ Flink reads each row from Kafka, looks up icao24 in aircraft_ref:
        └─ Cache hit (within 30s TTL): uses cached registration/operator
        └─ Cache miss: issues SELECT FROM aircraft_ref WHERE icao24 = ? to Teradata
   └─ Enriched rows written to Iceberg; committed every 30s checkpoint

6. Graceful shutdown (Ctrl+C)
   └─ Producer killed → 35s wait → final Iceberg snapshot committed
   └─ Flink job cancelled via REST PATCH /jobs/{id}?mode=cancel
```

## Custom Teradata dialect JAR

`flink-connector-jdbc` has no built-in Teradata dialect. The Flink image builds a custom JAR from source at image-build time (`flink/Dockerfile`):

- **SPI interface**: `org.apache.flink.connector.jdbc.core.database.JdbcFactory`
- **Compilation**: ECJ 3.29.0 (runs on JRE 11; ECJ 3.35+ requires JRE 17 which Flink 1.20 doesn't use)
- **JAR creation**: `apt-get install zip` in the Dockerfile build step; `zip -r` packages classes + `META-INF/services/`
- **Version**: Targets `flink-connector-jdbc 3.3.0-1.20` API — do **not** use 1.x (it references `flink-shaded-guava30` which Flink 1.20 dropped)

Cache options for 3.x (different from 1.x syntax):
```sql
'lookup.cache'                            = 'PARTIAL',
'lookup.partial-cache.max-rows'           = '10000',
'lookup.partial-cache.expire-after-write' = '30s'
```

## Live cache invalidation walkthrough

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

## aircraft_ref reference table

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

## Flink SQL key excerpts

```sql
-- Processing-time attribute — required for lookup join temporal syntax
CREATE TEMPORARY TABLE kafka_adsb (
    icao24    STRING,
    ...
    proc_time AS PROCTIME()
) WITH ('connector' = 'kafka', ...);

-- Teradata JDBC lookup table
CREATE TEMPORARY TABLE aircraft_ref (
    icao24        STRING,
    registration  STRING,
    aircraft_type STRING,
    operator      STRING,
    country       STRING,
    PRIMARY KEY (icao24) NOT ENFORCED
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

## Sample queries

```sql
-- Full per-aircraft enrichment summary
SELECT
    TRIM(icao24)        AS icao24,
    TRIM(callsign)      AS callsign,
    COUNT(*)            AS positions,
    MAX(registration)   AS registration,
    MAX(aircraft_type)  AS ac_type,
    MAX(operator)       AS operator,
    MAX(country)        AS country
FROM demo_iceberg.demo.enriched_positions
GROUP BY 1, 2
ORDER BY 1;

-- Iceberg snapshot history
SELECT * FROM TD_SNAPSHOTS(ON demo_iceberg.demo.enriched_positions) AS snap;
```
