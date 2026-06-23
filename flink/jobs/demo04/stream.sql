-- Demo 4: Continuous streaming from Kafka into Apache Iceberg via Flink SQL.
-- Kafka Access Module → Iceberg (Hive Metastore catalog, Parquet on MinIO).
--
-- Checkpointing is mandatory for the Iceberg Flink sink — each checkpoint
-- commits an atomic Iceberg snapshot. Without checkpointing, no data is written.
-- With a 30s interval, rows become visible to Teradata OTF within ~30s.
--
-- Run via:
--   docker compose exec flink-jobmanager /opt/flink/bin/sql-client.sh \
--     -f /opt/flink/jobs/demo04_stream.sql
-- The INSERT submits a streaming job and the SQL client exits.
-- Capture the Job ID from stdout and cancel via REST when done.

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

-- Register the Hive Metastore as an Iceberg catalog.
-- 'warehouse' uses s3:// which Flink routes through the flink-s3-fs-hadoop plugin
-- (configured with MinIO endpoint/credentials in FLINK_PROPERTIES).
CREATE CATALOG hive_catalog WITH (
    'type'         = 'iceberg',
    'catalog-type' = 'hive',
    'uri'          = 'thrift://hive-metastore:9083',
    'warehouse'    = 's3a://iceberg/warehouse'
);

USE CATALOG hive_catalog;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- Iceberg sink table.
-- Partitioned by pos_date (daily) — aligns with Demo 2's PPI design and
-- enables Teradata OTF partition pruning when filtering by date.
-- on_ground stored as INT (0/1) — avoids string-to-BOOLEAN cast complexity.
-- No PRIMARY INDEX / FALLBACK / SET — prohibited on OTF tables.
CREATE TABLE IF NOT EXISTS adsb_positions (
    icao24        STRING,
    callsign      STRING,
    latitude      DOUBLE,
    longitude     DOUBLE,
    altitude      INT,
    velocity      DOUBLE,
    heading       DOUBLE,
    vertical_rate INT,
    on_ground     INT,
    squawk        STRING,
    pos_date      STRING,
    ts            TIMESTAMP(3)
)
PARTITIONED BY (pos_date)
WITH (
    'format-version'       = '2',
    'write.format.default' = 'parquet'
);

-- Kafka source as a TEMPORARY table (not persisted to the Hive catalog).
-- All columns are STRING — the same approach as the TPT DELIMITED schema.
-- Pipe-delimited to match adsb_producer.py --format delimited output.
-- Column order must match encode_delimited() in adsb_producer.py exactly:
--   icao24 | callsign | latitude | longitude | altitude | velocity |
--   heading | vertical_rate | on_ground | squawk | pos_date | ts_str
CREATE TEMPORARY TABLE kafka_adsb (
    icao24        STRING,
    callsign      STRING,
    latitude      STRING,
    longitude     STRING,
    altitude      STRING,
    velocity      STRING,
    heading       STRING,
    vertical_rate STRING,
    on_ground     STRING,
    squawk        STRING,
    pos_date      STRING,
    ts_str        STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'adsb-positions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-demo04',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'csv',
    'csv.field-delimiter'          = '|',
    'csv.ignore-parse-errors'      = 'true'
);

-- Stream from Kafka into Iceberg.
-- ts_str format from adsb_producer.py encode_delimited(): 'yyyy-MM-dd HH:mm:ss.SSS'
INSERT INTO adsb_positions
SELECT
    icao24,
    callsign,
    CAST(latitude      AS DOUBLE),
    CAST(longitude     AS DOUBLE),
    CAST(altitude      AS INT),
    CAST(velocity      AS DOUBLE),
    CAST(heading       AS DOUBLE),
    CAST(vertical_rate AS INT),
    CAST(on_ground     AS INT),
    squawk,
    pos_date,
    TO_TIMESTAMP(ts_str, 'yyyy-MM-dd HH:mm:ss.SSS')
FROM kafka_adsb;
