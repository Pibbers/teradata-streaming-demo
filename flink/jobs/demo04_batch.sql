-- Demo 4 (bounded mode): read all messages currently on the Kafka topic and
-- write them to Iceberg in one batch job, then exit.
--
-- Flow: run the producer first (--count 200), then submit this job.
-- scan.bounded.mode = 'latest-offset' reads from offset 0 up to the offset
-- at job submission time, processes all 200 messages, commits one Iceberg
-- snapshot, and exits with state FINISHED.
--
-- The SQL client blocks until the batch job completes.
--
-- Run via:
--   docker compose exec flink-jobmanager /opt/flink/bin/sql-client.sh \
--     -f /opt/flink/jobs/demo04_batch.sql

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG hive_catalog WITH (
    'type'         = 'iceberg',
    'catalog-type' = 'hive',
    'uri'          = 'thrift://hive-metastore:9083',
    'warehouse'    = 's3a://iceberg/warehouse'
);

USE CATALOG hive_catalog;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

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
    'properties.group.id'          = 'flink-demo04-batch',
    'scan.startup.mode'            = 'earliest-offset',
    'scan.bounded.mode'            = 'latest-offset',
    'format'                       = 'csv',
    'csv.field-delimiter'          = '|',
    'csv.ignore-parse-errors'      = 'true'
);

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
