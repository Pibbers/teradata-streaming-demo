-- Demo 5 (bounded mode): read all messages currently on the Kafka topic,
-- enrich via Teradata JDBC Lookup Join, write to Iceberg, then exit.
--
-- Run the producer first (--count 200), then submit this job.
-- scan.bounded.mode = 'latest-offset' reads from offset 0 up to the offset
-- at job submission time, processes all messages, commits one Iceberg
-- snapshot, and exits with state FINISHED.
--
-- Variables substituted by perl in run.sh before submission.
-- Do not submit this file directly — run.sh writes demo05_batch_sub.sql.

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

CREATE CATALOG hive_catalog WITH (
    'type'         = 'iceberg',
    'catalog-type' = 'hive',
    'uri'          = 'thrift://hive-metastore:9083',
    'warehouse'    = 's3a://iceberg/warehouse'
);

USE CATALOG hive_catalog;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

CREATE TABLE IF NOT EXISTS enriched_positions (
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
    ts            TIMESTAMP(3),
    registration  STRING,
    aircraft_type STRING,
    operator      STRING,
    country       STRING
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
    ts_str        STRING,
    proc_time     AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'adsb-positions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-demo05-batch',
    'scan.startup.mode'            = 'earliest-offset',
    'scan.bounded.mode'            = 'latest-offset',
    'format'                       = 'csv',
    'csv.field-delimiter'          = '|',
    'csv.ignore-parse-errors'      = 'true'
);

CREATE TEMPORARY TABLE aircraft_ref (
    icao24        STRING,
    registration  STRING,
    aircraft_type STRING,
    operator      STRING,
    country       STRING,
    PRIMARY KEY (icao24) NOT ENFORCED
) WITH (
    'connector'                               = 'jdbc',
    'url'                                     = 'jdbc:teradata://${TD_HOST}/DATABASE=${TD_DATABASE},DBS_PORT=1025,COP=OFF,TMODE=ANSI,CHARSET=UTF8',
    'table-name'                              = 'aircraft_ref',
    'username'                                = '${TD_USER}',
    'password'                                = '${TD_PASSWORD}',
    'lookup.cache'                            = 'PARTIAL',
    'lookup.partial-cache.max-rows'           = '10000',
    'lookup.partial-cache.expire-after-write' = '30s'
);

INSERT INTO enriched_positions
SELECT
    a.icao24,
    a.callsign,
    CAST(a.latitude      AS DOUBLE),
    CAST(a.longitude     AS DOUBLE),
    CAST(a.altitude      AS INT),
    CAST(a.velocity      AS DOUBLE),
    CAST(a.heading       AS DOUBLE),
    CAST(a.vertical_rate AS INT),
    CAST(a.on_ground     AS INT),
    a.squawk,
    a.pos_date,
    TO_TIMESTAMP(a.ts_str, 'yyyy-MM-dd HH:mm:ss.SSS'),
    COALESCE(r.registration,  'UNKNOWN') AS registration,
    COALESCE(r.aircraft_type, 'UNKNOWN') AS aircraft_type,
    COALESCE(r.operator,      'UNKNOWN') AS operator,
    COALESCE(r.country,       'UNKNOWN') AS country
FROM kafka_adsb a
LEFT JOIN aircraft_ref FOR SYSTEM_TIME AS OF a.proc_time AS r
    ON a.icao24 = r.icao24;
