-- Demo 5: Continuous streaming from Kafka into Iceberg via Flink SQL.
-- Enriches each ADS-B position with aircraft reference data from Teradata
-- using a JDBC Lookup Join with a 30-second cache TTL.
--
-- Variables substituted by perl in run.sh before submission:
--   ${TD_HOST}  ${TD_USER}  ${TD_PASSWORD}  ${TD_DATABASE}
--
-- Do not submit this file directly — run.sh writes a substituted copy to
-- /opt/flink/jobs/demo05_stream_sub.sql before calling sql-client.sh.

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

-- Register the Hive Metastore as an Iceberg catalog.
CREATE CATALOG hive_catalog WITH (
    'type'         = 'iceberg',
    'catalog-type' = 'hive',
    'uri'          = 'thrift://hive-metastore:9083',
    'warehouse'    = 's3a://iceberg/warehouse'
);

USE CATALOG hive_catalog;
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- Iceberg sink: enriched ADS-B positions with reference fields from Teradata.
-- Partitioned by pos_date for OTF partition pruning.
-- Enriched columns (registration, aircraft_type, operator, country) are appended
-- after the raw position columns to make the join contribution visually clear.
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

-- Kafka source: pipe-delimited ADS-B positions from adsb_producer.py --format delimited.
-- proc_time is a Flink processing-time attribute — mandatory for the lookup join temporal
-- syntax (FOR SYSTEM_TIME AS OF proc_time). Not written to Iceberg.
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
    ts_str        STRING,
    proc_time     AS PROCTIME()
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'adsb-positions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-demo05',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'csv',
    'csv.field-delimiter'          = '|',
    'csv.ignore-parse-errors'      = 'true'
);

-- Teradata JDBC lookup table: aircraft master reference data.
-- PRIMARY KEY NOT ENFORCED declares the lookup key without enforcing uniqueness.
-- Cache: 30s TTL keeps Teradata load minimal while keeping the live-update demo
--   moment visible within one checkpoint window. Raise to '300s' for production.
-- COP=OFF: disables Teradata COP routing discovery — prevents failures in demo
--   environments where only a single node is reachable.
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

-- Enrich each Kafka position with reference data from Teradata.
-- FOR SYSTEM_TIME AS OF proc_time: temporal lookup join — queries aircraft_ref
--   using a prepared statement (SELECT ... WHERE icao24 = ?) and caches results
--   per the TTL above.
-- LEFT JOIN: rows with no matching icao24 receive COALESCE defaults ('UNKNOWN')
--   rather than being dropped — protects against gaps in the reference table.
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
