-- Demo 6 bounded mode: reads all messages already in 'adsb-avro' from the beginning.
-- Used when run.sh is invoked with --bounded: the producer publishes a fixed number
-- of messages first, then this job processes them as a finite batch.
-- Identical to stream.sql except 'scan.startup.mode' = 'earliest-offset'.

CREATE TEMPORARY TABLE kafka_adsb_avro (
    icao24        STRING,
    callsign      STRING,
    latitude      DOUBLE,
    longitude     DOUBLE,
    altitude      INT,
    velocity      DOUBLE,
    heading       DOUBLE,
    vertical_rate INT,
    on_ground     BOOLEAN,
    squawk        STRING,
    ts            BIGINT
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'adsb-avro',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'          = 'flink-demo06-batch',
    'scan.startup.mode'            = 'earliest-offset',
    'format'                       = 'avro-confluent',
    'avro-confluent.url'           = 'http://schema-registry:8081'
);

CREATE TEMPORARY TABLE kafka_adsb_raw (
    line STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'adsb-positions-flink',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'raw'
);

INSERT INTO kafka_adsb_raw
SELECT
    icao24                                                            || '|' ||
    callsign                                                          || '|' ||
    CAST(latitude      AS STRING)                                     || '|' ||
    CAST(longitude     AS STRING)                                     || '|' ||
    CAST(altitude      AS STRING)                                     || '|' ||
    CAST(velocity      AS STRING)                                     || '|' ||
    CAST(heading       AS STRING)                                     || '|' ||
    CAST(vertical_rate AS STRING)                                     || '|' ||
    CAST(CASE WHEN on_ground THEN 1 ELSE 0 END AS STRING)            || '|' ||
    squawk                                                            || '|' ||
    FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd')                           || '|' ||
    FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss') || '.' || LPAD(CAST(MOD(ts, 1000) AS STRING), 3, '0') ||
    CHR(10)
FROM kafka_adsb_avro;
