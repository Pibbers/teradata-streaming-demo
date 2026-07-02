-- Demo 6: Kafka (Avro + Schema Registry) → Flink transform → Kafka (pipe-delimited) → TPT STREAM → Teradata
--
-- Flink reads Confluent wire-format Avro from 'adsb-avro', decodes it using the
-- schema fetched from Schema Registry, applies type transformations that would be
-- impossible or unsafe for TPT's text-only DELIMITED format (boolean→INT,
-- timestamp-millis→formatted strings, pos_date derivation), then re-emits each
-- row as pipe-delimited text to 'adsb-positions-flink'.  TPT STREAM consumes that
-- intermediate topic exactly as it does Demo 02's 'adsb-positions' topic.
--
-- Type mapping from Avro schema (adsb_position.avsc):
--   string           → STRING
--   double           → DOUBLE
--   int              → INT
--   boolean          → BOOLEAN   (cast to INT 0/1 in INSERT)
--   long/timestamp-millis → BIGINT  (epoch ms; AvroSchemaConverter cannot convert TIMESTAMP_LTZ)
--
-- TIMESTAMP_LTZ(3) is intentionally avoided: AvroSchemaConverter.convertToSchema() in
-- flink-sql-avro-confluent-registry 1.20.1 throws UnsupportedOperationException for
-- TIMESTAMP_LTZ when deriving the Avro reader schema. Declaring BIGINT reads the raw
-- epoch-millisecond long directly. FROM_UNIXTIME + MOD handle the timestamp formatting.

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
    'properties.group.id'          = 'flink-demo06',
    'scan.startup.mode'            = 'latest-offset',
    'format'                       = 'avro-confluent',
    'avro-confluent.url'           = 'http://schema-registry:8081'
);

-- Sink: one Kafka message per record, raw bytes — no CSV quoting.
-- The 'raw' format writes the STRING column directly as UTF-8 bytes.
-- Each message is a complete pipe-delimited record with a trailing newline,
-- identical to what adsb_producer.py --format delimited produces.
-- Column order matches ADSB_SCHEMA in kafka_stream_06.tbuild exactly:
--   icao24 | callsign | latitude | longitude | altitude | velocity |
--   heading | vertical_rate | on_ground | squawk | pos_date | ts\n
CREATE TEMPORARY TABLE kafka_adsb_raw (
    line STRING
) WITH (
    'connector'                    = 'kafka',
    'topic'                        = 'adsb-positions-flink',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format'                       = 'raw'
);

-- ts is BIGINT (epoch ms). FROM_UNIXTIME converts to formatted strings.
-- LPAD pads the sub-second millis to 3 digits (e.g. 5 → '005').
-- CHR(10) appends the newline record terminator that TPT DELIMITED expects.
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
