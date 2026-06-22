-- Reset: drop the Demo 4 Iceberg table and namespace.
-- Run via: docker compose exec flink-jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/jobs/demo04_drop.sql
-- Dropping a managed Iceberg table removes both metadata and Parquet data files from MinIO.

CREATE CATALOG hive_catalog WITH (
    'type'         = 'iceberg',
    'catalog-type' = 'hive',
    'uri'          = 'thrift://hive-metastore:9083',
    'warehouse'    = 's3a://iceberg/warehouse'
);

USE CATALOG hive_catalog;

DROP TABLE IF EXISTS demo.adsb_positions;
DROP DATABASE IF EXISTS demo;
