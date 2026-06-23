-- Reset: drop the Demo 5 enriched_positions Iceberg table.
-- Does NOT drop the 'demo' database — Demo 4's adsb_positions lives there too.
-- Dropping a managed Iceberg table removes both metadata and Parquet data files from MinIO.

CREATE CATALOG hive_catalog WITH (
    'type'         = 'iceberg',
    'catalog-type' = 'hive',
    'uri'          = 'thrift://hive-metastore:9083',
    'warehouse'    = 's3a://iceberg/warehouse'
);

USE CATALOG hive_catalog;

DROP TABLE IF EXISTS demo.enriched_positions;
