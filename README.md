# Teradata Streaming Demo

A hands-on demonstration of Teradata's real-time and near-real-time data ingestion capabilities using Apache Kafka, MinIO, Apache Flink, and Teradata Parallel Transporter (TPT).

Each demo is self-contained and runnable with a single `bash demos/<n>-<name>/run.sh` command.

---

## Demos

| # | Name | Ingestion method | Key feature shown |
|---|------|-----------------|-------------------|
| 1 | [avro-blob](docs/demo01-avro-blob.md) | TPT STREAM + DataConnector | Avro container files → `DATASET AVRO` column via manifest; schema evolution |
| 2 | [kafka-tpt-stream](docs/demo02-kafka-tpt-stream.md) | TPT STREAM + Kafka Access Module | Continuous near-real-time INSERT from Kafka |
| 3 | [kafka-csv-minio](docs/demo03-kafka-csv-minio.md) | Kafka Connect S3 Sink + NOS | Object store → NOS foreign table with partition pruning |
| 4 | [kafka-flink-iceberg](docs/demo04-kafka-flink-iceberg.md) | Flink → Iceberg → OTF | Open Table Format query from Teradata via DATALAKE |
| 5 | [flink-td-enrich](docs/demo05-flink-td-enrich.md) | Flink Lookup Join ← Teradata | In-stream enrichment from Teradata reference table |
| 6 | [flink-avro-tpt](docs/demo06-flink-avro-tpt.md) | Flink Avro decode → TPT STREAM | Schema Registry Avro → Flink type transforms → TPT ingest |
| 7 | [kafka-connect-td](docs/demo07-kafka-connect-td.md) | Kafka Connect JDBC Sink | Direct Kafka → Teradata insert; no TPT, Community Licence |
| 8 | [dlq-pattern](docs/demo08-dlq-pattern.md) | Kafka Connect JDBC Sink + DLQ | errors.tolerance=all routes bad records to a dead-letter topic |

---

## Quick start

```bash
cp .env.example .env
# Edit .env: set TD_HOST, TD_USER, TD_PASSWORD, HOST_IP, KAFKA_CLUSTER_ID

docker build -t td-demo-kafka-connect:latest kafka-connect/
docker build -t flink-demo:latest flink/
docker compose up -d

docker compose exec -T tpt bash /tpt/scripts/run_setup.sh

# Then run any demo:
bash demos/01-avro-blob/run.sh
bash demos/04-kafka-flink-iceberg/run.sh --bounded
bash demos/05-flink-td-enrich/run.sh
```

Full setup instructions: [docs/setup.md](docs/setup.md)

---

## Teradata Express setup

If you are using [**Teradata Express (TDExpress v20)**](https://downloads.teradata.com/download/database/teradata-express/vmware) as the Teradata instance, additional one-time configuration is required to enable Open Table Format (OTF) support for Demos 3–5.

See [docs/setup-td-express-20.md](docs/setup-td-express-20.md) for the full configuration guide, including:

- OTF feature flags (`JavaOTFFlags`, `NativeOTFFlags`, `DisableMOTF`, `ColumnarPurchased`)
- NOS / object storage flags (HTTP mode, path-style S3 addressing)
- Optimizer and pipeline performance flags
- Step-by-step `dbscontrol` commands and validation queries

---

## Repository layout

```
demos/          # One subdirectory per demo — each contains run.sh
docs/           # Per-demo documentation + infrastructure setup guides
flink/
  Dockerfile    # Flink 1.20 + Iceberg + Hive + Teradata JDBC dialect
  dialect/      # Custom Teradata JdbcFactory/Dialect Java sources (built in Docker)
  jobs/
    demo04/     # Flink SQL scripts for Demo 4
    demo05/     # Flink SQL scripts for Demo 5
    demo06/     # Flink SQL scripts for Demo 6 (Avro → pipe-delimited Kafka)
kafka/
  connect/      # Kafka Connect connector configs and credentials (td-credentials.properties gitignored)
  producers/    # Python producers (ADS-B, weather, Avro data generator)
  schemas/      # Avro schemas
tpt/
  scripts/
    infra/      # One-time setup BTEQ (setup_demo_tables.bteq)
    demo01/     # BTEQ scripts for Demo 1 (prepare + decode)
    demo02/     # BTEQ scripts for Demo 2
    demo03/     # BTEQ scripts for Demo 3
    demo04/     # BTEQ scripts for Demo 4
    demo05/     # BTEQ scripts for Demo 5
    demo06/     # BTEQ scripts for Demo 6
    demo07/     # BTEQ scripts for Demo 7
    demo08/     # BTEQ scripts for Demo 8
    run_bteq.sh        # Runs any BTEQ script with ${VAR} substitution
    run_setup.sh       # One-time Teradata setup runner
    run_tbuild.sh      # Runs a TPT tbuild job
  tbuild/       # TPT job definitions
```

---

## Further reading

- [Teradata DATASET Data Type — B035-1198](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/DATASET-Data-Type)
- [Teradata Parallel Transporter Reference Guide — B035-2436](https://docs.teradata.com/r/Enterprise_IntelliFlex_Lake_VMware/Teradata-Parallel-Transporter-Reference-20.00/Teradata-PT-Utility-Commands/Overview)
- [TPT Kafka Access Module — B035-2447](https://docs.teradata.com/r/Enterprise_IntelliFlex_Lake_VMware/Teradata-Tools-and-Utilities-Access-Module-Reference-20.00/Teradata-Access-Module-for-Kafka/Overview-of-the-Teradata-Access-Module-for-Kafka)
- [Teradata Native Object Store Getting Started Guide — B035-2198](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Native-Object-Store-Getting-Started-Guide)