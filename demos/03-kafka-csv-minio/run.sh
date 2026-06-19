#!/bin/bash
# ============================================================
# Demo 3: MinIO CSV → Teradata NOS (Native Object Store)
# ============================================================
# Demonstrates:
#   • weather_batch.py generating synthetic METAR-style CSV
#     observations and uploading them directly to MinIO
#   • Teradata NOS FOREIGN TABLE pointing at the MinIO bucket,
#     allowing SQL queries over CSV files in object storage
#   • Bulk INSERT from NOS into the relational weather_obs table
#
# Architecture:
#   weather_batch.py → MinIO (s3://demo-csv/raw/weather/) → NOS → Teradata
#
# Alternative (not shown): publish to Kafka topic, use the
# kafka-connect S3-Sink connector (kafka/connect/s3-sink.json)
# to land CSV files in MinIO, then query via NOS.
#
# Prerequisites:
#   docker compose up -d        (minio, minio-init, tpt)
#   bash tpt/scripts/run_setup.sh
#   pip install boto3
#
# Run from project root:
#   bash demos/03-kafka-csv-minio/run.sh
# ============================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

MINIO_ENDPOINT="http://localhost:${MINIO_API_PORT:-9000}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"
WEATHER_RUNS="${WEATHER_RUNS:-3}"
WEATHER_INTERVAL="${WEATHER_INTERVAL:-5}"

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

echo "======================================================"
echo "  Demo 3: MinIO CSV → Teradata NOS"
echo "  MinIO endpoint: $MINIO_ENDPOINT"
echo "  Batch runs:     $WEATHER_RUNS  (interval: ${WEATHER_INTERVAL}s)"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/4  Uploading weather CSV batches to MinIO"
python3 kafka/producers/weather_batch.py \
  --endpoint    "$MINIO_ENDPOINT" \
  --access-key  "$MINIO_USER" \
  --secret-key  "$MINIO_PASS" \
  --runs        "$WEATHER_RUNS" \
  --interval    "$WEATHER_INTERVAL"

# ── Step 2 ─────────────────────────────────────────────────
step "2/4  Creating NOS FOREIGN TABLE over MinIO bucket"
echo "      HOST_IP=${HOST_IP}  (MinIO reachable from Teradata)"
docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh \
  /tpt/scripts/demo03_nos_create.bteq

# ── Step 3 ─────────────────────────────────────────────────
step "3/4  Loading weather_obs from NOS"
docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh \
  /tpt/scripts/demo03_nos_load.bteq

# ── Step 4 ─────────────────────────────────────────────────
step "4/4  Done"
echo ""
echo "======================================================"
echo "  Demo 3 complete!"
echo ""
echo "  The NOS table weather_nos_ft remains — re-run step 1"
echo "  to land more batches and re-run step 3 to ingest them."
echo ""
echo "  Try a live NOS query in Teradata Studio:"
echo "    SELECT * FROM ${TD_DATABASE:-demo_db}.weather_nos_ft"
echo "    WHERE station_id = 'EGLL';"
echo "======================================================"
