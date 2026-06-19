#!/bin/bash
# ============================================================
# Demo 3: Kafka → Kafka Connect S3 Sink → MinIO CSV → Teradata NOS
# ============================================================
# Demonstrates:
#   • weather_kafka.py publishing CSV batch messages to a Kafka topic
#   • Kafka Connect S3 Sink (ByteArrayFormat, flush.size=1) writing each
#     Kafka message as a separate CSV file in MinIO
#   • READ_NOS with NOSREAD_KEYS to list objects written by Kafka Connect
#   • READ_NOS with NOSREAD_SCHEMA to infer column names from CSV headers
#   • FOREIGN TABLE over the MinIO path (STOREDAS CSV, HEADER TRUE)
#   • Bulk INSERT from the NOS foreign table into the relational weather_obs table
#
# Architecture:
#   weather_kafka.py → Kafka (weather-csv) → Kafka Connect S3 Sink
#       → MinIO (demo-csv/raw/weather-csv/) → NOS FOREIGN TABLE → Teradata
#
# Prerequisites:
#   docker compose up -d   (kafka, kafka-connect, minio, minio-init, tpt)
#   bash tpt/scripts/run_setup.sh
#   pip install confluent-kafka boto3
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
CONNECT_URL="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
CONNECTOR_NAME="demo-weather-s3-sink"
WEATHER_BATCHES="${WEATHER_BATCHES:-3}"
WEATHER_INTERVAL="${WEATHER_INTERVAL:-5}"

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

# Wrapper: run a BTEQ script in the TPT container, forwarding the current
# env so run_bteq.sh's perl substitution picks up values from .env rather
# than the container's stale startup environment.
bteq_run() {
  docker compose exec -T \
    -e TD_HOST="$TD_HOST" \
    -e TD_USER="$TD_USER" \
    -e TD_PASSWORD="$TD_PASSWORD" \
    -e TD_DATABASE="${TD_DATABASE:-demo_db}" \
    -e HOST_IP="$HOST_IP" \
    -e MINIO_ROOT_USER="$MINIO_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_PASS" \
    tpt bash /tpt/scripts/run_bteq.sh "$1"
}

echo "======================================================"
echo "  Demo 3: Kafka → Kafka Connect → MinIO → NOS"
echo "  MinIO endpoint:   $MINIO_ENDPOINT"
echo "  Kafka Connect:    $CONNECT_URL"
echo "  Weather batches:  $WEATHER_BATCHES  (interval: ${WEATHER_INTERVAL}s)"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/6  Preparing — clean up any previous run"

# Delete connector if it exists from a prior run
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$CONNECTOR_NAME")
if [ "$HTTP_STATUS" = "200" ]; then
  echo "      Deleting existing connector: $CONNECTOR_NAME"
  curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null
  sleep 2
fi

# Delete and recreate the Kafka topic so offsets start from 0
echo "      Recreating Kafka topic: weather-csv"
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --delete --topic weather-csv 2>/dev/null || true
sleep 2
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --topic weather-csv \
  --partitions 1 --replication-factor 1

# Purge previous files from MinIO so NOS sees only this run's data
echo "      Purging MinIO prefix: demo-csv/raw/weather-csv/"
python3 - <<'PYEOF'
import os, boto3
from botocore.client import Config
endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
user     = os.environ.get("MINIO_ROOT_USER", "minioadmin")
pw       = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
s3 = boto3.client("s3", endpoint_url=endpoint,
    aws_access_key_id=user, aws_secret_access_key=pw,
    config=Config(signature_version="s3v4"), region_name="us-east-1")
resp = s3.list_objects_v2(Bucket="demo-csv", Prefix="raw/weather-csv/")
deleted = 0
for obj in resp.get("Contents", []):
    s3.delete_object(Bucket="demo-csv", Key=obj["Key"])
    deleted += 1
print(f"      Deleted {deleted} object(s) from MinIO.")
PYEOF

# Drop the foreign table and clear weather_obs so each run is idempotent
echo "      Clearing Teradata: weather_nos_ft, weather_obs"
bteq_run /tpt/scripts/demo03_nos_prepare.bteq

# ── Step 2 ─────────────────────────────────────────────────
step "2/6  Registering Kafka Connect S3 Sink connector"
echo "      Connector config: kafka/connect/s3-sink.json"
echo "      Topic: weather-csv → MinIO bucket: demo-csv"
echo "      Format: ByteArray (flush.size=1 → one file per Kafka message)"

curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @kafka/connect/s3-sink.json > /dev/null

# Poll until connector is RUNNING (max ~20s)
echo -n "      Waiting for connector to start"
STARTED=0
for i in $(seq 1 10); do
  sleep 2
  STATE=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
  echo -n "."
  if [ "$STATE" = "RUNNING" ]; then
    echo " RUNNING"
    STARTED=1
    break
  fi
done
if [ "$STARTED" = "0" ]; then
  echo " FAILED (last state: $STATE)"
  echo "      Check: docker compose logs kafka-connect"
  exit 1
fi

# ── Step 3 ─────────────────────────────────────────────────
step "3/6  Publishing weather batches to Kafka"
echo "      Each batch: 5 stations × 6 hourly offsets = 30 rows + CSV header"
echo "      3 Kafka messages → flush.size=1 → 3 CSV files in MinIO"
echo ""
python3 kafka/producers/weather_kafka.py \
  --broker "localhost:${KAFKA_EXTERNAL_PORT:-29092}" \
  --topic  weather-csv \
  --batches  "$WEATHER_BATCHES" \
  --interval "$WEATHER_INTERVAL"

# ── Step 4 ─────────────────────────────────────────────────
step "4/6  Waiting for Kafka Connect to flush files to MinIO"
echo "      flush.size=1 → files land almost immediately after each produce()"
echo "      Sleeping 20s as a safety buffer..."
sleep 20

# Verify expected file count before proceeding
EXPECTED="$WEATHER_BATCHES"
ACTUAL=$(python3 - <<'PYEOF'
import os, boto3
from botocore.client import Config
endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
user     = os.environ.get("MINIO_ROOT_USER", "minioadmin")
pw       = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
s3 = boto3.client("s3", endpoint_url=endpoint,
    aws_access_key_id=user, aws_secret_access_key=pw,
    config=Config(signature_version="s3v4"), region_name="us-east-1")
resp = s3.list_objects_v2(Bucket="demo-csv", Prefix="raw/weather-csv/")
print(len(resp.get("Contents", [])))
PYEOF
)
echo "      Files found in MinIO: $ACTUAL (expected: $EXPECTED)"
if [ "$ACTUAL" -lt "$EXPECTED" ]; then
  echo "      WARNING: fewer files than expected — sleeping 15s more"
  sleep 15
fi

# ── Step 5 ─────────────────────────────────────────────────
step "5/6  NOS: list objects, discover schema, create FOREIGN TABLE"
echo "      A) NOSREAD_KEYS  — list files written by Kafka Connect"
echo "      B) NOSREAD_SCHEMA — infer column names from CSV header"
echo "      C) CREATE FOREIGN TABLE weather_nos_ft"
echo "      D) COUNT(*) sanity check (expect $((WEATHER_BATCHES * 30)) rows)"
echo ""
bteq_run /tpt/scripts/demo03_nos_create.bteq

# ── Step 6 ─────────────────────────────────────────────────
step "6/6  Loading weather_obs from NOS and showing summary"
bteq_run /tpt/scripts/demo03_nos_load.bteq

echo ""
echo "======================================================"
echo "  Demo 3 complete!"
echo ""
echo "  Pipeline summary:"
echo "    $WEATHER_BATCHES Kafka messages (CSV batches)"
echo "    → $ACTUAL files in MinIO  (demo-csv/raw/weather-csv/)"
echo "    → NOS FOREIGN TABLE weather_nos_ft"
echo "    → $((WEATHER_BATCHES * 30)) rows loaded into demo_db.weather_obs"
echo ""
echo "  Try a live NOS query in Teradata Studio:"
echo "    SELECT * FROM ${TD_DATABASE:-demo_db}.weather_nos_ft"
echo "    WHERE station_id = 'EGLL';"
echo ""
echo "  The NOS FOREIGN TABLE remains — re-run steps 1-4 to"
echo "  land more batches, then step 6 to ingest them."
echo "======================================================"
