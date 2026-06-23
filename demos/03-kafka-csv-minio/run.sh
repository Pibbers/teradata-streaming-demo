#!/bin/bash
# ============================================================
# Demo 3: Kafka → Kafka Connect S3 Sink → MinIO CSV → Teradata NOS
# ============================================================
# Demonstrates:
#   • weather_kafka.py publishing CSV batch messages every 5 minutes
#   • Kafka Connect S3 Sink (ByteArrayFormat, TimeBasedPartitioner) writing
#     each Kafka message as a file under a plain time partition:
#       demo-csv/raw/weather-csv/YYYY-MM-DD/HH/
#   • READ_NOS with NOSREAD_KEYS to list files written by Kafka Connect
#   • FOREIGN TABLE with PATHPATTERN enabling NOS partition pruning
#   • Incremental INSERT: only the current-hour partition is loaded;
#     prior hours in weather_obs are left intact
#
# Architecture:
#   weather_kafka.py → Kafka (weather-csv) → Kafka Connect S3 Sink
#       → MinIO (YYYY-MM-DD/HH/)
#       → NOS FOREIGN TABLE (PATHPATTERN string pruning)
#       → INSERT INTO weather_obs WHERE $var3 = 'YYYY-MM-DD' AND $var4 = 'HH'
#
# Usage:
#   bash demos/03-kafka-csv-minio/run.sh           # accumulate this hour
#   bash demos/03-kafka-csv-minio/run.sh --fresh   # purge MinIO + all rows first
#
# Prerequisites:
#   docker compose up -d   (kafka, kafka-connect, minio, minio-init, tpt)
#   bash tpt/scripts/run_setup.sh
#   pip install confluent-kafka boto3
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
WEATHER_INTERVAL="${WEATHER_INTERVAL:-300}"   # 5 minutes between batches

FRESH=0
for arg in "$@"; do
  [ "$arg" = "--fresh" ] && FRESH=1
done

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

# Wrapper: run a BTEQ script inside the TPT container, forwarding current
# env vars (including date/hour partition) via -e flags so that run_bteq.sh's
# perl substitution uses values from .env rather than the container's startup env.
bteq_run() {
  docker compose exec -T \
    -e TD_HOST="$TD_HOST" \
    -e TD_USER="$TD_USER" \
    -e TD_PASSWORD="$TD_PASSWORD" \
    -e TD_DATABASE="${TD_DATABASE:-demo_db}" \
    -e HOST_IP="$HOST_IP" \
    -e MINIO_ROOT_USER="$MINIO_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_PASS" \
    -e CURRENT_YEAR="$CURRENT_YEAR" \
    -e CURRENT_MONTH="$CURRENT_MONTH" \
    -e CURRENT_DAY="$CURRENT_DAY" \
    -e CURRENT_HOUR="$CURRENT_HOUR" \
    tpt bash /tpt/scripts/run_bteq.sh "$1"
}

# Capture UTC date/hour once — this is the partition Kafka Connect will write
# and the partition NOS will prune to when loading weather_obs.
CURRENT_YEAR=$(date -u +%Y)
CURRENT_MONTH=$(date -u +%m)
CURRENT_DAY=$(date -u +%d)
CURRENT_HOUR=$(date -u +%H)
CURRENT_PARTITION="${CURRENT_YEAR}-${CURRENT_MONTH}-${CURRENT_DAY}/${CURRENT_HOUR}"

echo "======================================================"
echo "  Demo 3: Kafka → Kafka Connect → MinIO → NOS"
echo "  MinIO endpoint:     $MINIO_ENDPOINT"
echo "  Kafka Connect:      $CONNECT_URL"
echo "  Current partition:  $CURRENT_PARTITION"
echo "  Weather batches:    $WEATHER_BATCHES  (interval: ${WEATHER_INTERVAL}s)"
echo "  Mode:               $([ "$FRESH" = "1" ] && echo "FRESH (purge all)" || echo "ACCUMULATE (add to existing)")"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/6  Preparing — clean up prior connector and topic"

# Delete connector if it exists from a prior run
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$CONNECTOR_NAME")
if [ "$HTTP_STATUS" = "200" ]; then
  echo "      Deleting existing connector: $CONNECTOR_NAME"
  curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null
  sleep 2
fi

# Delete and recreate the Kafka topic so offsets reset to 0
echo "      Recreating Kafka topic: weather-csv"
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --delete --topic weather-csv 2>/dev/null || true
sleep 2
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --topic weather-csv \
  --partitions 1 --replication-factor 1

if [ "$FRESH" = "1" ]; then
  echo "      --fresh: purging all objects from MinIO demo-csv/raw/weather-csv/"
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
  echo "      --fresh: dropping weather_nos_ft and purging all weather_obs rows"
  bteq_run /tpt/scripts/demo03/nos_fresh.bteq
else
  # Preserve the existing FOREIGN TABLE across runs; create it only if missing.
  echo "      Preserving weather_nos_ft if it already exists"
fi

# ── Step 2 ─────────────────────────────────────────────────
step "2/6  Registering Kafka Connect S3 Sink connector"
echo "      Connector config: kafka/connect/s3-sink.json"
echo "      Topic: weather-csv → MinIO bucket: demo-csv"
echo "      Partitioner: TimeBasedPartitioner → $CURRENT_PARTITION/"

curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @kafka/connect/s3-sink.json > /dev/null

# Poll until connector is RUNNING (max ~20s)
echo -n "      Waiting for connector+task to start"
STARTED=0
for i in $(seq 1 15); do
  sleep 2
  STATUS_JSON=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" 2>/dev/null || echo "{}")
  CONN_STATE=$(echo "$STATUS_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('connector',{}).get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
  TASK_STATE=$(echo "$STATUS_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); tasks=d.get('tasks',[]); print(tasks[0].get('state','NOTSTARTED') if tasks else 'NOTSTARTED')" 2>/dev/null || echo "UNKNOWN")
  echo -n "."
  if [ "$CONN_STATE" = "RUNNING" ] && [ "$TASK_STATE" = "RUNNING" ]; then
    echo " RUNNING"
    STARTED=1
    break
  fi
  if [ "$TASK_STATE" = "FAILED" ]; then
    echo " TASK FAILED"
    TRACE=$(echo "$STATUS_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); tasks=d.get('tasks',[]); print(tasks[0].get('trace','?')[:300] if tasks else '?')" 2>/dev/null)
    echo "      $TRACE"
    echo "      Check: docker compose logs kafka-connect"
    exit 1
  fi
done
if [ "$STARTED" = "0" ]; then
  echo " FAILED (connector=$CONN_STATE task=$TASK_STATE)"
  echo "      Check: docker compose logs kafka-connect"
  exit 1
fi

# ── Step 3 ─────────────────────────────────────────────────
step "3/6  Publishing weather batches to Kafka"
echo "      Each batch: 5 stations × 6 hourly offsets = 30 rows + CSV header"
echo "      $WEATHER_BATCHES messages → flush.size=1 → $WEATHER_BATCHES files in $CURRENT_PARTITION/"
echo ""
python3 kafka/producers/weather_kafka.py \
  --broker "localhost:${KAFKA_EXTERNAL_PORT:-29092}" \
  --topic  weather-csv \
  --batches  "$WEATHER_BATCHES" \
  --interval "$WEATHER_INTERVAL"

# ── Step 4 ─────────────────────────────────────────────────
step "4/6  Waiting for Kafka Connect to flush files to MinIO"
echo "      TimeBasedPartitioner + flush.size=1: files land within seconds of each produce()"
echo "      Polling for $WEATHER_BATCHES file(s) in MinIO (timeout: 120s)..."

EXPECTED="$WEATHER_BATCHES"
ACTUAL=0
WAIT_MAX=120
WAITED=0
while [ "$ACTUAL" -lt "$EXPECTED" ] && [ "$WAITED" -lt "$WAIT_MAX" ]; do
  sleep 5
  WAITED=$((WAITED + 5))
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
  echo "      [${WAITED}s] Files in MinIO: $ACTUAL / $EXPECTED"
done

if [ "$ACTUAL" -lt "$EXPECTED" ]; then
  echo "      WARNING: only $ACTUAL of $EXPECTED files found after ${WAIT_MAX}s — proceeding anyway"
fi

# ── Step 5 ─────────────────────────────────────────────────
step "5/6  NOS: list objects, create FOREIGN TABLE with PATHPATTERN, count rows"
echo "      A) NOSREAD_KEYS  — list files (expect $WEATHER_BATCHES .bin files under $CURRENT_PARTITION/)"
echo "      B) CREATE FOREIGN TABLE weather_nos_ft with PATHPATTERN"
echo "      C) COUNT(*) over the full FOREIGN TABLE (all partitions)"
echo ""
bteq_run /tpt/scripts/demo03/nos_create.bteq

# ── Step 6 ─────────────────────────────────────────────────
step "6/6  Incremental load: partition $CURRENT_PARTITION → weather_obs"
echo "      WHERE \$var3 = '${CURRENT_YEAR}-${CURRENT_MONTH}-${CURRENT_DAY}' AND \$var4 = '${CURRENT_HOUR}'"
echo "      Prior hours in weather_obs are untouched."
echo ""
bteq_run /tpt/scripts/demo03/nos_load.bteq

echo ""
echo "======================================================"
echo "  Demo 3 complete!"
echo ""
echo "  Pipeline summary:"
echo "    $WEATHER_BATCHES Kafka messages"
echo "    → MinIO: demo-csv/raw/weather-csv/$CURRENT_PARTITION/"
echo "    → NOS FOREIGN TABLE weather_nos_ft  (PATHPATTERN string pruning)"
echo "    → $((WEATHER_BATCHES * 30)) new rows loaded into demo_db.weather_obs"
echo "       (accumulates across runs; prior hours are preserved)"
echo ""
echo "  Run again (same or different hour) to accumulate more partitions."
echo "  Run with --fresh to purge MinIO and start from a clean slate."
echo ""
echo "  Try in Teradata Studio:"
echo "    -- Current-hour partition only (PATHPATTERN path pruning):"
echo "    SELECT * FROM ${TD_DATABASE:-demo_db}.weather_nos_ft"
echo "      WHERE \$var3 = '${CURRENT_YEAR}-${CURRENT_MONTH}-${CURRENT_DAY}'"
echo "        AND \$var4 = '${CURRENT_HOUR}';"
echo "======================================================"
