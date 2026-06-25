#!/bin/bash
# ============================================================
# Demo 6: Kafka (Avro + Schema Registry) → Flink → TPT STREAM → Teradata
# ============================================================
# Demonstrates:
#   • adsb_producer.py publishing ADS-B positions to Kafka as
#     Confluent wire-format Avro (schema registered in Schema Registry)
#   • Flink SQL deserialising the Avro messages with full type awareness
#     (DOUBLE, INT, BOOLEAN, TIMESTAMP_LTZ) and re-emitting each record
#     as pipe-delimited text to an intermediate Kafka topic
#   • TPT STREAM consuming that intermediate topic and landing rows in
#     Teradata in real time — same commit cadence as Demo 2
#   • The point: Flink earns its place before Teradata, not instead of it
#
# Pipeline:
#   Producer (--format sr-avro)
#       → adsb-avro [Confluent wire Avro]
#           → Flink (avro-confluent → csv/pipe-delimited)
#               → adsb-positions-flink [pipe-delimited text]
#                   → TPT STREAM
#                       → Teradata: adsb_positions_06
#
# Modes:
#   bash demos/06-flink-avro-tpt/run.sh            # continuous (default)
#   bash demos/06-flink-avro-tpt/run.sh --bounded  # 100 messages, then stop
#
# Prerequisites:
#   docker compose build flink-jobmanager flink-taskmanager  # if not already built
#   docker compose up -d
#   bash tpt/scripts/run_setup.sh
#   pip install confluent-kafka fastavro
# ============================================================

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

KAFKA_EXTERNAL="localhost:${KAFKA_EXTERNAL_PORT:-29092}"
AVRO_TOPIC="adsb-avro"
FLINK_TOPIC="adsb-positions-flink"
FLINK_REST="http://localhost:${FLINK_JOBMANAGER_PORT:-8081}"
SR_URL="http://localhost:${SCHEMA_REGISTRY_PORT:-8082}"

BOUNDED=0
for arg in "$@"; do
  [ "$arg" = "--bounded" ] && BOUNDED=1
done

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

bteq_run() {
  docker compose exec -T \
    -e TD_HOST="$TD_HOST" \
    -e TD_USER="$TD_USER" \
    -e TD_PASSWORD="$TD_PASSWORD" \
    -e TD_DATABASE="${TD_DATABASE:-demo_db}" \
    tpt bash /tpt/scripts/run_bteq.sh "$1"
}

flink_sql() {
  docker compose exec -T flink-jobmanager \
    /opt/flink/bin/sql-client.sh -f "$1"
}

echo "======================================================"
echo "  Demo 6: Kafka Avro → Flink → TPT STREAM (ADS-B)"
echo "  Kafka broker (host):      $KAFKA_EXTERNAL"
echo "  Kafka broker (container): kafka:9092"
echo "  Avro topic:               $AVRO_TOPIC"
echo "  Flink output topic:       $FLINK_TOPIC"
echo "  Flink REST:               $FLINK_REST"
echo "  Schema Registry (host):   $SR_URL"
echo "  Schema Registry (ctr):    http://schema-registry:8081"
echo "  Mode:                     $([ "$BOUNDED" = "1" ] && echo "BOUNDED (100 messages)" || echo "CONTINUOUS (Ctrl+C to stop)")"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/4  Resetting prior state"

echo "      Cancelling any running Flink jobs from prior runs..."
curl -s "${FLINK_REST}/jobs" 2>/dev/null \
  | python3 -c "
import sys, json
try:
    jobs = json.load(sys.stdin).get('jobs', [])
    for j in jobs:
        if j['status'] == 'RUNNING':
            print(j['id'])
except Exception:
    pass
" | while read -r jid; do
    echo "      Cancelling Flink job $jid"
    curl -s -X PATCH "${FLINK_REST}/jobs/${jid}?mode=cancel" > /dev/null || true
  done
sleep 2

bteq_run /tpt/scripts/demo06/prepare.bteq

docker compose exec -T tpt twbrmcp ttuuser 2>/dev/null || true

for topic in "$AVRO_TOPIC" "$FLINK_TOPIC"; do
  echo "      Recreating Kafka topic '$topic'..."
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --delete --topic "$topic" 2>/dev/null || true
done
sleep 2
for topic in "$AVRO_TOPIC" "$FLINK_TOPIC"; do
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server kafka:9092 \
    --create --topic "$topic" \
    --partitions 1 --replication-factor 1 2>/dev/null || true
  echo "      Topic '$topic' recreated."
done

# ── Step 2 ─────────────────────────────────────────────────
step "2/4  Starting Flink streaming job"

echo "      Copying SQL to Flink container..."
docker compose cp flink/jobs/demo06/stream.sql flink-jobmanager:/opt/flink/jobs/demo06_stream.sql

if [ "$BOUNDED" = "1" ]; then
  # Bounded mode: submit batch job after producer finishes (step 3 below).
  # Copy now so it's ready.
  docker compose cp flink/jobs/demo06/batch.sql flink-jobmanager:/opt/flink/jobs/demo06_batch.sql
  echo "      Bounded mode: Flink batch job will be submitted after producer completes."
else
  JOB_ID=""

  FLINK_OUTPUT=$(flink_sql /opt/flink/jobs/demo06_stream.sql 2>&1)
  echo "$FLINK_OUTPUT"

  JOB_ID=$(echo "$FLINK_OUTPUT" | grep -i 'Job ID' | sed 's/.*Job ID[: ]*//' | tr -d '[:space:]' | head -1)

  if [ -z "$JOB_ID" ]; then
    echo "      Could not parse Job ID from output — polling REST API..."
    sleep 5
    JOB_ID=$(curl -s "${FLINK_REST}/jobs" 2>/dev/null \
      | python3 -c "
import sys, json
try:
    jobs = json.load(sys.stdin).get('jobs', [])
    running = [j['id'] for j in jobs if j['status'] == 'RUNNING']
    print(running[0] if running else '')
except Exception:
    print('')
")
  fi

  if [ -z "$JOB_ID" ]; then
    echo "ERROR: Could not determine Flink job ID. Check Flink UI at $FLINK_REST"
    exit 1
  fi

  echo "      Flink job ID: $JOB_ID"
  echo "      Waiting 5s for job to enter RUNNING state..."
  sleep 5
fi

# ── Step 3 ─────────────────────────────────────────────────
step "3/4  Starting TPT STREAM and producer"

TBUILD_PID=""
PRODUCER_PID=""

if [ "$BOUNDED" = "1" ]; then
  # ── Bounded mode ─────────────────────────────────────────
  echo "      Publishing 100 ADS-B messages (10 aircraft × 10 cycles × 1s interval)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$AVRO_TOPIC" \
    --interval  1 \
    --count     100 \
    --format    sr-avro \
    --registry  "$SR_URL"
  echo "      Producer done. Submitting Flink batch job..."

  FLINK_OUTPUT=$(flink_sql /opt/flink/jobs/demo06_batch.sql 2>&1)
  echo "$FLINK_OUTPUT"
  BATCH_JOB_ID=$(echo "$FLINK_OUTPUT" | grep 'Job ID' | sed 's/.*Job ID[: ]*//' | tr -d '[:space:]' | head -1)
  echo "      Waiting 10s for Flink batch job to complete..."
  sleep 10

  echo "      Starting TPT STREAM (consuming Flink output, idle timeout 15s)..."
  docker compose exec -T \
    -e "KAFKA_BOOTSTRAP=kafka:9092" \
    -e "KAFKA_TOPIC=$FLINK_TOPIC" \
    -e "KAFKA_IDLE_TIMEOUT=15" \
    tpt bash /tpt/scripts/run_tbuild.sh /tpt/tbuild/kafka_stream_06.tbuild
  echo "      TPT STREAM complete."

  echo "      Cancelling Flink batch job..."
  [ -n "$BATCH_JOB_ID" ] && curl -s -X PATCH "${FLINK_REST}/jobs/${BATCH_JOB_ID}?mode=cancel" > /dev/null || true

else
  # ── Continuous mode ───────────────────────────────────────
  cleanup() {
    echo ""
    echo "── Stopping producer..."
    [ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null || true

    echo "── Waiting for TPT to drain (up to 35s after Flink goes quiet)..."
    wait "$TBUILD_PID" 2>/dev/null || true

    echo "── Cancelling Flink job ${JOB_ID}..."
    if [ -n "$JOB_ID" ]; then
      curl -s -X PATCH "${FLINK_REST}/jobs/${JOB_ID}?mode=cancel" > /dev/null || true
    fi

    echo ""
    step "4/4  Final row count"
    bteq_run /tpt/scripts/demo06/verify.bteq
    echo ""
    echo "======================================================"
    echo "  Demo 6 complete!"
    echo "======================================================"
    exit 0
  }
  trap cleanup INT TERM

  echo "      Starting TPT STREAM (topic: $FLINK_TOPIC, latency flush -l 5, idle timeout 30s)..."
  docker compose exec -T \
    -e "KAFKA_BOOTSTRAP=kafka:9092" \
    -e "KAFKA_TOPIC=$FLINK_TOPIC" \
    -e "KAFKA_IDLE_TIMEOUT=30" \
    tpt bash /tpt/scripts/run_tbuild.sh /tpt/tbuild/kafka_stream_06.tbuild -l 5 &
  TBUILD_PID=$!

  echo "      Waiting 5s for TPT to connect..."
  sleep 5

  echo "      Starting producer (continuous, 1s interval, sr-avro format)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$AVRO_TOPIC" \
    --interval  1 \
    --continuous \
    --format    sr-avro \
    --registry  "$SR_URL" &
  PRODUCER_PID=$!

  echo ""
  echo "  Streaming — rows land in Teradata every ~5s (TPT latency flush)."
  echo "  Press Ctrl+C to stop."
  echo ""

  while kill -0 "$TBUILD_PID" 2>/dev/null && kill -0 "$PRODUCER_PID" 2>/dev/null; do
    sleep 10
    echo -n "  [$(date -u +'%H:%M:%S UTC')]  "
    bteq_run /tpt/scripts/demo06/status.bteq 2>/dev/null \
      | grep "^STATUS" \
      || echo "?"
  done

  cleanup
fi

# ── Step 4 (bounded only) ───────────────────────────────────
step "4/4  Verifying rows in adsb_positions_06"
bteq_run /tpt/scripts/demo06/verify.bteq

echo ""
echo "======================================================"
echo "  Demo 6 complete!"
echo "======================================================"
