#!/bin/bash
# ============================================================
# Demo 4: Kafka → Flink → Iceberg → Teradata OTF
# ============================================================
# Demonstrates:
#   • Flink SQL streaming job consuming ADS-B messages from Kafka
#     and writing them to an Apache Iceberg table (Parquet on MinIO,
#     metadata in Hive Metastore) — a checkpoint every 30s commits
#     an atomic Iceberg snapshot
#   • Teradata querying the live Iceberg table via a DATALAKE object
#     (OTF, 3-part dot notation: demo_iceberg.demo.adsb_positions)
#     — new rows visible within ~30s of ingestion; no ETL, no COPY
#   • Iceberg snapshot history via TD_SNAPSHOTS()
#   • Open-lakehouse pattern: Flink writes, Teradata queries, same files
#
# Continuous streaming mechanism:
#   Flink checkpointing (30s interval) commits atomic Iceberg snapshots.
#   Teradata OTF reads the current snapshot on each SELECT — growing row
#   count visible in real time. Ctrl+C stops the producer; a 35s drain
#   window ensures the final checkpoint commits before job cancellation.
#
# Modes:
#   bash demos/04-kafka-flink-iceberg/run.sh            # continuous (default)
#   bash demos/04-kafka-flink-iceberg/run.sh --bounded  # 200 messages, then stop
#
# Prerequisites:
#   docker compose up -d
#   bash tpt/scripts/run_setup.sh         (creates auth objects in demo_db)
#   docker build -t flink-demo:latest flink/
#   pip install confluent-kafka fastavro
# ============================================================

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

# Activate Python virtual environment if available
[ -f .venv-activate.sh ] && source .venv-activate.sh

KAFKA_EXTERNAL="localhost:${KAFKA_EXTERNAL_PORT:-29092}"
TOPIC="${KAFKA_TOPIC:-adsb-positions}"
FLINK_REST="http://localhost:${FLINK_JOBMANAGER_PORT:-8081}"

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
    -e HOST_IP="$HOST_IP" \
    tpt bash /tpt/scripts/run_bteq.sh "$1"
}

flink_sql() {
  docker compose exec -T flink-jobmanager \
    /opt/flink/bin/sql-client.sh -f "$1"
}

echo "======================================================"
echo "  Demo 4: Kafka → Flink → Iceberg → Teradata OTF"
echo "  Kafka broker (host):      $KAFKA_EXTERNAL"
echo "  Kafka broker (container): kafka:9092"
echo "  Topic:                    $TOPIC"
echo "  Flink REST:               $FLINK_REST"
echo "  HMS (host-facing):        ${HOST_IP}:9083"
echo "  MinIO (host-facing):      ${HOST_IP}:9000"
echo "  Mode:                     $([ "$BOUNDED" = "1" ] && echo "BOUNDED (200 messages)" || echo "CONTINUOUS (Ctrl+C to stop)")"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/4  Resetting prior state"

echo "      Dropping Iceberg table via Flink SQL (removes Parquet files from MinIO)..."
flink_sql /opt/flink/jobs/demo04/drop.sql || true

echo "      Dropping DATALAKE from Teradata..."
bteq_run /tpt/scripts/demo04/datalake_drop.bteq || true

echo "      Recreating Kafka topic '$TOPIC'..."
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --delete --topic "$TOPIC" 2>/dev/null || true
sleep 2
docker compose exec -T kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1 2>/dev/null || true
echo "      Topic recreated."

# ── Step 2 ─────────────────────────────────────────────────
step "2/4  Creating DATALAKE object in Teradata"

echo "      Connecting Teradata to Hive Metastore (${HOST_IP}:9083) and MinIO (${HOST_IP}:9000)..."
bteq_run /tpt/scripts/demo04/datalake_create.bteq

# ── Step 3 ─────────────────────────────────────────────────
step "3/4  Starting Flink job and producer"

PRODUCER_PID=""
JOB_ID=""

if [ "$BOUNDED" = "1" ]; then
  # ── Bounded mode ────────────────────────────────────────
  echo "      Publishing 200 ADS-B messages (10 aircraft × 20 cycles × 1s interval)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$TOPIC" \
    --interval  1 \
    --count     200 \
    --format    delimited
  echo "      Producer done. Submitting Flink batch job (earliest→latest offset)..."

  # Batch mode: sql-client blocks until job FINISHED
  flink_sql /opt/flink/jobs/demo04/batch.sql
  echo "      Flink batch job complete — Iceberg snapshot committed."

else
  # ── Continuous mode ─────────────────────────────────────
  #
  # Streaming INSERT is submitted; sql-client exits and returns the job ID.
  # The INSERT continues running in the Flink cluster until cancelled.
  #
  # Graceful shutdown on Ctrl+C:
  #   1. Kill the producer (no more messages to Kafka)
  #   2. Wait 35s — slightly longer than the 30s checkpoint interval —
  #      so the final Iceberg snapshot (containing the last batch of rows
  #      since the previous checkpoint) has time to commit
  #   3. Cancel the Flink job via the REST API
  #   4. Run the final OTF verify query

  cleanup() {
    echo ""
    echo "── Stopping producer..."
    [ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null || true

    echo "── Waiting 35s for final Iceberg checkpoint to commit..."
    sleep 35

    echo "── Cancelling Flink job ${JOB_ID}..."
    if [ -n "$JOB_ID" ]; then
      curl -s -X PATCH "${FLINK_REST}/jobs/${JOB_ID}?mode=cancel" > /dev/null || true
    fi

    echo ""
    step "4/4  Final snapshot"
    bteq_run /tpt/scripts/demo04/otf_verify.bteq
    echo ""
    echo "======================================================"
    echo "  Demo 4 complete!"
    echo "======================================================"
    exit 0
  }
  trap cleanup INT TERM

  echo "      Submitting Flink streaming job (checkpoint every 30s)..."
  FLINK_OUTPUT=$(flink_sql /opt/flink/jobs/demo04/stream.sql 2>&1)
  echo "$FLINK_OUTPUT"

  # Extract job ID from SQL client output
  JOB_ID=$(echo "$FLINK_OUTPUT" | grep 'Job ID' | sed 's/.*Job ID[: ]*//' | tr -d '[:space:]' | head -1)

  # Fallback: poll REST API for a RUNNING job if output parsing failed
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

  echo "      Starting producer (continuous, 1s interval, 10 aircraft)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$TOPIC" \
    --interval  1 \
    --continuous \
    --format    delimited &
  PRODUCER_PID=$!

  echo ""
  echo "  Streaming — rows visible in Teradata every ~30s (one Iceberg checkpoint)."
  echo "  Press Ctrl+C to stop."
  echo ""

  # Monitoring loop: query Teradata via OTF every 30s.
  # First query is delayed 35s to allow the first checkpoint to complete.
  # Subsequent queries run every 30s (aligned to checkpoint cadence).
  sleep 35
  while kill -0 "$PRODUCER_PID" 2>/dev/null; do
    echo -n "  [$(date -u +'%H:%M:%S UTC')]  "
    bteq_run /tpt/scripts/demo04/otf_query.bteq 2>/dev/null \
      | grep "^STATUS" \
      || echo "(no data yet — waiting for first checkpoint)"
    sleep 30
  done

  cleanup
fi

# ── Step 4 (bounded only) ───────────────────────────────────
step "4/4  Verifying rows via Teradata OTF"
bteq_run /tpt/scripts/demo04/otf_verify.bteq

echo ""
echo "======================================================"
echo "  Demo 4 complete!"
echo "======================================================"
