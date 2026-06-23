#!/bin/bash
# ============================================================
# Demo 5: Flink Lookup Join → Teradata enrichment
# ============================================================
# Demonstrates:
#   • Teradata as an enrichment source for a streaming pipeline
#     — Flink's JDBC Lookup Join reads reference data from the
#     Teradata aircraft_ref table and caches it for 30 seconds
#   • Each ADS-B position message from Kafka is enriched in-flight
#     with registration, aircraft_type, operator, and country fields
#     before being written to Iceberg (MinIO, Hive Metastore catalog)
#   • Teradata queries the enriched Iceberg table via OTF — both
#     origin (lookup source) and destination (OTF query target)
#   • Live cache invalidation: UPDATE a row in aircraft_ref in
#     Teradata and the enriched data changes within 30 seconds
#
# Modes:
#   bash demos/05-flink-td-enrich/run.sh            # continuous (default)
#   bash demos/05-flink-td-enrich/run.sh --bounded  # 200 messages, then stop
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
    -e MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}" \
    -e MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}" \
    tpt bash /tpt/scripts/run_bteq.sh "$1"
}

# Substitute ${VAR} placeholders in a SQL template and call sql-client.
# The flink/jobs/ directory is bind-mounted at /opt/flink/jobs/ in the
# Flink containers, so writing the substituted file there makes it
# immediately accessible without copying into the container.
flink_sql_subst() {
  local template="$1"
  local output="${template%.sql}_sub.sql"
  TD_HOST="$TD_HOST" \
  TD_USER="$TD_USER" \
  TD_PASSWORD="$TD_PASSWORD" \
  TD_DATABASE="${TD_DATABASE:-demo_db}" \
  perl -pe 's/\$\{([^}]+)\}/$ENV{$1}/ge' "$template" > "$output"
  local container_path="/opt/flink/jobs/$(basename "$output")"
  docker compose exec -T flink-jobmanager \
    /opt/flink/bin/sql-client.sh -f "$container_path"
}

flink_sql() {
  docker compose exec -T flink-jobmanager \
    /opt/flink/bin/sql-client.sh -f "$1"
}

echo "======================================================"
echo "  Demo 5: Flink Lookup Join → Teradata enrichment"
echo "  Kafka broker (host):      $KAFKA_EXTERNAL"
echo "  Topic:                    $TOPIC"
echo "  Flink REST:               $FLINK_REST"
echo "  Teradata host:            $TD_HOST"
echo "  Reference table:          ${TD_DATABASE:-demo_db}.aircraft_ref"
echo "  Mode:                     $([ "$BOUNDED" = "1" ] && echo "BOUNDED (200 messages)" || echo "CONTINUOUS (Ctrl+C to stop)")"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/6  Resetting prior state"

echo "      Dropping enriched_positions Iceberg table..."
flink_sql /opt/flink/jobs/demo05_drop.sql || true

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
step "2/6  Setting up Teradata reference table"

echo "      Creating demo_db.aircraft_ref (10 aircraft)..."
bteq_run /tpt/scripts/demo05_setup.bteq

# ── Step 3 ─────────────────────────────────────────────────
step "3/6  Ensuring DATALAKE object is present"

echo "      Creating/verifying demo_iceberg DATALAKE (connects Teradata → MinIO/HMS)..."
bteq_run /tpt/scripts/demo04_datalake_create.bteq

# ── Step 4 ─────────────────────────────────────────────────
step "4/6  Starting Flink lookup-join streaming job"

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

  # Submit bounded job and capture the Job ID (sql-client returns immediately after
  # submission — the job continues running in the cluster). Poll REST until FINISHED.
  BATCH_OUTPUT=$(flink_sql_subst flink/jobs/demo05_batch.sql 2>&1)
  echo "$BATCH_OUTPUT" | grep -v "^WARNING" | grep -v "^Jun "
  BATCH_JOB_ID=$(echo "$BATCH_OUTPUT" | grep 'Job ID' | sed 's/.*Job ID[: ]*//' | tr -d '[:space:]' | head -1)
  if [ -z "$BATCH_JOB_ID" ]; then
    echo "ERROR: Could not parse Flink job ID from output — check Flink UI at $FLINK_REST"
    exit 1
  fi
  echo "      Flink job ID: $BATCH_JOB_ID — waiting for FINISHED state..."
  for i in $(seq 1 60); do
    BATCH_STATE=$(curl -s "${FLINK_REST}/jobs/${BATCH_JOB_ID}" \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','?'))" 2>/dev/null)
    if [ "$BATCH_STATE" = "FINISHED" ]; then
      echo "      Flink batch job FINISHED — Iceberg snapshot committed."
      break
    elif [ "$BATCH_STATE" = "FAILED" ] || [ "$BATCH_STATE" = "CANCELED" ]; then
      echo "ERROR: Flink job entered state $BATCH_STATE — check Flink UI at $FLINK_REST"
      exit 1
    fi
    echo "      [$i] state=$BATCH_STATE — waiting..."
    sleep 3
  done

else
  # ── Continuous mode ─────────────────────────────────────
  #
  # The streaming INSERT is submitted; sql-client exits and returns the job ID.
  # The job continues running in the Flink cluster until cancelled.
  #
  # Graceful shutdown on Ctrl+C:
  #   1. Kill the producer (no more messages to Kafka)
  #   2. Wait 35s — slightly longer than the 30s checkpoint interval —
  #      so the final Iceberg snapshot commits before cancellation
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
    step "6/6  Final verification"
    bteq_run /tpt/scripts/demo05_otf_verify.bteq
    echo ""
    echo "======================================================"
    echo "  Demo 5 complete!"
    echo "======================================================"
    exit 0
  }
  trap cleanup INT TERM

  echo "      Substituting variables into Flink SQL template..."
  echo "      Submitting Flink streaming job (lookup join, checkpoint every 30s)..."
  FLINK_OUTPUT=$(flink_sql_subst flink/jobs/demo05_stream.sql 2>&1)
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

  # ── Step 5 ─────────────────────────────────────────────
  step "5/6  Streaming — press Ctrl+C to stop"

  echo "      Starting producer (continuous, 1s interval, 10 aircraft)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$TOPIC" \
    --interval  1 \
    --continuous \
    --format    delimited &
  PRODUCER_PID=$!

  echo ""
  echo "  Enriched rows visible in Teradata every ~30s (one Iceberg checkpoint)."
  echo "  Each row includes registration, aircraft_type, operator, country"
  echo "  sourced from Teradata aircraft_ref via JDBC Lookup Join."
  echo ""
  echo "  DEMO TIP — live cache invalidation:"
  echo "    UPDATE ${TD_DATABASE:-demo_db}.aircraft_ref WHERE icao24='3f7062'"
  echo "    SET operator='Ryanair DAC';"
  echo "    The change appears in enriched_positions after the 30s cache expires."
  echo ""

  # Monitoring loop: query Teradata via OTF every 30s.
  # First query is delayed 35s to allow the first checkpoint to complete.
  sleep 35
  while kill -0 "$PRODUCER_PID" 2>/dev/null; do
    echo -n "  [$(date -u +'%H:%M:%S UTC')]  "
    bteq_run /tpt/scripts/demo05_otf_query.bteq 2>/dev/null \
      | grep "^STATUS" \
      || echo "(no data yet — waiting for first checkpoint)"
    sleep 30
  done

  cleanup
fi

# ── Step 5 / 6 (bounded only) ──────────────────────────────
step "5/6  Verifying enriched rows via Teradata OTF"
bteq_run /tpt/scripts/demo05_otf_verify.bteq

echo ""
echo "======================================================"
echo "  Demo 5 complete!"
echo "======================================================"
