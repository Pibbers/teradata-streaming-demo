#!/bin/bash
# ============================================================
# Demo 2: Kafka → Teradata live streaming via TPT STREAM operator
# ============================================================
# Demonstrates:
#   • adsb_producer.py publishing synthetic ADS-B position fixes
#     continuously to Kafka as pipe-delimited text
#   • TPT STREAM operator consuming from Kafka in real time
#   • Rows visible in Teradata within seconds — not at job end
#   • Graceful shutdown: Ctrl+C kills the producer; TPT drains
#     remaining buffered messages then exits; final count printed
#
# Continuous streaming mechanism (tbuild -l):
#   The STREAM operator only commits when its internal buffers are
#   flushed.  Without -l, flush happens at end-of-source (job end).
#   The -l <seconds> flag (TPT Reference B035-2436, §tbuild) forces
#   a periodic latency flush even with a live Kafka feed — rows land
#   in Teradata every ~5 seconds throughout the run.
#   Source: Teradata PT Reference Guide 20.00, B035-2436-103K.
#
# Modes:
#   bash demos/02-kafka-tpt-stream/run.sh            # continuous (default)
#   bash demos/02-kafka-tpt-stream/run.sh --bounded  # 100 messages, then stop
#
# Prerequisites:
#   docker compose up -d        (kafka and tpt services)
#   bash tpt/scripts/run_setup.sh
#   pip install confluent-kafka fastavro
# ============================================================

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

KAFKA_EXTERNAL="localhost:${KAFKA_EXTERNAL_PORT:-29092}"
TOPIC="${KAFKA_TOPIC:-adsb-positions}"

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

echo "======================================================"
echo "  Demo 2: Kafka → Teradata TPT STREAM (ADS-B)"
echo "  Broker (host):      $KAFKA_EXTERNAL"
echo "  Broker (container): kafka:9092"
echo "  Topic:              $TOPIC"
echo "  Mode:               $([ "$BOUNDED" = "1" ] && echo "BOUNDED (100 messages)" || echo "CONTINUOUS (Ctrl+C to stop)")"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/3  Clearing TPT and Kafka state from any prior run"

docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh \
  /tpt/scripts/demo02_prepare.bteq

docker compose exec -T tpt twbrmcp ttuuser 2>/dev/null || true

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
step "2/3  Starting TPT STREAM job and producer"

TBUILD_PID=""
PRODUCER_PID=""

if [ "$BOUNDED" = "1" ]; then
  # ── Bounded mode (original behaviour) ──────────────────
  # No -l needed: STREAM flushes when the Kafka idle timeout fires.
  echo "      TPT connects to kafka:9092 inside the demo-net network."

  docker compose exec -T \
    -e "KAFKA_BOOTSTRAP=kafka:9092" \
    -e "KAFKA_TOPIC=$TOPIC" \
    -e "KAFKA_IDLE_TIMEOUT=15" \
    tpt bash /tpt/scripts/run_tbuild.sh /tpt/tbuild/kafka_stream.tbuild &
  TBUILD_PID=$!

  echo "      Waiting 5 seconds for TPT to connect..."
  sleep 5

  echo ""
  echo "      Publishing 100 ADS-B messages (10 aircraft × 10 cycles × 1s interval)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$TOPIC" \
    --interval  1 \
    --count     100 \
    --format    delimited
  echo "      Producer done. TPT will exit ~15 seconds after the last message."

  wait $TBUILD_PID || true

else
  # ── Continuous mode ─────────────────────────────────────
  #
  # -l 5: tbuild latency interval — the STREAM operator flushes its
  #   internal buffers and commits rows to Teradata every 5 seconds,
  #   even while the Kafka producer is still running.  Without -l, the
  #   flush only happens when the source signals end-of-data (Kafka idle
  #   timeout), so rows would never be visible during a live run.
  #   Reference: Teradata PT Reference Guide 20.00, B035-2436-103K §tbuild.
  #
  # KAFKA_IDLE_TIMEOUT=30: how long the Kafka Access Module waits after
  #   the last message before signalling end-of-stream.  This becomes
  #   the graceful drain window after the producer is killed (Ctrl+C).

  cleanup() {
    echo ""
    echo "── Stopping producer..."
    [ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null || true
    echo "── Waiting for TPT to drain (up to 30 s)..."
    wait "$TBUILD_PID" 2>/dev/null || true
    echo ""
    step "3/3  Final row count"
    bteq_run /tpt/scripts/demo02_verify.bteq
    echo ""
    echo "======================================================"
    echo "  Demo 2 complete!"
    echo "======================================================"
    exit 0
  }
  trap cleanup INT TERM

  echo "      Starting TPT (KAFKA_IDLE_TIMEOUT=30, latency flush -l 5)..."
  docker compose exec -T \
    -e "KAFKA_BOOTSTRAP=kafka:9092" \
    -e "KAFKA_TOPIC=$TOPIC" \
    -e "KAFKA_IDLE_TIMEOUT=30" \
    tpt bash /tpt/scripts/run_tbuild.sh /tpt/tbuild/kafka_stream.tbuild -l 5 &
  TBUILD_PID=$!

  echo "      Waiting 5 seconds for TPT to connect..."
  sleep 5

  echo "      Starting producer (continuous, 1 s interval, 10 aircraft)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$TOPIC" \
    --interval  1 \
    --continuous \
    --format    delimited &
  PRODUCER_PID=$!

  echo ""
  echo "  Streaming — press Ctrl+C to stop."
  echo ""

  # Monitoring loop: show a heartbeat every 10 s
  while kill -0 "$TBUILD_PID" 2>/dev/null && kill -0 "$PRODUCER_PID" 2>/dev/null; do
    sleep 10
    echo -n "  [$(date -u +'%H:%M:%S UTC')]  "
    bteq_run /tpt/scripts/demo02_status.bteq 2>/dev/null \
      | grep "^STATUS" \
      || echo "?"
  done

  cleanup
fi

# ── Step 3 (bounded only) ───────────────────────────────────
step "3/3  Verifying rows in adsb_positions"
bteq_run /tpt/scripts/demo02_verify.bteq

echo ""
echo "======================================================"
echo "  Demo 2 complete!"
echo "======================================================"
