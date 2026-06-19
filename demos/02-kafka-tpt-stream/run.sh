#!/bin/bash
# ============================================================
# Demo 2: Kafka → Teradata live streaming via TPT STREAM operator
# ============================================================
# Demonstrates:
#   • adsb_producer.py publishing synthetic ADS-B position fixes
#     to Kafka as pipe-delimited text (one message per aircraft per interval)
#   • TPT STREAM operator consuming from Kafka (Format=DELIMITED) and inserting
#     rows into the partitioned adsb_positions table in real time
#   • No Schema Registry required — plain text over Kafka
#
# Prerequisites:
#   docker compose up -d        (kafka and tpt services)
#   bash tpt/scripts/run_setup.sh
#   pip install confluent-kafka fastavro
#
# Run from project root:
#   bash demos/02-kafka-tpt-stream/run.sh
#
# Stop: Ctrl+C — kills both the producer and the TPT job.
# ============================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

KAFKA_EXTERNAL="localhost:${KAFKA_EXTERNAL_PORT:-29092}"
TOPIC="${KAFKA_TOPIC:-adsb-positions}"
INTERVAL="${ADSB_INTERVAL:-5}"

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

echo "======================================================"
echo "  Demo 2: Kafka → Teradata TPT STREAM (ADS-B)"
echo "  Broker (host):      $KAFKA_EXTERNAL"
echo "  Broker (container): kafka:9092"
echo "  Topic:              $TOPIC"
echo "  Interval:           ${INTERVAL}s per aircraft"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/3  Starting ADS-B JSON producer (background)"
python3 kafka/producers/adsb_producer.py \
  --bootstrap "$KAFKA_EXTERNAL" \
  --topic     "$TOPIC" \
  --interval  "$INTERVAL" \
  --format    delimited \
  --continuous &
PRODUCER_PID=$!
echo "      Producer PID: $PRODUCER_PID"

# Ensure producer is killed when the script exits for any reason
trap 'echo ""; echo "Stopping producer (PID $PRODUCER_PID)..."; kill $PRODUCER_PID 2>/dev/null || true' EXIT

# Let a batch accumulate before TPT connects
sleep 5

# ── Step 1b ────────────────────────────────────────────────
step "1b/3  Clearing TPT state from any prior run"
docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh \
  /tpt/scripts/demo02_prepare.bteq
# Remove stale checkpoint files so tbuild starts clean (not as a restart)
docker compose exec -T tpt twbrmcp ttuuser 2>/dev/null || true

# ── Step 2 ─────────────────────────────────────────────────
step "2/3  Running TPT STREAM job (press Ctrl+C to stop)"
echo "      TPT connects to kafka:9092 inside the demo-net network."
docker compose exec -T \
  -e "KAFKA_BOOTSTRAP=kafka:9092" \
  -e "KAFKA_TOPIC=$TOPIC" \
  tpt bash /tpt/scripts/run_tbuild.sh /tpt/tbuild/kafka_stream.tbuild

# ── Step 3 ─────────────────────────────────────────────────
step "3/3  Verifying rows in adsb_positions"
docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh \
  /tpt/scripts/demo02_verify.bteq

echo ""
echo "======================================================"
echo "  Demo 2 complete!"
echo "======================================================"
