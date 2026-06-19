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
# TPT reads from offset 0 in the partition (PRESERVE_RESTART_INFO mode).
# The topic is recreated each run so only fresh messages are consumed.
# TPT is started before the producer so it is already connected when
# messages arrive. It ends naturally ~15 seconds after the last message.
# Total runtime ≈ 35 seconds.
# ============================================================

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

KAFKA_EXTERNAL="localhost:${KAFKA_EXTERNAL_PORT:-29092}"
TOPIC="${KAFKA_TOPIC:-adsb-positions}"

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

echo "======================================================"
echo "  Demo 2: Kafka → Teradata TPT STREAM (ADS-B)"
echo "  Broker (host):      $KAFKA_EXTERNAL"
echo "  Broker (container): kafka:9092"
echo "  Topic:              $TOPIC"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/3  Clearing TPT and Kafka state from any prior run"

# Truncate target table and drop STREAM operator log/error tables
docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh \
  /tpt/scripts/demo02_prepare.bteq

# Remove stale TPT checkpoint files
docker compose exec -T tpt twbrmcp ttuuser 2>/dev/null || true

# Delete and recreate the Kafka topic so TPT starts at offset 0 with clean data
# (TPT Kafka Access Module reads from offset 0 when checkpoint is cleared)
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
step "2/3  Starting TPT STREAM job (background) then producing messages"
echo "      TPT connects to kafka:9092 inside the demo-net network."

docker compose exec -T \
  -e "KAFKA_BOOTSTRAP=kafka:9092" \
  -e "KAFKA_TOPIC=$TOPIC" \
  tpt bash /tpt/scripts/run_tbuild.sh /tpt/tbuild/kafka_stream.tbuild &
TBUILD_PID=$!

# Wait for the Kafka Access Module to connect before producing
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

# Wait for tbuild to finish (exits after 15s idle timeout)
wait $TBUILD_PID || true

# ── Step 3 ─────────────────────────────────────────────────
step "3/3  Verifying rows in adsb_positions"
docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh \
  /tpt/scripts/demo02_verify.bteq

echo ""
echo "======================================================"
echo "  Demo 2 complete!"
echo "======================================================"
