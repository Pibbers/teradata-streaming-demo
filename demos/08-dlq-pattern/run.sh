#!/bin/bash
# ============================================================
# Demo 8: Kafka Connect JDBC Sink — Dead-Letter Queue Pattern
# ============================================================
# Demonstrates:
#   • adsb_producer.py publishing ADS-B positions as Connect JSON
#     with --inject-errors 5: every 5th message has latitude=null
#   • Kafka Connect JDBC Sink with errors.tolerance=all routing
#     invalid rows to a dead-letter topic instead of failing
#   • Valid rows land in adsb_positions_08 (Teradata)
#   • Bad rows land in adsb-positions-dlq-demo.dlq (Kafka) with
#     full error context headers: exception class, message, offset
#
# Architecture:
#   adsb_producer.py (--format connect-json --inject-errors 5)
#       → Kafka (adsb-positions-dlq-demo)
#       → Kafka Connect JDBC Sink (demo08-td-jdbc-sink-dlq)
#           → [valid]  Teradata adsb_positions_08
#           → [errors] Kafka adsb-positions-dlq-demo.dlq
#
# Modes:
#   bash demos/08-dlq-pattern/run.sh              # bounded (100 msgs, ~20 errors)
#   bash demos/08-dlq-pattern/run.sh --continuous # until Ctrl+C
#
# Prerequisites:
#   docker compose up -d   (kafka, kafka-connect, tpt)
#   bash tpt/scripts/run_setup.sh
#   pip install confluent-kafka
# ============================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

# Activate Python virtual environment if available
[ -f .venv-activate.sh ] && source .venv-activate.sh

KAFKA_EXTERNAL="localhost:${KAFKA_EXTERNAL_PORT:-29092}"
CONNECT_URL="http://localhost:${KAFKA_CONNECT_PORT:-8083}"
CONNECTOR_NAME="demo08-td-jdbc-sink-dlq"
TOPIC="adsb-positions-dlq-demo"
DLQ_TOPIC="adsb-positions-dlq-demo.dlq"
MSG_COUNT="${ADSB_COUNT:-100}"
MSG_INTERVAL="${ADSB_INTERVAL:-1}"
INJECT_ERRORS=5   # inject a bad record every N messages

CONTINUOUS=0
for arg in "$@"; do
  [ "$arg" = "--continuous" ] && CONTINUOUS=1
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
echo "  Demo 8: Kafka Connect JDBC Sink — DLQ Pattern"
echo "  Broker (host):     $KAFKA_EXTERNAL"
echo "  Kafka Connect:     $CONNECT_URL"
echo "  Topic:             $TOPIC"
echo "  DLQ topic:         $DLQ_TOPIC"
echo "  Connector:         $CONNECTOR_NAME"
echo "  Error injection:   every ${INJECT_ERRORS}th message (latitude=null)"
echo "  Mode:              $([ "$CONTINUOUS" = "1" ] && echo "CONTINUOUS (Ctrl+C to stop)" || echo "BOUNDED ($MSG_COUNT messages)")"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/5  Writing Teradata credentials for FileConfigProvider"

cat > kafka/connect/td-credentials.properties <<EOF
td_host=${TD_HOST}
td_user=${TD_USER}
td_password=${TD_PASSWORD}
td_database=${TD_DATABASE:-demo_db}
EOF
echo "      Written: kafka/connect/td-credentials.properties"

# ── Step 2 ─────────────────────────────────────────────────
step "2/5  Resetting connector, topics, and target table"

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$CONNECTOR_NAME")
if [ "$HTTP_STATUS" = "200" ]; then
  echo "      Deleting existing connector: $CONNECTOR_NAME"
  curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null
  sleep 2
fi

for t in "$TOPIC" "$DLQ_TOPIC"; do
  docker compose exec -T kafka kafka-topics \
    --bootstrap-server localhost:9092 --delete --topic "$t" 2>/dev/null || true
done
sleep 2
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --topic "$DLQ_TOPIC" \
  --partitions 1 --replication-factor 1
echo "      Topics created: $TOPIC, $DLQ_TOPIC"

echo "      Clearing adsb_positions_08..."
bteq_run /tpt/scripts/demo08/prepare.bteq

# ── Step 3 ─────────────────────────────────────────────────
step "3/5  Registering JDBC Sink connector with DLQ config"
echo "      Config: kafka/connect/td-jdbc-sink-dlq.json"
echo "      errors.tolerance=all → bad rows go to $DLQ_TOPIC, connector stays RUNNING"

curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @kafka/connect/td-jdbc-sink-dlq.json > /dev/null

echo -n "      Waiting for connector+task to start"
STARTED=0
for i in $(seq 1 20); do
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
    TRACE=$(echo "$STATUS_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); tasks=d.get('tasks',[]); print(tasks[0].get('trace','?')[:500] if tasks else '?')" 2>/dev/null)
    echo "      $TRACE"
    echo ""
    echo "      Troubleshooting tips:"
    echo "        • 'No suitable driver' → terajdbc JAR classloader issue"
    echo "        • 'Authentication failed' → check .env values"
    echo "        • 'Table not found' → run: bash tpt/scripts/run_setup.sh"
    echo "      Check: docker compose logs kafka-connect"
    exit 1
  fi
done
if [ "$STARTED" = "0" ]; then
  echo " TIMED OUT (connector=$CONN_STATE task=$TASK_STATE)"
  echo "      Check: docker compose logs kafka-connect"
  exit 1
fi

# ── Step 4 ─────────────────────────────────────────────────
step "4/5  Publishing ADS-B positions (with injected errors)"

PRODUCER_PID=""

inspect_dlq() {
  echo ""
  echo "── DLQ sample (up to 3 records with error headers) ───"
  docker compose exec -T kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$DLQ_TOPIC" \
    --from-beginning \
    --max-messages 3 \
    --property print.headers=true \
    --property print.timestamp=true \
    --timeout-ms 5000 2>/dev/null \
    | python3 -c "
import sys
for line in sys.stdin:
    line = line.rstrip()
    if line.startswith('__connect.errors'):
        # Format header key=value pairs one per line for readability
        for kv in line.split(','):
            k, _, v = kv.partition(':')
            k = k.strip(); v = v.strip()
            if k in ('__connect.errors.exception.class.name',
                     '__connect.errors.exception.message',
                     '__connect.errors.topic',
                     '__connect.errors.offset'):
                print(f'      {k}: {v[:120]}')
    elif line.startswith('CreateTime') or (line.startswith('{') and '\"payload\"' in line):
        print(f'  {line[:120]}')
" 2>/dev/null || echo "      (no DLQ records yet or consumer timed out)"
}

cleanup() {
  echo ""
  echo "── Stopping producer..."
  [ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null || true
  wait "$PRODUCER_PID" 2>/dev/null || true
  inspect_dlq
  echo ""
  echo "── Final row count (Teradata)"
  bteq_run /tpt/scripts/demo08/verify.bteq
  echo ""
  echo "======================================================"
  echo "  Demo 8 complete!"
  echo "======================================================"
  exit 0
}
trap cleanup INT TERM

if [ "$CONTINUOUS" = "1" ]; then
  echo "      Publishing continuously (${MSG_INTERVAL}s interval, 10 aircraft, every ${INJECT_ERRORS}th msg bad)..."
  echo "      Valid rows → adsb_positions_08 | Bad rows → $DLQ_TOPIC"
  echo "      Press Ctrl+C to stop."
  echo ""

  python3 kafka/producers/adsb_producer.py \
    --bootstrap     "$KAFKA_EXTERNAL" \
    --topic         "$TOPIC" \
    --interval      "$MSG_INTERVAL" \
    --continuous \
    --format        connect-json \
    --inject-errors "$INJECT_ERRORS" &
  PRODUCER_PID=$!

  while kill -0 "$PRODUCER_PID" 2>/dev/null; do
    sleep 10
    echo -n "  [$(date -u +'%H:%M:%S UTC')]  "
    bteq_run /tpt/scripts/demo08/status.bteq 2>/dev/null \
      | grep "^STATUS" \
      || echo "?"
  done

  cleanup

else
  echo "      Publishing $MSG_COUNT messages (10 aircraft × $((MSG_COUNT / 10)) cycles, ${MSG_INTERVAL}s interval)..."
  echo "      Every ${INJECT_ERRORS}th message has latitude=null → routed to DLQ."
  EXPECTED_GOOD=$(( MSG_COUNT - MSG_COUNT / INJECT_ERRORS ))
  echo "      Expected: ~${EXPECTED_GOOD} good rows in Teradata, ~$((MSG_COUNT / INJECT_ERRORS)) records in DLQ."
  echo ""

  python3 kafka/producers/adsb_producer.py \
    --bootstrap     "$KAFKA_EXTERNAL" \
    --topic         "$TOPIC" \
    --count         "$MSG_COUNT" \
    --interval      "$MSG_INTERVAL" \
    --format        connect-json \
    --inject-errors "$INJECT_ERRORS"

  echo ""
  echo "      Producer done. Waiting for JDBC Sink to flush to Teradata..."
  ACTUAL=0
  WAIT_MAX=60
  WAITED=0
  while [ "$ACTUAL" -lt "$EXPECTED_GOOD" ] && [ "$WAITED" -lt "$WAIT_MAX" ]; do
    sleep 3
    WAITED=$((WAITED + 3))
    ACTUAL=$(bteq_run /tpt/scripts/demo08/status.bteq 2>/dev/null \
      | grep "^STATUS" \
      | sed "s/.*rows=\([0-9]*\).*/\1/" || echo "0")
    echo "      [${WAITED}s] Rows in adsb_positions_08: $ACTUAL / ~${EXPECTED_GOOD}"
  done

  # ── Step 5 ───────────────────────────────────────────────
  step "5/5  Inspecting DLQ and verifying Teradata"

  inspect_dlq

  echo ""
  echo "── Teradata row count ─────────────────────────────────"
  bteq_run /tpt/scripts/demo08/verify.bteq

  DLQ_COUNT=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic "$DLQ_TOPIC" \
    --time -1 2>/dev/null \
    | awk -F: '{sum+=$3} END {print sum+0}' || echo "?")

  echo ""
  echo "======================================================"
  echo "  Demo 8 complete!"
  echo ""
  echo "  Results:"
  echo "    $MSG_COUNT messages produced"
  printf "    %s good rows → Teradata %s.adsb_positions_08\n" "$ACTUAL" "${TD_DATABASE:-demo_db}"
  printf "    %s error records → Kafka %s\n" "$DLQ_COUNT" "$DLQ_TOPIC"
  echo "    Connector status: RUNNING (errors.tolerance=all kept it alive)"
  echo ""
  echo "  Inspect the DLQ topic:"
  echo "    docker compose exec kafka kafka-console-consumer \\"
  echo "      --bootstrap-server localhost:9092 \\"
  echo "      --topic $DLQ_TOPIC \\"
  echo "      --from-beginning --property print.headers=true"
  echo ""
  echo "  Replay from DLQ (after fixing the issue):"
  echo "    # Re-publish fixed records from the DLQ topic to \$TOPIC"
  echo "    # Then let the connector re-process them."
  echo "======================================================"
fi
