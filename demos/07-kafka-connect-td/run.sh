#!/bin/bash
# ============================================================
# Demo 7: Kafka → Kafka Connect JDBC Sink → Teradata
# ============================================================
# Demonstrates:
#   • adsb_producer.py publishing ADS-B position fixes as JSON
#     to the adsb-positions-json Kafka topic
#   • Kafka Connect JDBC Sink connector consuming that topic
#     and inserting rows directly into Teradata — no TPT involved
#   • Rows visible in Teradata within seconds of each produce()
#   • Confluent JDBC Sink + terajdbc from Maven Central (no TPT,
#     no Teradata Connect plugin licence required)
#
# Architecture:
#   adsb_producer.py (--format json)
#       → Kafka (adsb-positions-json)
#       → Kafka Connect JDBC Sink (demo07-td-jdbc-sink)
#       → Teradata adsb_positions_07
#
# Credential handling:
#   run.sh writes kafka/connect/td-credentials.properties from .env.
#   That file is bind-mounted at /etc/kafka/connect-configs/ inside
#   the kafka-connect container.  The connector config references it
#   via Kafka Connect's FileConfigProvider: ${file:/etc/kafka/...}.
#
# Modes:
#   bash demos/07-kafka-connect-td/run.sh            # bounded (200 msgs)
#   bash demos/07-kafka-connect-td/run.sh --continuous  # until Ctrl+C
#
# Prerequisites:
#   docker compose up -d   (kafka, kafka-connect, schema-registry, tpt)
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
CONNECTOR_NAME="demo07-td-jdbc-sink"
TOPIC="adsb-positions-json"
MSG_COUNT="${ADSB_COUNT:-200}"     # bounded: messages to produce
MSG_INTERVAL="${ADSB_INTERVAL:-1}" # seconds between cycles (10 aircraft per cycle)

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
echo "  Demo 7: Kafka → Kafka Connect JDBC Sink → Teradata"
echo "  Broker (host):     $KAFKA_EXTERNAL"
echo "  Kafka Connect:     $CONNECT_URL"
echo "  Topic:             $TOPIC"
echo "  Connector:         $CONNECTOR_NAME"
echo "  Mode:              $([ "$CONTINUOUS" = "1" ] && echo "CONTINUOUS (Ctrl+C to stop)" || echo "BOUNDED ($MSG_COUNT messages)")"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/4  Writing Teradata credentials for FileConfigProvider"

cat > kafka/connect/td-credentials.properties <<EOF
td_host=${TD_HOST}
td_user=${TD_USER}
td_password=${TD_PASSWORD}
td_database=${TD_DATABASE:-demo_db}
EOF
echo "      Written: kafka/connect/td-credentials.properties"
echo "      (mounted at /etc/kafka/connect-configs/ inside kafka-connect container)"

# ── Step 2 ─────────────────────────────────────────────────
step "2/4  Resetting connector, topic, and target table"

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$CONNECTOR_NAME")
if [ "$HTTP_STATUS" = "200" ]; then
  echo "      Deleting existing connector: $CONNECTOR_NAME"
  curl -s -X DELETE "$CONNECT_URL/connectors/$CONNECTOR_NAME" > /dev/null
  sleep 2
fi

echo "      Recreating Kafka topic: $TOPIC"
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --delete --topic "$TOPIC" 2>/dev/null || true
sleep 2
docker compose exec -T kafka kafka-topics \
  --bootstrap-server localhost:9092 --create --topic "$TOPIC" \
  --partitions 1 --replication-factor 1

echo "      Clearing adsb_positions_07..."
bteq_run /tpt/scripts/demo07/prepare.bteq

# ── Step 3 ─────────────────────────────────────────────────
step "3/4  Registering JDBC Sink connector"
echo "      Config: kafka/connect/td-jdbc-sink.json"
echo "      Credentials: FileConfigProvider → /etc/kafka/connect-configs/td-credentials.properties"
echo "      JDBC URL: jdbc:teradata://${TD_HOST}/DATABASE=${TD_DATABASE:-demo_db},TMODE=ANSI"

curl -s -X POST "$CONNECT_URL/connectors" \
  -H "Content-Type: application/json" \
  -d @kafka/connect/td-jdbc-sink.json > /dev/null

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
    echo "        • 'No suitable driver' → terajdbc JAR classloader issue:"
    echo "          Check kafka-connect/Dockerfile RUN curl path matches the JAR location"
    echo "        • 'Authentication failed' → credentials in td-credentials.properties"
    echo "        • 'Table not found'       → run: bash tpt/scripts/run_setup.sh"
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
step "4/4  Publishing ADS-B positions as JSON"

PRODUCER_PID=""

cleanup() {
  echo ""
  echo "── Stopping producer..."
  [ -n "$PRODUCER_PID" ] && kill "$PRODUCER_PID" 2>/dev/null || true
  wait "$PRODUCER_PID" 2>/dev/null || true
  echo ""
  echo "── Final row count"
  bteq_run /tpt/scripts/demo07/verify.bteq
  echo ""
  echo "======================================================"
  echo "  Demo 7 complete!"
  echo "======================================================"
  exit 0
}
trap cleanup INT TERM

if [ "$CONTINUOUS" = "1" ]; then
  echo "      Publishing continuously (1 s interval, 10 aircraft, JSON format)..."
  echo "      Rows land in adsb_positions within seconds of each cycle."
  echo "      Press Ctrl+C to stop."
  echo ""

  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$TOPIC" \
    --interval  "$MSG_INTERVAL" \
    --continuous \
    --format    connect-json &
  PRODUCER_PID=$!

  while kill -0 "$PRODUCER_PID" 2>/dev/null; do
    sleep 10
    echo -n "  [$(date -u +'%H:%M:%S UTC')]  "
    bteq_run /tpt/scripts/demo07/status.bteq 2>/dev/null \
      | grep "^STATUS" \
      || echo "?"
  done

  cleanup

else
  echo "      Publishing $MSG_COUNT messages (10 aircraft × $((MSG_COUNT / 10)) cycles, ${MSG_INTERVAL}s interval)..."
  python3 kafka/producers/adsb_producer.py \
    --bootstrap "$KAFKA_EXTERNAL" \
    --topic     "$TOPIC" \
    --count     "$MSG_COUNT" \
    --interval  "$MSG_INTERVAL" \
    --format    connect-json

  echo ""
  echo "      Producer done. Waiting for JDBC Sink to flush to Teradata..."
  EXPECTED="$MSG_COUNT"
  ACTUAL=0
  WAIT_MAX=60
  WAITED=0
  while [ "$ACTUAL" -lt "$EXPECTED" ] && [ "$WAITED" -lt "$WAIT_MAX" ]; do
    sleep 3
    WAITED=$((WAITED + 3))
    ACTUAL=$(bteq_run /tpt/scripts/demo07/status.bteq 2>/dev/null \
      | grep "^STATUS" \
      | sed "s/.*rows=\([0-9]*\).*/\1/" || echo "0")
    echo "      [${WAITED}s] Rows in adsb_positions_07: $ACTUAL / $EXPECTED"
  done

  echo ""
  bteq_run /tpt/scripts/demo07/verify.bteq

  echo ""
  echo "======================================================"
  echo "  Demo 7 complete!"
  echo ""
  echo "  Pipeline summary:"
  echo "    $MSG_COUNT JSON messages"
  echo "      → Kafka ($TOPIC)"
  echo "      → Kafka Connect JDBC Sink ($CONNECTOR_NAME)"
  echo "      → Teradata ${TD_DATABASE:-demo_db}.adsb_positions_07"
  echo ""
  echo "  No TPT, no NOS, no Flink — just Kafka Connect + JDBC."
  echo ""
  echo "  Try in Teradata Studio:"
  echo "    SELECT icao24, callsign, COUNT(*) AS positions"
  echo "    FROM ${TD_DATABASE:-demo_db}.adsb_positions_07"
  echo "    GROUP BY icao24, callsign ORDER BY positions DESC;"
  echo "======================================================"
fi
