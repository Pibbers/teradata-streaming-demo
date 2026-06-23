#!/bin/bash
# ============================================================
# Demo 1: Avro BLOB Loading into Teradata DATASET column
# ============================================================
# Demonstrates:
#   • Loading Avro container files as BLOBs via BTEQ DEFERRED BY NAME
#   • Using AvroContainerSplit to expand each BLOB into DATASET AVRO rows
#   • Schema evolution: v1 (5 fields) and v2 (8 fields) coexist;
#     v1 rows return NULL for the three v2-only fields
#
# Prerequisites:
#   docker compose up -d        (kafka, minio, tpt at minimum)
#   bash tpt/scripts/run_setup.sh  (one-time Teradata table setup)
#   pip install fastavro        (for generate_product_avro.py)
#
# Run from project root:
#   bash demos/01-avro-blob/run.sh
# ============================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

# Helper: run a BTEQ script inside the tpt container
run_bteq() {
  docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh "$1"
}

# Helper: run a tbuild job inside the tpt container with optional -e KEY=VAL overrides
run_tbuild() {
  local tbuild_file="$1"; shift
  local extra_env=()
  for kv in "$@"; do extra_env+=(-e "$kv"); done
  docker compose exec -T "${extra_env[@]}" tpt bash /tpt/scripts/run_tbuild.sh "$tbuild_file"
}

echo "======================================================"
echo "  Demo 1: Avro BLOB → Teradata DATASET column"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/4  Generating Avro container files"
python3 kafka/producers/generate_product_avro.py

# ── Step 2 ─────────────────────────────────────────────────
step "2/4  Clearing staging tables (TPT LOAD requires empty target)"
run_bteq /tpt/scripts/demo01/prepare.bteq

# ── Step 3 ─────────────────────────────────────────────────
step "3/4  Loading Avro files into Teradata (BTEQ DEFERRED BY NAME)"
echo "      → product_v1.avro (schema v1: 5 fields)"
run_bteq /tpt/scripts/demo01/load_v1.bteq

echo "      → product_v2.avro (schema v2: 8 fields)"
run_bteq /tpt/scripts/demo01/load_v2.bteq

# ── Step 4 ─────────────────────────────────────────────────
step "4/4  Decoding BLOBs into DATASET column and verifying"
run_bteq /tpt/scripts/demo01/decode.bteq

echo ""
echo "======================================================"
echo "  Demo 1 complete!"
echo ""
echo "  Next — query individual records via BTEQ or SQL:"
echo ""
echo "    SELECT *"
echo "    FROM TABLE (UNNEST_DATASET_V2("
echo "      ON (SELECT avro FROM ${TD_DATABASE:-demo_db}.avro_product)"
echo "      USING OUTPUT COLUMN (avro)"
echo "    )) AS t;"
echo "======================================================"
