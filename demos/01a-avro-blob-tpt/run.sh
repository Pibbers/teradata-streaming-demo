#!/bin/bash
# ============================================================
# Demo 1A: Avro BLOB Loading into Teradata DATASET column (TPT variant)
# ============================================================
# Demonstrates the same Avro container file workflow as Demo 1 but
# replaces the BTEQ DEFERRED BY NAME staging step with a TPT job:
#
#   • TPT DataConnector PRODUCER reads a pipe-delimited manifest
#   • BLOB BY NAME column: DataConnector reads each file path and
#     streams the raw binary bytes to the STREAM operator
#   • STREAM inserts both container files in a single TPT job
#   • STREAM is the only TPT operator that supports BLOB/CLOB columns;
#     LOAD, MLoad, and Update operators all reject LOB columns
#
# Contrast with Demo 1:
#   Demo 1:  BTEQ .IMPORT VARTEXT ... AS DEFERRED BY NAME — one file per session
#   Demo 1A: tbuild DataConnector + STREAM            — N files, one job, restartable
#
# Prerequisites:
#   docker compose up -d        (kafka, minio, tpt at minimum)
#   bash tpt/scripts/run_setup.sh  (one-time Teradata table setup)
#   pip install fastavro        (for generate_product_avro.py)
#
# Run from project root:
#   bash demos/01a-avro-blob-tpt/run.sh
# ============================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

[ -f .env ] && set -a && source .env && set +a

step() { echo ""; echo "── $* ─────────────────────────────────────"; }

run_bteq() {
  docker compose exec -T tpt bash /tpt/scripts/run_bteq.sh "$1"
}

run_tbuild() {
  local tbuild_file="$1"; shift
  local extra_env=()
  for kv in "$@"; do extra_env+=(-e "$kv"); done
  docker compose exec -T "${extra_env[@]}" tpt bash /tpt/scripts/run_tbuild.sh "$tbuild_file"
}

echo "======================================================"
echo "  Demo 1A: Avro BLOB → Teradata DATASET column (TPT)"
echo "======================================================"

# ── Step 1 ─────────────────────────────────────────────────
step "1/4  Generating Avro container files + TPT manifest"
python3 kafka/producers/generate_product_avro.py
# Writes:
#   data/sample/product_v1.avro
#   data/sample/product_v2.avro
#   data/sample/avro_manifest.txt  ← used by tbuild (1A only)
#   data/sample/load_v[12]_index.txt ← used by Demo 1 BTEQ only

# ── Step 2 ─────────────────────────────────────────────────
step "2/4  Clearing staging tables and TPT work tables"
run_bteq /tpt/scripts/demo01a/prepare.bteq

# ── Step 3 ─────────────────────────────────────────────────
step "3/4  Loading Avro files into Teradata (TPT STREAM, BLOB AS DEFERRED BY NAME)"
echo "      manifest: /tpt/data/sample/avro_manifest.txt (2 container files)"
# Clear any checkpoint from a previous run so tbuild starts fresh (not a restart job).
docker compose exec -T tpt twbrmcp ttuuser 2>/dev/null || true
run_tbuild /tpt/tbuild/avro_blob_load.tbuild \
  "MANIFEST_FILE=/tpt/data/sample/avro_manifest.txt"
# Both container files (v1 and v2) loaded in a single TPT job.
# stage_id 1 = product_v1.avro (schema v1: 5 fields)
# stage_id 2 = product_v2.avro (schema v2: 8 fields)

# ── Step 4 ─────────────────────────────────────────────────
step "4/4  Decoding BLOBs into DATASET column and verifying"
run_bteq /tpt/scripts/demo01/decode.bteq

echo ""
echo "======================================================"
echo "  Demo 1A complete!"
echo ""
echo "  TPT loaded both Avro container files in one job."
echo "  AvroContainerSplit expanded them into 60 DATASET rows."
echo ""
echo "  Compare with Demo 1 (BTEQ) — same result, different loader:"
echo "    bash demos/01-avro-blob/run.sh"
echo "======================================================"
