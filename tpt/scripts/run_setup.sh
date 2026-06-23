#!/bin/bash
# ============================================================
# One-time demo setup: creates Teradata tables, NOS auth
# objects, and seeds the aircraft registry.
#
# Run inside the TPT container:
#   docker compose exec tpt bash /tpt/scripts/run_setup.sh
#
# Expects these env vars (set in docker-compose.yml from .env):
#   TD_HOST, TD_USER, TD_PASSWORD, TD_DATABASE
#   HOST_IP, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BTEQ_SCRIPT="${SCRIPT_DIR}/infra/setup_demo_tables.bteq"

# Validate required variables
for var in TD_HOST TD_USER TD_PASSWORD TD_DATABASE HOST_IP MINIO_ROOT_USER MINIO_ROOT_PASSWORD; do
  if [ -z "${!var}" ]; then
    echo "ERROR: Environment variable $var is not set."
    exit 1
  fi
done

echo "======================================================="
echo "  Teradata Streaming Demo — Setup"
echo "  Target: ${TD_HOST}  Database: ${TD_DATABASE}"
echo "  HOST_IP (for NOS/OTF): ${HOST_IP}"
echo "======================================================="
echo ""

# Substitute env vars and pipe to BTEQ (perl replaces ${VAR} with env value)
perl -pe 's/\$\{([^}]+)\}/$ENV{$1}/ge' "${BTEQ_SCRIPT}" | bteq

EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  echo ""
  echo "✓ Setup complete."
else
  echo ""
  echo "✗ Setup encountered errors (BTEQ exit code: $EXIT_CODE)."
  echo "  Review output above. Some errors (e.g. table already exists) may be harmless."
fi

exit $EXIT_CODE
