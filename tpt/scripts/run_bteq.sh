#!/bin/bash
# Run a BTEQ script with ${VAR} substitution from the container environment.
# Usage (inside tpt container): bash run_bteq.sh /tpt/scripts/some_script.bteq
set -e
perl -pe 's/\$\{([^}]+)\}/$ENV{$1}/ge' "$1" | bteq
exit "${PIPESTATUS[1]}"
