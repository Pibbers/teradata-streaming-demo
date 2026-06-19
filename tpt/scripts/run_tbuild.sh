#!/bin/bash
# Preprocess a tbuild script (substituting $(VAR) from the container environment),
# write to a temp file, and execute with tbuild.
# Usage (inside tpt container): bash run_tbuild.sh /tpt/tbuild/job.tbuild
set -e

TBUILD_FILE="$1"; shift

TMPSCRIPT=$(mktemp /tmp/tbuild_XXXXXX.tbuild)
trap 'rm -f "$TMPSCRIPT"' EXIT

# Substitute $(VAR) patterns from environment — mirrors the ${VAR} technique in run_bteq.sh
# Recognised vars: TD_HOST TD_USER TD_PASSWORD TD_DATABASE INDEX_FILE KAFKA_BOOTSTRAP KAFKA_TOPIC
perl -pe 's/\$\(([^)]+)\)/defined $ENV{$1} ? $ENV{$1} : "(UNDEF:$1)"/ge' \
  "$TBUILD_FILE" > "$TMPSCRIPT"

exec tbuild -f "$TMPSCRIPT" "$@"
