#!/bin/bash
set -euo pipefail

if [ -f /opt/hive/conf/hive-site.xml.template ]; then
  envsubst < /opt/hive/conf/hive-site.xml.template > /opt/hive/conf/hive-site.xml
fi

exec /entrypoint.sh "$@"
