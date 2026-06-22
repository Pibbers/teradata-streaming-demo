#!/usr/bin/env bash
# Docker Compose forms container FQDNs as {name}.{project}_{network} — the
# underscore separator is illegal in URIs (RFC 3986). When the Hive Metastore
# client in Flink does a reverse DNS lookup of the HMS IP it receives back
# from the server, Docker DNS returns the full FQDN with underscore, which
# then fails URI parsing in the Thrift client.
#
# Fix: add {ip} hive-metastore to /etc/hosts so the reverse lookup from
# /etc/hosts (files) wins over Docker DNS, returning the short name.

set -e

# Fix HMS reverse-DNS: Docker network FQDN has underscore (illegal in URIs)
HMS_IP=$(getent hosts hive-metastore 2>/dev/null | awk '{print $1}' | head -1)
if [ -n "$HMS_IP" ]; then
    echo "$HMS_IP hive-metastore" >> /etc/hosts
fi

# Tell Hadoop where to find core-site.xml (for S3AFileSystem credentials)
export HADOOP_CONF_DIR=/opt/flink/conf

exec /docker-entrypoint.sh "$@"
