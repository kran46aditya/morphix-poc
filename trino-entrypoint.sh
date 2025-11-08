#!/bin/bash
set -e

# Create data directory with proper permissions for trino user (uid 1000)
mkdir -p /var/lib/trino/data
chown -R trino:trino /var/lib/trino

# Execute the original Trino entrypoint
exec /usr/lib/trino/bin/run-trino "$@"

