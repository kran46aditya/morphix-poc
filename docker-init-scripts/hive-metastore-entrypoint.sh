#!/bin/bash
# Custom entrypoint for Hive Metastore that skips schema init if already done
set -e

# Check schema version - if it exists and is correct, skip init
echo "Checking Hive Metastore schema version..."
SCHEMA_VERSION=$(/opt/hive/bin/schematool -dbType postgres -info 2>&1 | grep -o "version [0-9.]*" | head -1 || echo "")

if [ -z "$SCHEMA_VERSION" ] || ! echo "$SCHEMA_VERSION" | grep -q "3.1.0"; then
    echo "Schema not initialized or wrong version, initializing..."
    /opt/hive/bin/schematool -dbType postgres -initSchema -verbose || {
        echo "Schema initialization failed, but continuing (may already exist)..."
    }
else
    echo "Hive Metastore schema already initialized at version $SCHEMA_VERSION"
fi

# Start the metastore service
echo "Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore

