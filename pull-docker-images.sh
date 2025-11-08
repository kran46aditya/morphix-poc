#!/bin/bash
# Script to pull Docker images individually to avoid timeout issues
# Includes retry logic for network issues

set -e

MAX_RETRIES=3
RETRY_DELAY=10

retry_pull() {
    local image=$1
    local attempt=1
    
    while [ $attempt -le $MAX_RETRIES ]; do
        echo "Attempt $attempt/$MAX_RETRIES: Pulling $image..."
        if docker pull "$image"; then
            echo "✓ Successfully pulled $image"
            return 0
        else
            if [ $attempt -lt $MAX_RETRIES ]; then
                echo "✗ Failed to pull $image. Waiting ${RETRY_DELAY}s before retry..."
                sleep $RETRY_DELAY
            else
                echo "✗ Failed to pull $image after $MAX_RETRIES attempts"
                echo "  Try manually: docker pull $image"
                echo "  Or check network connection and Docker Hub status"
                return 1
            fi
        fi
        attempt=$((attempt + 1))
    done
}

echo "=========================================="
echo "Pulling Docker images for Morphix ETL Platform"
echo "=========================================="
echo "This may take several minutes depending on your connection speed."
echo "Images will be retried up to $MAX_RETRIES times if pull fails."
echo ""

# Core services
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1/6 Pulling PostgreSQL..."
retry_pull postgres:15-alpine

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2/6 Pulling MinIO..."
retry_pull minio/minio:latest
retry_pull minio/mc:latest

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3/6 Pulling Spark..."
retry_pull apache/spark:latest

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4/6 Pulling Trino (this may take longer)..."
retry_pull trinodb/trino:latest

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5/6 Pulling Hive Metastore..."
retry_pull bde2020/hive-metastore-postgresql:latest

echo ""
echo "=========================================="
echo "✓ All images pulled successfully!"
echo "You can now run: docker-compose up -d"
echo "=========================================="

