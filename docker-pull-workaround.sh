#!/bin/bash
# Workaround script for persistent TLS handshake timeout issues
# This script pulls images one at a time with extended timeouts

set -e

echo "=========================================="
echo "Docker Image Pull Workaround"
echo "For persistent TLS handshake timeout issues"
echo "=========================================="
echo ""

# Function to pull with extended timeout
pull_with_timeout() {
    local image=$1
    local timeout=${2:-300}  # Default 5 minutes
    
    echo "Pulling $image (timeout: ${timeout}s)..."
    
    # Try with timeout command if available
    if command -v timeout &> /dev/null; then
        timeout $timeout docker pull "$image" || {
            echo "⚠ Timeout or error pulling $image"
            echo "  Trying again without timeout..."
            docker pull "$image" || {
                echo "✗ Failed to pull $image"
                echo "  Manual command: docker pull $image"
                return 1
            }
        }
    else
        # macOS doesn't have timeout by default
        docker pull "$image" || {
            echo "✗ Failed to pull $image"
            echo "  Manual command: docker pull $image"
            return 1
        }
    fi
    
    echo "✓ Successfully pulled $image"
    echo ""
}

# Wait between pulls to avoid rate limiting
wait_between_pulls() {
    echo "Waiting 5 seconds before next pull..."
    sleep 5
}

echo "Starting image pulls..."
echo "Large images (Spark, Trino) may take several minutes."
echo ""

pull_with_timeout "postgres:15-alpine" 120
wait_between_pulls

pull_with_timeout "minio/minio:latest" 180
wait_between_pulls

pull_with_timeout "minio/mc:latest" 120
wait_between_pulls

pull_with_timeout "apache/spark:latest" 600
wait_between_pulls

echo "Pulling Trino (this is a large image, may take 10+ minutes)..."
pull_with_timeout "trinodb/trino:latest" 900
wait_between_pulls

pull_with_timeout "bde2020/hive-metastore-postgresql:latest" 300

echo ""
echo "=========================================="
echo "✓ All images pulled!"
echo "Run: docker-compose up -d"
echo "=========================================="

