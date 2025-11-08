#!/bin/bash
# Alternative script to pull images using Docker Hub registry mirror
# Configure your Docker daemon first: see DOCKER_TROUBLESHOOTING.md

set -e

echo "=========================================="
echo "Pulling Docker images (using registry mirror if configured)"
echo "=========================================="
echo ""
echo "Note: Ensure Docker daemon is configured with a registry mirror"
echo "See DOCKER_TROUBLESHOOTING.md for configuration"
echo ""

images=(
    "postgres:15-alpine"
    "minio/minio:latest"
    "minio/mc:latest"
    "apache/spark:latest"
    "trinodb/trino:latest"
    "bde2020/hive-metastore-postgresql:latest"
)

for image in "${images[@]}"; do
    echo "Pulling $image..."
    docker pull "$image" || {
        echo "⚠ Failed to pull $image"
        echo "  This might be due to:"
        echo "  - Network connectivity issues"
        echo "  - Docker Hub rate limiting"
        echo "  - Registry mirror misconfiguration"
        echo "  Try: docker pull $image"
    }
    echo ""
done

echo "Done!"

