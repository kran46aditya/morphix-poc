#!/bin/bash

# Quick setup script for Trino server

set -e

echo "Setting up Trino server..."
echo ""

if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed. Please install Docker first."
    echo "Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "WARNING: Port 8080 is already in use."
    read -p "Stop the existing service? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker stop trino-server 2>/dev/null || true
        docker rm trino-server 2>/dev/null || true
    else
        echo "Please stop the service on port 8080 first."
        exit 1
    fi
fi

echo "Pulling Trino Docker image..."
docker pull trinodb/trino:latest

echo "Starting Trino server..."
docker run -d --name trino-server -p 8080:8080 trinodb/trino:latest

echo "Waiting for Trino to start (30-60 seconds)..."
sleep 15

MAX_RETRIES=20
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s http://localhost:8080/v1/info > /dev/null 2>&1; then
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "Waiting... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 3
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "ERROR: Trino server did not start in time."
    echo "Check logs: docker logs trino-server"
    exit 1
fi

echo ""
echo "Trino server is running!"
echo ""
echo "Connection details:"
echo "  Host: localhost"
echo "  Port: 8080"
echo "  Web UI: http://localhost:8080"
echo ""
echo "Next steps:"
echo "  1. Add to your .env file:"
echo "     TRINO_HOST=localhost"
echo "     TRINO_PORT=8080"
echo "     TRINO_USER=admin"
echo "     TRINO_CATALOG=system"
echo "     TRINO_SCHEMA=default"
echo ""
echo "  2. Test: curl http://localhost:8080/v1/info"
echo "  3. Use API: GET /trino/health (with authentication)"
echo ""
echo "Commands:"
echo "  Stop:   docker stop trino-server"
echo "  Start:  docker start trino-server"
echo "  Logs:   docker logs -f trino-server"
echo "  Remove: docker rm -f trino-server"
echo ""

