# Docker Troubleshooting Guide

## Network Timeout Issues

If you encounter TLS handshake timeout or context canceled errors when pulling Docker images, try these solutions **in order**:

### Quick Fixes (Try These First)

1. **Use the retry script**:
   ```bash
   ./pull-docker-images.sh
   ```
   This script automatically retries failed pulls.

2. **Use the workaround script** (for persistent timeouts):
   ```bash
   ./docker-pull-workaround.sh
   ```
   This pulls images one at a time with extended timeouts.

3. **Pull during off-peak hours** - Docker Hub may be slow during peak times.

### 1. Increase Docker Pull Timeout and Configure Retries

**For Linux (`/etc/docker/daemon.json`):**
```json
{
  "max-concurrent-downloads": 1,
  "max-concurrent-uploads": 1,
  "registry-mirrors": ["https://mirror.gcr.io"],
  "experimental": false
}
```

**For macOS/Windows (Docker Desktop):**
1. Open Docker Desktop
2. Go to Settings → Docker Engine
3. Add configuration:
```json
{
  "max-concurrent-downloads": 1,
  "max-concurrent-uploads": 1,
  "registry-mirrors": ["https://mirror.gcr.io"]
}
```

Then restart Docker:
```bash
# Linux
sudo systemctl restart docker

# Mac/Windows - Restart Docker Desktop from UI
```

**Alternative Registry Mirrors** (if default doesn't work):
- `https://dockerhub.azk8s.cn` (Azure China)
- `https://reg-mirror.qiniu.com` (Qiniu)
- `https://docker.mirrors.ustc.edu.cn` (USTC)

### 2. Pull Images Individually with Retry Logic

**Option A: Use the retry script**
```bash
./pull-docker-images.sh
```

This script will automatically retry failed pulls up to 3 times.

**Option B: Manual pull with retry**
```bash
# Pull with automatic retry
for i in {1..3}; do
  docker pull trinodb/trino:latest && break || sleep 10
done

# Or pull all images manually
docker pull postgres:15-alpine
docker pull minio/minio:latest
docker pull minio/mc:latest
docker pull apache/spark:latest
docker pull trinodb/trino:latest
docker pull bde2020/hive-metastore-postgresql:latest
```

**Option C: Pull during off-peak hours**
Docker Hub has rate limits and may be slower during peak times. Try pulling images:
- Early morning (local time)
- Late evening
- Weekends

### 3. Use Docker Registry Mirror

If you're in a region with slow Docker Hub access, configure a mirror:

Edit `/etc/docker/daemon.json`:
```json
{
  "registry-mirrors": ["https://mirror.gcr.io"]
}
```

### 4. Check Network Connectivity

```bash
# Test Docker Hub connectivity
curl -I https://registry-1.docker.io/v2/

# Test specific image manifest
curl -I https://registry-1.docker.io/v2/apache/hive/manifests/3.1.3
```

### 5. Alternative: Start Services One at a Time

If all services fail to start together:

```bash
# Start core services first
docker-compose up -d postgres minio

# Wait for them to be healthy, then start others
docker-compose up -d spark-master spark-worker
docker-compose up -d hive-metastore
docker-compose up -d trino
```

### 6. Alternative Hive Metastore Image

If `bde2020/hive-metastore-postgresql` fails, try:

Option A: Use a different tag
```yaml
image: bde2020/hive-metastore-postgresql:2.3.0
```

Option B: Build custom metastore (see below)

### 7. Build Custom Hive Metastore (Advanced)

If images keep failing, you can build a custom metastore:

Create `Dockerfile.hive-metastore`:
```dockerfile
FROM openjdk:8-jre-slim

RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

ENV HIVE_VERSION=3.1.3
ENV METASTORE_VERSION=3.1.3

WORKDIR /opt/hive-metastore

# Download and extract Hive Metastore
RUN wget https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz --strip-components=1 && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

# Copy metastore configuration
COPY hive-site.xml /opt/hive-metastore/conf/

EXPOSE 9083

CMD ["/opt/hive-metastore/bin/hive", "--service", "metastore"]
```

Then in docker-compose.yml:
```yaml
hive-metastore:
  build:
    context: .
    dockerfile: Dockerfile.hive-metastore
  ...
```

## Service-Specific Issues

### MinIO Init Fails

```bash
# Check MinIO logs
docker-compose logs minio

# Manually create bucket
docker-compose exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin123
docker-compose exec minio mc mb myminio/hudi-data
```

### Hive Metastore Connection Issues

```bash
# Check if metastore_db exists
docker-compose exec postgres psql -U morphix_user -l | grep metastore_db

# Create manually if needed
docker-compose exec postgres psql -U morphix_user -d postgres -c "CREATE DATABASE metastore_db;"
```

### Trino Cannot Connect to Hive

1. Check Hive Metastore is running: `docker-compose ps hive-metastore`
2. Test connection: `docker-compose exec trino curl http://hive-metastore:9083`
3. Check Trino logs: `docker-compose logs trino`

## Resource Issues

If services fail due to insufficient resources:

1. **Reduce Memory Usage**:
   - Edit docker-compose.yml to reduce Spark worker memory
   - Reduce PostgreSQL shared_buffers if needed

2. **Start Fewer Services**:
   - Remove spark-worker if only testing
   - Use local Spark instead of cluster mode

## Complete Reset

If everything is broken and you want to start fresh:

```bash
# Stop and remove everything
docker-compose down -v

# Remove all images (optional)
docker image prune -a

# Restart Docker (if issues persist)
# Mac: Restart Docker Desktop
# Linux: sudo systemctl restart docker

# Start fresh
docker-compose pull
docker-compose up -d
```

## Common Error Messages

### "context canceled"
- Usually means the pull/download was interrupted
- Solution: Try again or pull images individually

### "TLS handshake timeout"
- Network connectivity issue with Docker Hub
- Solution: Use registry mirror or increase timeout

### "service unhealthy"
- Service failed health check
- Solution: Check logs with `docker-compose logs [service]`

### "port already in use"
- Another process is using the port
- Solution: Change port in docker-compose.yml or stop conflicting service

### "connection refused"
- Service not ready yet
- Solution: Wait for dependencies or check startup order

