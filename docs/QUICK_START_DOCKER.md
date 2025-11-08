# Quick Start with Docker Compose

This is a quick reference for getting started with the Docker Compose setup.

## 1. Pull Docker Images

**If you encounter timeout errors**, use one of these methods:

```bash
# Option A: Use retry script (recommended - auto-retries on failure)
./pull-docker-images.sh

# Option B: Use workaround script (for persistent TLS timeout issues)
./docker-pull-workaround.sh

# Option C: Manual pull (if scripts don't work)
docker pull postgres:15-alpine
docker pull minio/minio:latest
docker pull minio/mc:latest
docker pull apache/spark:latest
docker pull trinodb/trino:latest
docker pull bde2020/hive-metastore-postgresql:latest
```

**Troubleshooting TLS Timeouts**: See [DOCKER_TROUBLESHOOTING.md](DOCKER_TROUBLESHOOTING.md) for solutions including:
- Configuring Docker registry mirrors
- Increasing timeout settings
- Pulling during off-peak hours

## 2. Start All Services

```bash
docker-compose up -d
```

## 3. Verify Services

```bash
docker-compose ps
```

All services should show as "Up" and healthy.

**Troubleshooting**: If services fail to start, see [DOCKER_TROUBLESHOOTING.md](DOCKER_TROUBLESHOOTING.md)

## 4. Configure Environment

Create a `.env` file with these key settings:

```bash
# Database (use service name when APIs run in Docker, localhost when running from host)
DB_HOST=postgres  # or localhost
DB_PORT=5432
DB_USER=morphix_user
DB_PASSWORD=morphix_password
DB_DATABASE=morphix

# Spark (use service name when APIs run in Docker)
SPARK_MASTER=spark://spark-master:7077  # or spark://localhost:7077

# Trino (use service name when APIs run in Docker)
TRINO_HOST=trino  # or localhost
TRINO_PORT=8080   # or 8083 when connecting from host

# MinIO (use service name when APIs run in Docker)
MINIO_ENDPOINT=minio:9000  # or localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false

# MongoDB (external - not in docker-compose)
MONGO_DEFAULT_HOST=your-mongodb-host
MONGO_DEFAULT_PORT=27017
```

## 5. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master UI | http://localhost:8080 | - |
| Trino | http://localhost:8083 | - |
| PostgreSQL | localhost:5432 | morphix_user / morphix_password |

## 6. Start Your API

```bash
# Make sure .env is configured correctly
uvicorn src.api.mongo_api:app --reload --port 8000
```

## 7. Test Connections

```bash
# Test Trino
curl http://localhost:8083/v1/info

# Test PostgreSQL
docker-compose exec postgres psql -U morphix_user -d morphix -c "SELECT 1;"

# Test MinIO
curl http://localhost:9000/minio/health/live
```

## Important Notes

1. **When APIs run from host machine**: Use `localhost` and external ports (5432, 8083, 9000, 7077)
2. **When APIs run in Docker**: Use service names (`postgres`, `trino`, `minio`, `spark-master`) and internal ports
3. **MongoDB**: Must be accessible from wherever your API runs (host or container)

## Troubleshooting

```bash
# View logs
docker-compose logs -f [service-name]

# Restart a service
docker-compose restart [service-name]

# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: data loss)
docker-compose down -v
```

See [DOCKER_SETUP.md](DOCKER_SETUP.md) for detailed documentation.

