# Docker Compose Setup Guide

This guide explains how to run all Morphix components using Docker Compose, except for MongoDB which remains external.

## Services Included

- **PostgreSQL**: Metadata and job storage
- **Spark**: Master and Worker nodes for data processing
- **Trino**: SQL query engine for Hudi tables
- **Hive Metastore**: Required for Trino to query Hudi via Hive connector
- **MinIO**: S3-compatible object storage for Hudi data
- **MongoDB**: External (not in docker-compose) - connect to your existing MongoDB instance

## Prerequisites

- Docker and Docker Compose installed
- At least 8GB RAM available for Docker
- MongoDB instance accessible (external)

## Quick Start

1. **Create environment file** (copy from below or use existing `.env`):
   ```bash
   # See Environment Configuration section below
   ```

2. **Start all services**:
   ```bash
   docker-compose up -d
   ```

3. **Verify services are running**:
   ```bash
   docker-compose ps
   ```

4. **Check service health**:
   ```bash
   # PostgreSQL
   docker-compose exec postgres pg_isready -U morphix_user

   # MinIO Console (access in browser)
   # http://localhost:9001
   # Login: minioadmin / minioadmin123

   # Spark Master UI
   # http://localhost:8080

   # Trino
   curl http://localhost:8083/v1/info
   ```

## Service Ports

| Service | Port | Description |
|---------|------|-------------|
| PostgreSQL | 5432 | Database |
| MinIO API | 9000 | S3 API endpoint |
| MinIO Console | 9001 | Web UI |
| Spark Master UI | 8080 | Spark Web UI |
| Spark Worker UI | 8082 | Worker Web UI |
| Trino | 8083 | Trino coordinator |
| Hive Metastore | 9083 | Metastore service |

## Environment Configuration

Create a `.env` file in the project root with the following variables:

```bash
# Database Configuration - Use service names for Docker networking
DB_HOST=postgres
DB_PORT=5432
DB_USER=morphix_user
DB_PASSWORD=morphix_password
DB_DATABASE=morphix

# MongoDB Configuration (External)
MONGO_DEFAULT_HOST=your-mongodb-host
MONGO_DEFAULT_PORT=27017
MONGO_DEFAULT_DATABASE=testdb

# Spark Configuration
SPARK_MASTER=spark://spark-master:7077
SPARK_APP_NAME=MorphixETL
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g

# Hudi Configuration
# Option 1: Use MinIO (S3-compatible)
HUDI_BASE_PATH=s3://hudi-data/data
# Option 2: Use local path (mapped volume)
# HUDI_BASE_PATH=/data/hudi

# MinIO Configuration
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false
MINIO_REGION=us-east-1

# Trino Configuration
TRINO_HOST=trino
TRINO_PORT=8080
TRINO_USER=admin
TRINO_CATALOG=hive
TRINO_SCHEMA=default
TRINO_HTTP_SCHEME=http
TRINO_REQUEST_TIMEOUT=300

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=false

# Job Scheduler
JOB_SCHEDULER_ENABLED=true
JOB_SCHEDULER_CHECK_INTERVAL=60

# Authentication
AUTH_SECRET_KEY=your-secret-key-change-in-production
AUTH_ALGORITHM=HS256
AUTH_ACCESS_TOKEN_EXPIRE_MINUTES=30

# Environment
ENVIRONMENT=development
DEBUG=false
```

## Service Details

### PostgreSQL
- Database: `morphix`
- User: `morphix_user`
- Password: `morphix_password`
- Also hosts `metastore_db` for Hive Metastore

### MinIO
- Access via S3 API: `http://localhost:9000`
- Console: `http://localhost:9001`
- Default credentials: `minioadmin` / `minioadmin123`
- Initial bucket: `hudi-data` (auto-created)

### Spark
- Master: `spark://spark-master:7077` (internal)
- Master UI: `http://localhost:8080`
- Worker UI: `http://localhost:8082`

### Trino
- Internal: `http://trino:8080`
- External: `http://localhost:8083`
- Catalog: `hive` (configured to use Hive Metastore)
- Schema: `default`

### Hive Metastore
- Connects to PostgreSQL `metastore_db`
- Used by Trino to discover Hudi tables
- Port: `9083` (internal)

## Connecting from APIs

When your APIs run inside Docker or from host, use these connection strings:

### From Host Machine (your Python app)
```python
# PostgreSQL
DB_HOST=localhost  # or postgres if API is also in Docker
DB_PORT=5432

# Trino
TRINO_HOST=localhost
TRINO_PORT=8083  # External port

# Spark
SPARK_MASTER=spark://localhost:7077

# MinIO
MINIO_ENDPOINT=localhost:9000
```

### From Docker Container (if API runs in Docker)
```python
# Use service names directly
DB_HOST=postgres
TRINO_HOST=trino
TRINO_PORT=8080  # Internal port
SPARK_MASTER=spark://spark-master:7077
MINIO_ENDPOINT=minio:9000
```

## Data Persistence

Data is persisted in Docker volumes:
- `postgres-data`: PostgreSQL data
- `minio-data`: MinIO object storage
- `spark-data`: Spark master data
- `spark-worker-data`: Spark worker data
- `hive-metastore-data`: Hive Metastore data
- `./hudi-data`: Local directory for Hudi tables (mapped volume)

To backup data:
```bash
docker-compose exec postgres pg_dump -U morphix_user morphix > backup.sql
```

To reset everything:
```bash
docker-compose down -v  # WARNING: Deletes all volumes
```

## Troubleshooting

### Services not starting
```bash
# Check logs
docker-compose logs -f [service-name]

# Common issues:
# 1. Port conflicts - check if ports are already in use
# 2. Insufficient memory - increase Docker memory limit
# 3. Volume permissions - ensure ./hudi-data is writable
```

### Trino cannot connect to Hive Metastore
```bash
# Check Hive Metastore logs
docker-compose logs hive-metastore

# Ensure metastore_db database exists
docker-compose exec postgres psql -U morphix_user -l | grep metastore_db
```

### Spark cannot connect to MinIO
- Ensure MinIO bucket `hudi-data` exists
- Check MinIO credentials in configuration
- Verify S3 endpoint configuration in Spark

### APIs cannot connect to services
- If running from host: use `localhost` and external ports
- If running in Docker: use service names and internal ports
- Check network: `docker network ls` and `docker network inspect morphix-poc_morphix-network`

## Stopping Services

```bash
# Stop all services
docker-compose stop

# Stop and remove containers (keeps volumes)
docker-compose down

# Stop and remove everything including volumes (WARNING: data loss)
docker-compose down -v
```

## Updating Services

```bash
# Pull latest images
docker-compose pull

# Recreate containers with new images
docker-compose up -d --force-recreate
```

## Monitoring

- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8082

**Note**: Spark uses the official Apache Spark image (`apache/spark:latest`) configured for standalone cluster mode.
- **Trino**: http://localhost:8083/ui (if Trino UI is enabled)
- **MinIO Console**: http://localhost:9001

## Next Steps

1. Verify all services are healthy: `docker-compose ps`
2. Configure your `.env` file with correct values
3. Start the API server: `uvicorn src.api.mongo_api:app --reload`
4. Test connections from API to services

For more details, see:
- [SETUP.md](SETUP.md) - General setup guide
- [API.md](API.md) - API documentation
- [FLOW.md](FLOW.md) - Architecture overview

