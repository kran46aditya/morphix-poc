# Next Steps - After Docker Compose Setup

Congratulations! Your Docker Compose infrastructure is now running. Here's what to do next:

## 1. Verify All Services

Check that all services are running and healthy:

```bash
# Check all services
docker compose ps

# Check individual service logs
docker compose logs -f postgres
docker compose logs -f minio
docker compose logs -f spark-master
docker compose logs -f spark-worker
docker compose logs -f hive-metastore
docker compose logs -f trino
```

## 2. Verify Service Endpoints

Test that all services are accessible:

```bash
# PostgreSQL
docker compose exec postgres psql -U morphix_user -d morphix -c "SELECT version();"

# MinIO Console (open in browser)
# http://localhost:9001
# Login: minioadmin / minioadmin123

# Spark Master UI (open in browser)
# http://localhost:8080

# Spark Worker UI (open in browser)
# http://localhost:8082

# Trino
curl http://localhost:8083/v1/info
```

## 3. Configure Your Environment

Create or update your `.env` file with the correct connection strings:

```bash
# Database Configuration
DB_HOST=localhost  # or 'postgres' if API runs in Docker
DB_PORT=5432
DB_USER=morphix_user
DB_PASSWORD=morphix_password
DB_DATABASE=morphix

# MongoDB Configuration (External - your existing MongoDB)
MONGO_DEFAULT_HOST=your-mongodb-host
MONGO_DEFAULT_PORT=27017
MONGO_DEFAULT_DATABASE=testdb

# Spark Configuration
SPARK_MASTER=spark://localhost:7077  # or spark://spark-master:7077 if API in Docker
SPARK_APP_NAME=MorphixETL
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g

# Hudi Configuration
# Option 1: Use local filesystem
HUDI_BASE_PATH=/tmp/hudi
# Option 2: Use MinIO (S3) - requires additional Spark config
# HUDI_BASE_PATH=s3a://hudi-data/data

# Trino Configuration
TRINO_HOST=localhost  # or 'trino' if API runs in Docker
TRINO_PORT=8083  # External port
TRINO_USER=admin
TRINO_CATALOG=hive
TRINO_SCHEMA=default
TRINO_HTTP_SCHEME=http

# MinIO Configuration
MINIO_ENDPOINT=localhost:9000  # or 'minio:9000' if API in Docker
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Authentication
AUTH_SECRET_KEY=your-secret-key-change-in-production
```

## 4. Test Trino Connection

Verify Trino can connect to Hive Metastore:

```bash
# Connect to Trino CLI (if available) or use curl
curl http://localhost:8083/v1/info

# List catalogs
curl -X GET "http://localhost:8083/v1/catalog" -H "X-Trino-User: admin"

# Test query (once you have tables)
curl -X POST "http://localhost:8083/v1/statement" \
  -H "X-Trino-User: admin" \
  -H "X-Trino-Catalog: hive" \
  -H "X-Trino-Schema: default" \
  -d "SELECT 1"
```

## 5. Test Spark Connection

Verify Spark cluster is accessible:

```bash
# Check Spark Master UI
curl http://localhost:8080

# Submit a test Spark job (if needed)
# docker compose exec spark-master /opt/spark/bin/spark-submit --version
```

## 6. Start Your API Server

Now start your Morphix API:

```bash
# Make sure you're in the project directory
cd /Users/mondee/workspace/morphix-poc

# Activate virtual environment (if using one)
source venv/bin/activate

# Start the API server
uvicorn src.api.mongo_api:app --reload --host 0.0.0.0 --port 8000
```

## 7. Test API Endpoints

Once the API is running:

```bash
# Health check
curl http://localhost:8000/health

# View API documentation
# Open in browser: http://localhost:8000/docs

# Test authentication (register a user)
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "email": "test@example.com",
    "password": "TestPass123!",
    "full_name": "Test User"
  }'
```

## 8. End-to-End Workflow Test

### Step 1: Store MongoDB Credentials

```bash
# Login first (get token from registration)
TOKEN="your-jwt-token-here"

curl -X POST "http://localhost:8000/mongo/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "my_source",
    "mongo_uri": "mongodb://your-mongodb-host:27017/testdb",
    "database": "testdb",
    "collection": "test_collection"
  }'
```

### Step 2: Generate Schema

```bash
curl -X POST "http://localhost:8000/schema/generate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "my_source",
    "sample_size": 1000
  }'
```

### Step 3: Transform and Load Data to Hudi

```bash
curl -X POST "http://localhost:8000/mongo/transform" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "my_source",
    "flatten_data": true,
    "apply_schema": true,
    "use_spark": true
  }'
```

### Step 4: Query via Trino

```bash
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM hive.default.your_table_name LIMIT 10",
    "catalog": "hive",
    "schema": "default"
  }'
```

## 9. Monitoring and Debugging

### Check Service Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f trino
docker compose logs -f spark-master
docker compose logs -f hive-metastore
```

### Check Service Health

```bash
# Docker health status
docker compose ps

# Individual health checks
docker inspect morphix-trino | grep Health -A 10
```

## 10. Common Next Steps

### Configure Hudi to Use MinIO (Optional)

If you want Hudi tables stored in MinIO (S3-compatible):

1. Update Spark configuration to include S3 support
2. Configure Hive Metastore to point to S3 paths
3. Update HUDI_BASE_PATH in `.env` to use `s3a://` protocol

### Scale Spark Workers (Optional)

Add more Spark workers by updating `docker-compose.yml`:

```yaml
spark-worker-2:
  # Copy spark-worker config and change container name
```

### Set Up Backups

```bash
# Backup PostgreSQL
docker compose exec postgres pg_dump -U morphix_user morphix > backup.sql

# Backup MinIO data
docker compose exec minio mc mirror /data backup-folder
```

## Troubleshooting

### If services fail to start:
- Check logs: `docker compose logs [service-name]`
- Verify ports aren't in use: `lsof -i :5432 -i :8080 -i :8083`
- Check disk space: `df -h`

### If APIs can't connect:
- Verify service names (use `localhost` when API runs on host)
- Check network: `docker network inspect morphix-poc_morphix-network`
- Test connectivity: `docker compose exec trino curl http://hive-metastore:9083`

### If Trino can't see tables:
- Verify Hive Metastore is running: `docker compose ps hive-metastore`
- Check metastore database: `docker compose exec postgres psql -U morphix_user -d metastore_db -c "\dt"`
- Restart services: `docker compose restart hive-metastore trino`

## Documentation

- [DOCKER_SETUP.md](DOCKER_SETUP.md) - Detailed Docker setup guide
- [QUICK_START_DOCKER.md](QUICK_START_DOCKER.md) - Quick reference
- [DOCKER_TROUBLESHOOTING.md](DOCKER_TROUBLESHOOTING.md) - Troubleshooting guide
- [API.md](API.md) - Complete API documentation
- [SETUP.md](SETUP.md) - General setup guide

---

**You're all set!** Your Docker Compose infrastructure is ready. Start your API and begin processing data! 🚀

