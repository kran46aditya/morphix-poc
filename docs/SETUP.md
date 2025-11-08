# Setup Guide

Complete installation and configuration guide for the Morphix ETL Platform.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Python Environment Setup](#python-environment-setup)
3. [Database Setup](#database-setup)
4. [MongoDB Configuration](#mongodb-configuration)
5. [Trino Server Setup](#trino-server-setup)
6. [Environment Configuration](#environment-configuration)
7. [Verification](#verification)

---

## Prerequisites

### System Requirements

- Python 3.11 or higher
- PostgreSQL 12+ (for credential and job storage)
- MongoDB (source data)
- Docker (optional, for Trino server)
- Java 11+ (required for Spark/Hudi operations)
- 8GB+ RAM recommended

### Required Services

- MongoDB server (running and accessible)
- PostgreSQL database (for application metadata)
- Trino server (optional, for querying Hudi tables)

---

## Python Environment Setup

### 1. Clone Repository

```bash
cd /path/to/workspace
git clone <repository-url>
cd morphix-poc
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Key dependencies include:
- `fastapi` - API framework
- `pymongo` - MongoDB driver
- `pyspark` - Spark integration
- `pandas` - Data manipulation
- `psycopg2` - PostgreSQL driver
- `trino` - Trino Python client
- `hudi` - Apache Hudi

### 4. Verify Installation

```bash
python -c "import fastapi, pymongo, pyspark, pandas, trino; print('All dependencies installed')"
```

---

## Database Setup

### PostgreSQL Setup

PostgreSQL is used to store:
- MongoDB connection credentials
- Job configurations and execution history
- User authentication data

#### 1. Install PostgreSQL

**macOS:**
```bash
brew install postgresql
brew services start postgresql
```

**Linux:**
```bash
sudo apt-get install postgresql postgresql-contrib
sudo systemctl start postgresql
```

**Windows:**
Download from https://www.postgresql.org/download/

#### 2. Create Database

```bash
psql -U postgres
```

```sql
CREATE DATABASE morphix_db;
CREATE USER morphix_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE morphix_db TO morphix_user;
\q
```

#### 3. Update Configuration

Add to `.env` file:
```bash
DATABASE_URL=postgresql://morphix_user:your_password@localhost:5432/morphix_db
```

---

## MongoDB Configuration

### 1. MongoDB Server Setup

Ensure MongoDB is running and accessible:

```bash
# Check MongoDB status
mongosh --eval "db.version()"

# Or with legacy client
mongo --eval "db.version()"
```

### 2. MongoDB Credentials

You can either:
- Store credentials via API (`POST /mongo/credentials`)
- Use direct connection in requests

### 3. Test Connection

```bash
curl -X POST "http://localhost:8000/mongo/credentials" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "test_user",
    "username": "your_mongo_user",
    "password": "your_mongo_pass",
    "host": "localhost",
    "port": 27017,
    "database": "test_db",
    "collection": "test_collection"
  }'
```

---

## Trino Server Setup

Trino is required for querying Hudi tables. The Python client is included in `requirements.txt`, but the Trino server must be installed separately.

### Option 1: Quick Setup Script (Recommended)

```bash
./setup-trino.sh
```

This script will:
- Check for Docker
- Pull Trino Docker image
- Start Trino server on port 8080
- Verify connectivity

### Option 2: Docker Compose

```bash
docker-compose -f docker-compose.trino.yml up -d
```

### Option 3: Manual Docker Setup

```bash
docker pull trinodb/trino:latest
docker run -d \
  --name trino-server \
  -p 8080:8080 \
  trinodb/trino:latest
```

### Option 4: Local Installation

1. Download Trino:
```bash
wget https://repo1.maven.org/maven2/io/trino/trino-server/450/trino-server-450.tar.gz
tar -xzf trino-server-450.tar.gz
cd trino-server-450
```

2. Configure:
```bash
mkdir -p etc
echo "coordinator=true" > etc/config.properties
echo "http-server.http.port=8080" >> etc/config.properties
```

3. Start:
```bash
bin/launcher start
```

### Verify Trino

```bash
curl http://localhost:8080/v1/info
```

### Configure Hive Connector (for Hudi Tables)

Create `trino-config/catalog/hive.properties`:

```properties
connector.name=hive-hadoop2
hive.metastore.uri=file:///tmp/hudi
```

Mount this config when running Docker:

```bash
docker run -d \
  --name trino-server \
  -p 8080:8080 \
  -v $(pwd)/trino-config:/etc/trino \
  trinodb/trino:latest
```

---

## Environment Configuration

Create a `.env` file in the project root:

```bash
# Database Configuration
DATABASE_URL=postgresql://morphix_user:your_password@localhost:5432/morphix_db

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# MongoDB Defaults (optional)
MONGO_HOST=localhost
MONGO_PORT=27017

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=2g

# Hudi Configuration
HUDI_BASE_PATH=/tmp/hudi
HUDI_TABLE_TYPE=COPY_ON_WRITE
HUDI_OPERATION_TYPE=UPSERT

# Trino Configuration
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=admin
TRINO_CATALOG=hive
TRINO_SCHEMA=default
TRINO_HTTP_SCHEME=http
TRINO_REQUEST_TIMEOUT=300

# Job Scheduler
JOB_SCHEDULER_ENABLED=true

# Authentication
AUTH_SECRET_KEY=your-secret-key-change-in-production
AUTH_ALGORITHM=HS256
AUTH_ACCESS_TOKEN_EXPIRE_MINUTES=30
```

### Environment Variable Prefixes

All settings can be overridden with environment variables using these prefixes:

- `DB_*` - Database settings
- `API_*` - API server settings
- `MONGO_*` - MongoDB settings
- `SPARK_*` - Spark settings
- `HUDI_*` - Hudi settings
- `TRINO_*` - Trino settings
- `JOB_*` - Job scheduler settings
- `AUTH_*` - Authentication settings

---

## Verification

### 1. Start API Server

```bash
uvicorn src.api.mongo_api:app --reload --port 8000
```

Server should start at `http://localhost:8000`

### 2. Check API Documentation

Visit `http://localhost:8000/docs` for interactive API documentation.

### 3. Test Authentication

```bash
# Register user
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "email": "test@example.com",
    "password": "TestPass123!",
    "full_name": "Test User"
  }'

# Login
curl -X POST "http://localhost:8000/auth/login" \
  -d "username=testuser&password=TestPass123!"
```

### 4. Test MongoDB Connection

```bash
TOKEN="your_access_token_here"

curl -X POST "http://localhost:8000/mongo/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "test_user",
    "host": "localhost",
    "port": 27017,
    "database": "test_db",
    "collection": "test_collection"
  }'
```

### 5. Test Trino Connection (if configured)

```bash
TOKEN="your_access_token_here"

curl -X GET "http://localhost:8000/trino/health" \
  -H "Authorization: Bearer $TOKEN"
```

---

## Troubleshooting

### Python Dependencies

If installation fails:
```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt --no-cache-dir
```

### PostgreSQL Connection Issues

- Verify PostgreSQL is running: `pg_isready`
- Check connection string format
- Ensure database and user exist
- Check firewall settings

### MongoDB Connection Issues

- Verify MongoDB is running: `mongosh --eval "db.version()"`
- Check network connectivity
- Verify credentials
- Check MongoDB authentication requirements

### Trino Server Issues

- Check if Trino is running: `curl http://localhost:8080/v1/info`
- Check Docker container: `docker ps | grep trino`
- View logs: `docker logs trino-server`
- Verify port 8080 is not in use: `lsof -i :8080`

### Spark/Hudi Issues

- Ensure Java 11+ is installed: `java -version`
- Check `JAVA_HOME` environment variable
- Verify Spark can access MongoDB connector
- Check Hudi base path permissions

### Port Conflicts

- API port (8000): Change in `.env` or command line
- Trino port (8080): Update `TRINO_PORT` in `.env`
- PostgreSQL (5432): Standard port, rarely conflicts
- MongoDB (27017): Standard port, rarely conflicts

---

## Next Steps

After setup is complete:

1. Review [API.md](API.md) for available endpoints
2. Review [FLOW.md](FLOW.md) for system architecture and workflows
3. Start with simple ETL pipeline:
   - Store MongoDB credentials
   - Read data from MongoDB
   - Generate schema
   - Transform data
   - Write to Hudi table
   - Query via Trino

---

## Production Considerations

### Security

- Change default passwords
- Use strong `AUTH_SECRET_KEY`
- Enable HTTPS
- Restrict network access
- Use environment-specific `.env` files

### Performance

- Tune Spark executor memory
- Configure connection pooling
- Set appropriate timeouts
- Monitor resource usage

### Monitoring

- Set up logging aggregation
- Monitor API response times
- Track job execution metrics
- Alert on failures

### Backup

- Regular PostgreSQL backups
- Hudi table backup strategy
- Configuration backup
- Credential management

