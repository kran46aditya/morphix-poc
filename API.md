# API Reference

Complete API documentation for the Morphix ETL Platform.

## Table of Contents

1. [Authentication](#authentication)
2. [MongoDB Operations](#mongodb-operations)
3. [Schema Management](#schema-management)
4. [Data Transformation](#data-transformation)
5. [Hudi Table Management](#hudi-table-management)
6. [Job Management](#job-management)
7. [Trino Query API](#trino-query-api)

---

## Authentication

All endpoints require authentication except registration and login.

### Register User

```bash
POST /auth/register
```

**Request:**
```json
{
  "username": "testuser",
  "email": "test@example.com",
  "password": "TestPass123!",
  "full_name": "Test User"
}
```

**Response:**
```json
{
  "username": "testuser",
  "email": "test@example.com",
  "full_name": "Test User",
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

### Login

```bash
POST /auth/login
```

**Request:**
```bash
curl -X POST "http://localhost:8000/auth/login" \
  -d "username=testuser&password=TestPass123!"
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

### Using Authentication Token

Include the token in all subsequent requests:

```bash
curl -X GET "http://localhost:8000/endpoint" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

---

## MongoDB Operations

### Save MongoDB Credentials

Store MongoDB connection credentials for later use.

```bash
POST /mongo/credentials
```

**Request:**
```json
{
  "user_id": "user123",
  "username": "mongo_user",
  "password": "mongo_pass",
  "host": "localhost",
  "port": 27017,
  "database": "testdb",
  "collection": "users",
  "query": {"active": true}
}
```

**Alternative with URI:**
```json
{
  "user_id": "user456",
  "mongo_uri": "mongodb://user:pass@localhost:27017/testdb?authSource=admin",
  "database": "testdb",
  "collection": "products"
}
```

**Response:**
```json
{
  "status": "ok",
  "message": "Credentials saved successfully",
  "user_id": "user123"
}
```

### Read Data from MongoDB

Read data using PyMongo or PySpark.

```bash
POST /mongo/read
```

**Request with Direct Credentials:**
```json
{
  "mongo_uri": "mongodb://localhost:27017/testdb",
  "database": "testdb",
  "collection": "users",
  "query": {"active": true},
  "use_pyspark": false,
  "limit": 100
}
```

**Request with Stored Credentials:**
```json
{
  "user_id": "user123",
  "query": {"age": {"$gte": 18}},
  "use_pyspark": false,
  "limit": 50
}
```

**Response:**
```json
{
  "status": "ok",
  "rows_returned": 42,
  "data": [
    {"_id": "507f1f77bcf86cd799439011", "name": "John", "age": 30},
    {"_id": "507f191e810c19729de860ea", "name": "Jane", "age": 25}
  ],
  "method": "pymongo"
}
```

**Parameters:**
- `use_pyspark`: `true` for Spark DataFrame, `false` for pandas DataFrame
- `limit`: Maximum number of documents to return
- `query`: MongoDB query filter (optional)

---

## Schema Management

### Generate Schema from Collection

Automatically generate schema from MongoDB collection samples.

```bash
POST /schema/generate
```

**Request:**
```json
{
  "mongo_uri": "mongodb://localhost:27017/testdb",
  "database": "testdb",
  "collection": "users",
  "query": {"active": true},
  "sample_size": 1000,
  "include_constraints": true
}
```

**Response:**
```json
{
  "status": "ok",
  "schema": {
    "user_id": {
      "type": "integer",
      "nullable": false,
      "description": "User identifier"
    },
    "username": {
      "type": "string",
      "nullable": false,
      "max_length": 50,
      "description": "User login name"
    },
    "email": {
      "type": "string",
      "nullable": true,
      "patterns": ["email"],
      "description": "User email"
    }
  },
  "summary": {
    "total_fields": 15,
    "nullable_fields": 8,
    "complex_fields": 2
  }
}
```

**Parameters:**
- `sample_size`: Number of documents to analyze (default: 1000)
- `include_constraints`: Include min/max values, patterns (default: true)

### Upload Avro Schema

Upload an Avro schema file for validation.

```bash
POST /schema/upload-avro
```

**Request:**
```bash
curl -X POST "http://localhost:8000/schema/upload-avro" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -F "file=@schema.avsc"
```

**Response:**
```json
{
  "status": "ok",
  "message": "Schema uploaded successfully",
  "schema_name": "UserRecord",
  "fields_count": 8
}
```

### Load Schema from JSON

Load schema definition from JSON format.

```bash
POST /schema/from-json
```

**Request:**
```json
{
  "user_id": {
    "type": "integer",
    "nullable": false,
    "min_value": 1,
    "max_value": 999999
  },
  "username": {
    "type": "string",
    "nullable": false,
    "max_length": 50,
    "patterns": ["alphanumeric"]
  },
  "email": {
    "type": "string",
    "nullable": true,
    "patterns": ["email"],
    "max_length": 255
  }
}
```

**Response:**
```json
{
  "status": "ok",
  "message": "Schema loaded successfully",
  "fields_count": 3
}
```

---

## Data Transformation

### Transform Data

Transform MongoDB data with optional schema validation.

```bash
POST /mongo/transform
```

**Request:**
```json
{
  "mongo_uri": "mongodb://localhost:27017/testdb",
  "database": "testdb",
  "collection": "users",
  "query": {"active": true},
  "schema": {
    "user_id": {"type": "integer", "nullable": false},
    "username": {"type": "string", "nullable": false, "max_length": 50},
    "email": {"type": "string", "nullable": true, "patterns": ["email"]}
  },
  "limit": 100,
  "flatten_data": true,
  "apply_schema": true,
  "strict_schema": false
}
```

**Response:**
```json
{
  "status": "ok",
  "rows_transformed": 85,
  "rows_valid": 85,
  "rows_invalid": 0,
  "transformed_data": [
    {
      "user_id": 1,
      "username": "john_doe",
      "email": "john@example.com",
      "user_profile_age": 30
    }
  ],
  "validation_errors": []
}
```

**Parameters:**
- `flatten_data`: Flatten nested structures (default: `true`)
- `apply_schema`: Apply schema validation/transformation (default: `true`)
- `strict_schema`: Fail on validation errors (default: `false`)

---

## Hudi Table Management

### Create Hudi Table

Create a new Hudi table.

```bash
POST /hudi/table/create
```

**Request:**
```json
{
  "table_name": "products",
  "database": "default",
  "base_path": "/tmp/hudi/products",
  "record_key_field": "id",
  "precombine_field": "updated_at",
  "table_type": "COPY_ON_WRITE",
  "schema": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "price": {"type": "double"}
  }
}
```

**Response:**
```json
{
  "status": "ok",
  "message": "Table created successfully",
  "table_name": "products",
  "database": "default"
}
```

### List Hudi Tables

List all Hudi tables.

```bash
GET /hudi/tables?database=default
```

**Response:**
```json
{
  "status": "ok",
  "tables": [
    {
      "table_name": "products",
      "database": "default",
      "table_type": "COPY_ON_WRITE",
      "total_records": 15000,
      "total_size_bytes": 5242880
    }
  ]
}
```

### Get Table Information

```bash
GET /hudi/table/info?table_name=products&database=default
```

**Response:**
```json
{
  "status": "ok",
  "table_name": "products",
  "database": "default",
  "base_path": "/tmp/hudi/products",
  "table_type": "COPY_ON_WRITE",
  "schema": {...},
  "total_files": 12,
  "total_records": 15000,
  "total_size_bytes": 5242880
}
```

---

## Job Management

### Create Batch Job

Create a scheduled batch ETL job.

```bash
POST /jobs/batch/create
```

**Request:**
```json
{
  "job_name": "daily_product_sync",
  "description": "Daily sync of products from MongoDB",
  "mongo_credentials": {
    "user_id": "user123"
  },
  "hudi_config": {
    "table_name": "products",
    "database": "default"
  },
  "schedule": {
    "type": "cron",
    "expression": "0 2 * * *"
  },
  "enabled": true
}
```

**Response:**
```json
{
  "status": "ok",
  "job_id": "job_abc123",
  "job_name": "daily_product_sync",
  "job_type": "batch"
}
```

### Create Stream Job

Create a streaming ETL job.

```bash
POST /jobs/stream/create
```

**Request:**
```json
{
  "job_name": "realtime_orders",
  "description": "Real-time order processing",
  "mongo_credentials": {
    "user_id": "user123"
  },
  "hudi_config": {
    "table_name": "orders",
    "database": "default"
  },
  "stream_config": {
    "checkpoint_location": "/tmp/checkpoints/orders"
  },
  "enabled": true
}
```

### List Jobs

```bash
GET /jobs
```

**Query Parameters:**
- `job_type`: Filter by `batch` or `stream`
- `status`: Filter by status
- `enabled`: Filter by enabled/disabled

**Response:**
```json
{
  "status": "ok",
  "jobs": [
    {
      "job_id": "job_abc123",
      "job_name": "daily_product_sync",
      "job_type": "batch",
      "status": "running",
      "enabled": true,
      "created_at": "2024-01-15T10:00:00Z"
    }
  ]
}
```

### Start Job

```bash
POST /jobs/{job_id}/start
```

### Stop Job

```bash
POST /jobs/{job_id}/stop
```

### Update Job

```bash
PUT /jobs/{job_id}
```

**Request:**
```json
{
  "description": "Updated description",
  "enabled": false,
  "schedule": {
    "type": "cron",
    "expression": "0 3 * * *"
  }
}
```

### Delete Job

```bash
DELETE /jobs/{job_id}
```

### Enable/Disable Job

```bash
PUT /jobs/{job_id}/enable
PUT /jobs/{job_id}/disable
```

### Get Job Status

```bash
GET /jobs/{job_id}/status
```

**Response:**
```json
{
  "status": "ok",
  "job_id": "job_abc123",
  "job_status": "running",
  "last_run": "2024-01-15T02:00:00Z",
  "next_run": "2024-01-16T02:00:00Z",
  "execution_count": 45,
  "success_count": 43,
  "failure_count": 2
}
```

---

## Trino Query API

### Health Check

```bash
GET /trino/health
```

**Response:**
```json
{
  "status": "ok",
  "message": "Trino server is reachable",
  "catalogs_count": 5
}
```

### List Catalogs

```bash
GET /trino/catalogs
```

**Response:**
```json
{
  "status": "ok",
  "catalogs": [
    {"catalog_name": "hive", "connector_name": "hive"},
    {"catalog_name": "system", "connector_name": "system"}
  ]
}
```

### List Schemas

```bash
GET /trino/schemas?catalog=hive
```

**Response:**
```json
{
  "status": "ok",
  "catalog": "hive",
  "schemas": [
    {"catalog": "hive", "schema": "default"},
    {"catalog": "hive", "schema": "test_schema"}
  ]
}
```

### List Tables

```bash
GET /trino/tables?catalog=hive&schema=default
```

**Response:**
```json
{
  "status": "ok",
  "catalog": "hive",
  "schema": "default",
  "tables": [
    {
      "catalog": "hive",
      "schema": "default",
      "table": "products",
      "table_type": "BASE TABLE"
    }
  ]
}
```

### Describe Table

```bash
GET /trino/table/products/describe?catalog=hive&schema=default
```

**Response:**
```json
{
  "status": "ok",
  "catalog": "hive",
  "schema": "default",
  "table": "products",
  "columns": [
    {"name": "id", "type": "varchar"},
    {"name": "name", "type": "varchar"},
    {"name": "price", "type": "double"}
  ]
}
```

### Execute Query

```bash
POST /trino/query
```

**Request:**
```json
{
  "sql": "SELECT * FROM hive.default.products LIMIT 10",
  "catalog": "hive",
  "schema": "default",
  "max_rows": 100,
  "timeout": 60
}
```

**Response (Success):**
```json
{
  "status": "ok",
  "query": "SELECT * FROM hive.default.products LIMIT 10",
  "columns": ["id", "name", "price", "category"],
  "data": [
    ["1", "Product A", 99.99, "Electronics"],
    ["2", "Product B", 149.99, "Clothing"]
  ],
  "rows_returned": 2,
  "execution_time_ms": 245.5,
  "query_id": "20231215_123456_00001_abcde",
  "error": null
}
```

**Response (Error):**
```json
{
  "status": "error",
  "query": "SELECT * FROM non_existent_table",
  "columns": [],
  "data": [],
  "rows_returned": 0,
  "execution_time_ms": 12.3,
  "query_id": null,
  "error": "Table 'hive.default.non_existent_table' does not exist"
}
```

**Query Hudi Tables:**
```json
{
  "sql": "SELECT _hoodie_commit_time, _hoodie_record_key, * FROM hive.default.hudi_products WHERE _hoodie_partition_path = '2024/01/15' LIMIT 100",
  "catalog": "hive",
  "schema": "default"
}
```

---

## Error Responses

All endpoints return consistent error formats:

```json
{
  "detail": {
    "error": "error_code",
    "message": "Human-readable error message",
    "help": "Additional help text"
  }
}
```

**Common Error Codes:**
- `400` - Bad Request (invalid input)
- `401` - Unauthorized (missing/invalid token)
- `404` - Not Found (resource doesn't exist)
- `500` - Internal Server Error
- `503` - Service Unavailable (Trino server down, etc.)

---

## Rate Limiting

Rate limiting can be configured via environment variables:
- `API_RATE_LIMIT_ENABLED=true`
- `API_RATE_LIMIT_REQUESTS=100`
- `API_RATE_LIMIT_WINDOW=60`

---

## API Documentation

Interactive API documentation available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

