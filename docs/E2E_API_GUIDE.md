# End-to-End API Guide

Complete guide for using the Morphix ETL Platform APIs from user registration to data querying.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [User Flow](#user-flow)
4. [API Endpoints Reference](#api-endpoints-reference)
5. [Step-by-Step Walkthrough](#step-by-step-walkthrough)
6. [Complete Example Script](#complete-example-script)
7. [Error Handling](#error-handling)
8. [Best Practices](#best-practices)

---

## Overview

The Morphix ETL Platform provides a comprehensive REST API for:
- **User Management**: Registration, authentication, and authorization
- **Data Source Configuration**: MongoDB connection management
- **Schema Management**: Schema inference and validation
- **ETL Job Management**: Batch and stream job creation and execution
- **Data Querying**: Query processed data via Trino
- **Monitoring**: Job status and execution history

### API Base URL

```
http://localhost:8000
```

### Authentication

Most endpoints require authentication using Bearer tokens obtained via login.

---

## Prerequisites

1. **API Server Running**
   ```bash
   uvicorn src.api.mongo_api:app --reload --host 0.0.0.0 --port 8000
   ```

2. **MongoDB Accessible**
   - Local or remote MongoDB instance
   - Connection credentials

3. **PostgreSQL** (for job management and CDC checkpoints)
   - Accessible at configured host/port
   - Database: `morphix`

4. **Trino Server** (optional, for querying)
   - Accessible at configured host/port

---

## User Flow

```
┌─────────────────┐
│ 1. Register     │
│    User         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Login        │
│    Get Token    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Store MongoDB│
│    Credentials  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 4. Test         │
│    Connection   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 5. Generate     │
│    Schema       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 6. Create       │
│    Stream Job   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 7. Start Job    │
│    Execution    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 8. Monitor Job  │
│    Status       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 9. Query Data   │
│    via Trino    │
└─────────────────┘
```

---

## API Endpoints Reference

### Authentication Endpoints

#### 1. Register User

**Endpoint:** `POST /auth/register`

**Description:** Create a new user account.

**Request:**
```json
{
  "username": "alice",
  "email": "alice@example.com",
  "password": "StrongPass123!",
  "full_name": "Alice User"
}
```

**Response (201 Created):**
```json
{
  "id": 1,
  "username": "alice",
  "email": "alice@example.com",
  "full_name": "Alice User",
  "is_active": true,
  "is_superuser": false,
  "created_at": "2025-11-09T11:00:00Z"
}
```

**cURL Example:**
```bash
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "email": "alice@example.com",
    "password": "StrongPass123!",
    "full_name": "Alice User"
  }'
```

---

#### 2. Login

**Endpoint:** `POST /auth/login`

**Description:** Authenticate user and obtain access token.

**Request:**
```json
{
  "username": "alice",
  "password": "StrongPass123!"
}
```

**Response (200 OK):**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 1800,
  "user": {
    "id": 1,
    "username": "alice",
    "email": "alice@example.com",
    "full_name": "Alice User"
  }
}
```

**cURL Example:**
```bash
curl -X POST "http://localhost:8000/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "password": "StrongPass123!"
  }'
```

**Save Token:**
```bash
export TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

---

#### 3. Get Current User

**Endpoint:** `GET /auth/me`

**Description:** Get current authenticated user information.

**Headers:**
```
Authorization: Bearer <token>
```

**Response (200 OK):**
```json
{
  "id": 1,
  "username": "alice",
  "email": "alice@example.com",
  "full_name": "Alice User",
  "is_active": true,
  "created_at": "2025-11-09T11:00:00Z"
}
```

**cURL Example:**
```bash
curl -X GET "http://localhost:8000/auth/me" \
  -H "Authorization: Bearer $TOKEN"
```

---

### MongoDB Endpoints

#### 4. Store MongoDB Credentials

**Endpoint:** `POST /mongo/credentials`

**Description:** Store MongoDB connection credentials for later use.

**Headers:**
```
Authorization: Bearer <token>
Content-Type: application/json
```

**Request (with URI):**
```json
{
  "mongo_uri": "mongodb://user:pass@localhost:27017/test_db?authSource=admin",
  "database": "test_db",
  "collection": "meetings"
}
```

**Request (with individual fields):**
```json
{
  "username": "mongo_user",
  "password": "mongo_pass",
  "host": "localhost",
  "port": 27017,
  "database": "test_db",
  "collection": "meetings",
  "query": {"status": "active"}
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "user_id": "1"
}
```

**cURL Example:**
```bash
curl -X POST "http://localhost:8000/mongo/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings"
  }'
```

---

#### 5. Read Data from MongoDB

**Endpoint:** `POST /mongo/read`

**Description:** Read data from MongoDB collection.

**Headers:**
```
Authorization: Bearer <token>
Content-Type: application/json
```

**Request:**
```json
{
  "mongo_uri": "mongodb://localhost:27017/test_db",
  "database": "test_db",
  "collection": "meetings",
  "query": {},
  "limit": 100,
  "use_pyspark": false
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "data": [
    {
      "_id": "69009c16a01d1c1d889a16e9",
      "meeting_id": "meeting-test-001",
      "meeting_title": "Q4 Product Strategy Review",
      "user_id": "user-12345",
      ...
    }
  ],
  "count": 100
}
```

**cURL Example:**
```bash
curl -X POST "http://localhost:8000/mongo/read" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "limit": 10
  }'
```

---

### Schema Management Endpoints

#### 6. Generate Schema

**Endpoint:** `POST /schema/generate`

**Description:** Infer schema from MongoDB collection sample.

**Headers:**
```
Authorization: Bearer <token>
Content-Type: application/json
```

**Request:**
```json
{
  "mongo_uri": "mongodb://localhost:27017/test_db",
  "database": "test_db",
  "collection": "meetings",
  "sample_size": 1000,
  "include_constraints": true
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "schema": {
    "meeting_id": {
      "type": "string",
      "nullable": false,
      "min_length": 10,
      "max_length": 50
    },
    "meeting_title": {
      "type": "string",
      "nullable": false
    },
    "estimated_total_cost": {
      "type": "number",
      "nullable": true,
      "min": 0,
      "max": 10000
    },
    "source_location": {
      "type": "object",
      "nullable": false,
      "properties": {
        "city": {"type": "string"},
        "country": {"type": "string"}
      }
    }
  },
  "summary": {
    "total_fields": 25,
    "required_fields": 15,
    "optional_fields": 10,
    "nested_objects": 2,
    "arrays": 1
  },
  "sample_info": {
    "rows_analyzed": 1000,
    "columns": 25,
    "collection": "meetings",
    "database": "test_db"
  }
}
```

**cURL Example:**
```bash
curl -X POST "http://localhost:8000/schema/generate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "sample_size": 1000
  }'
```

---

#### 7. Transform Data

**Endpoint:** `POST /mongo/transform`

**Description:** Transform and validate data using schema.

**Headers:**
```
Authorization: Bearer <token>
Content-Type: application/json
```

**Request:**
```json
{
  "mongo_uri": "mongodb://localhost:27017/test_db",
  "database": "test_db",
  "collection": "meetings",
  "schema": {
    "meeting_id": {"type": "string"},
    "meeting_title": {"type": "string"}
  },
  "limit": 100,
  "flatten_data": true,
  "apply_schema": true,
  "strict_schema": false
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "data": [...],
  "statistics": {
    "total_records": 100,
    "valid_records": 98,
    "invalid_records": 2,
    "validation_errors": []
  }
}
```

---

### Job Management Endpoints

#### 8. Create Stream Job

**Endpoint:** `POST /jobs/stream/create`

**Description:** Create a new streaming ETL job for CDC.

**Headers:**
```
Authorization: Bearer <token>
Content-Type: application/json
```

**Request:**
```json
{
  "job_name": "meeting_cdc_realtime",
  "mongo_uri": "mongodb://localhost:27017/test_db",
  "database": "test_db",
  "collection": "meetings",
  "query": {},
  "hudi_table_name": "meetings",
  "hudi_base_path": "/tmp/hudi_test/meetings",
  "polling_interval_seconds": 5,
  "batch_size": 1000,
  "schedule": {
    "type": "interval",
    "interval_seconds": 60
  },
  "description": "Real-time CDC for meetings collection"
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "message": "Stream job created successfully",
  "job_id": "job_abc123",
  "job_name": "meeting_cdc_realtime"
}
```

**cURL Example:**
```bash
curl -X POST "http://localhost:8000/jobs/stream/create" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "meeting_cdc_realtime",
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "hudi_table_name": "meetings",
    "hudi_base_path": "/tmp/hudi_test/meetings",
    "polling_interval_seconds": 5,
    "batch_size": 1000,
    "schedule": {
      "type": "interval",
      "interval_seconds": 60
    }
  }'
```

---

#### 9. Create Batch Job

**Endpoint:** `POST /jobs/batch/create`

**Description:** Create a scheduled batch ETL job.

**Request:**
```json
{
  "job_name": "daily_meeting_sync",
  "mongo_uri": "mongodb://localhost:27017/test_db",
  "database": "test_db",
  "collection": "meetings",
  "date_field": "created_date",
  "hudi_table_name": "meetings",
  "hudi_base_path": "/tmp/hudi_test/meetings",
  "batch_size": 10000,
  "schedule": {
    "type": "cron",
    "cron_expression": "0 2 * * *"
  },
  "description": "Daily sync of meetings data"
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "message": "Batch job created successfully",
  "job_id": "job_xyz789",
  "job_name": "daily_meeting_sync"
}
```

---

#### 10. List Jobs

**Endpoint:** `GET /jobs`

**Description:** List all jobs for the current user.

**Headers:**
```
Authorization: Bearer <token>
```

**Query Parameters:**
- `job_type`: Filter by `batch` or `stream`
- `status`: Filter by status
- `enabled`: Filter by enabled/disabled

**Response (200 OK):**
```json
{
  "status": "ok",
  "jobs": [
    {
      "job_id": "job_abc123",
      "job_name": "meeting_cdc_realtime",
      "job_type": "stream",
      "enabled": true,
      "created_at": "2025-11-09T11:00:00Z",
      "last_run": "2025-11-09T12:00:00Z",
      "description": "Real-time CDC for meetings"
    }
  ]
}
```

**cURL Example:**
```bash
curl -X GET "http://localhost:8000/jobs?job_type=stream" \
  -H "Authorization: Bearer $TOKEN"
```

---

#### 11. Get Job Status

**Endpoint:** `GET /jobs/{job_id}/status`

**Description:** Get detailed job status and execution history.

**Headers:**
```
Authorization: Bearer <token>
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "job": {
    "job_id": "job_abc123",
    "job_name": "meeting_cdc_realtime",
    "job_type": "stream",
    "enabled": true,
    "created_at": "2025-11-09T11:00:00Z",
    "last_run": "2025-11-09T12:00:00Z"
  },
  "executions": [
    {
      "execution_id": "exec_001",
      "status": "completed",
      "started_at": "2025-11-09T12:00:00Z",
      "completed_at": "2025-11-09T12:05:00Z",
      "records_processed": 1500,
      "records_written": 1500
    }
  ],
  "metrics": {
    "total_executions": 10,
    "successful_executions": 9,
    "failed_executions": 1,
    "average_duration_seconds": 300.5,
    "total_records_processed": 15000
  }
}
```

---

#### 12. Start Job

**Endpoint:** `POST /jobs/{job_id}/start`

**Description:** Manually trigger job execution.

**Headers:**
```
Authorization: Bearer <token>
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "message": "Job started successfully",
  "execution_id": "exec_002"
}
```

---

#### 13. Stop Job

**Endpoint:** `POST /jobs/{job_id}/stop`

**Description:** Stop a running job.

**Headers:**
```
Authorization: Bearer <token>
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "message": "Job stopped successfully",
  "execution_id": "exec_002"
}
```

---

### Hudi Table Management Endpoints

#### 14. Create Hudi Table

**Endpoint:** `POST /hudi/table/create`

**Description:** Create a new Hudi table.

**Headers:**
```
Authorization: Bearer <token>
Content-Type: application/json
```

**Request:**
```json
{
  "table_name": "meetings",
  "database": "default",
  "base_path": "/tmp/hudi_test/meetings",
  "record_key_field": "_id",
  "precombine_field": "last_modified_date",
  "table_type": "COPY_ON_WRITE",
  "schema": {
    "meeting_id": {"type": "string"},
    "meeting_title": {"type": "string"},
    "estimated_total_cost": {"type": "number"}
  }
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "message": "Table created successfully",
  "table_name": "meetings",
  "database": "default"
}
```

---

#### 15. List Hudi Tables

**Endpoint:** `GET /hudi/tables`

**Description:** List all Hudi tables.

**Headers:**
```
Authorization: Bearer <token>
```

**Query Parameters:**
- `database`: Filter by database name

**Response (200 OK):**
```json
{
  "status": "ok",
  "tables": [
    {
      "table_name": "meetings",
      "database": "default",
      "table_type": "COPY_ON_WRITE",
      "total_records": 15000,
      "total_size_bytes": 5242880
    }
  ]
}
```

---

#### 16. Get Table Information

**Endpoint:** `GET /hudi/table/{table_name}`

**Description:** Get detailed information about a Hudi table.

**Headers:**
```
Authorization: Bearer <token>
```

**Query Parameters:**
- `database`: Database name (default: "default")

**Response (200 OK):**
```json
{
  "status": "ok",
  "table_name": "meetings",
  "database": "default",
  "base_path": "/tmp/hudi_test/meetings",
  "table_type": "COPY_ON_WRITE",
  "schema": {...},
  "total_files": 12,
  "total_records": 15000,
  "total_size_bytes": 5242880
}
```

---

### Trino Query Endpoints

#### 17. Trino Health Check

**Endpoint:** `GET /trino/health`

**Description:** Check Trino server availability.

**Headers:**
```
Authorization: Bearer <token>
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "message": "Trino server is reachable",
  "catalogs_count": 5
}
```

---

#### 18. List Trino Catalogs

**Endpoint:** `GET /trino/catalogs`

**Description:** Get available Trino catalogs.

**Headers:**
```
Authorization: Bearer <token>
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "catalogs": [
    {"catalog_name": "hive", "connector_name": "hive"},
    {"catalog_name": "system", "connector_name": "system"}
  ]
}
```

---

#### 19. List Trino Schemas

**Endpoint:** `GET /trino/schemas`

**Description:** List schemas in a catalog.

**Headers:**
```
Authorization: Bearer <token>
```

**Query Parameters:**
- `catalog`: Catalog name (required)

**Response (200 OK):**
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

---

#### 20. List Trino Tables

**Endpoint:** `GET /trino/tables`

**Description:** List tables in a catalog and schema.

**Headers:**
```
Authorization: Bearer <token>
```

**Query Parameters:**
- `catalog`: Catalog name (required)
- `schema`: Schema name (required)

**Response (200 OK):**
```json
{
  "status": "ok",
  "catalog": "hive",
  "schema": "default",
  "tables": [
    {
      "catalog": "hive",
      "schema": "default",
      "table": "meetings",
      "table_type": "BASE TABLE"
    }
  ]
}
```

---

#### 21. Describe Trino Table

**Endpoint:** `GET /trino/table/{table_name}/describe`

**Description:** Get table schema and column information.

**Headers:**
```
Authorization: Bearer <token>
```

**Query Parameters:**
- `catalog`: Catalog name (required)
- `schema`: Schema name (required)

**Response (200 OK):**
```json
{
  "status": "ok",
  "catalog": "hive",
  "schema": "default",
  "table": "meetings",
  "columns": [
    {"name": "meeting_id", "type": "varchar"},
    {"name": "meeting_title", "type": "varchar"},
    {"name": "estimated_total_cost", "type": "double"}
  ]
}
```

---

#### 22. Execute Trino Query

**Endpoint:** `POST /trino/query`

**Description:** Execute SQL query on data lake.

**Headers:**
```
Authorization: Bearer <token>
Content-Type: application/json
```

**Request:**
```json
{
  "sql": "SELECT * FROM hive.default.meetings LIMIT 10",
  "catalog": "hive",
  "schema": "default",
  "max_rows": 100,
  "timeout": 60
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "query": "SELECT * FROM hive.default.meetings LIMIT 10",
  "columns": ["meeting_id", "meeting_title", "user_id", "estimated_total_cost"],
  "data": [
    ["meeting-001", "Q4 Strategy Review", "user-12345", 2500],
    ["meeting-002", "Team Standup", "user-12346", 500]
  ],
  "rows_returned": 2,
  "execution_time_ms": 245.5,
  "query_id": "20231109_120000_00001_abcde",
  "error": null
}
```

**cURL Example:**
```bash
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) as total FROM hive.default.meetings",
    "max_rows": 10
  }'
```

---

## Step-by-Step Walkthrough

### Complete E2E Flow

#### Step 1: Register User

```bash
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "email": "alice@example.com",
    "password": "StrongPass123!",
    "full_name": "Alice User"
  }'
```

**Expected:** 201 Created with user details

---

#### Step 2: Login

```bash
curl -X POST "http://localhost:8000/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "alice",
    "password": "StrongPass123!"
  }'
```

**Save the token:**
```bash
export TOKEN="<access_token_from_response>"
```

---

#### Step 3: Store MongoDB Credentials

```bash
curl -X POST "http://localhost:8000/mongo/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings"
  }'
```

**Expected:** 200 OK with status confirmation

---

#### Step 4: Read Sample Data

```bash
curl -X POST "http://localhost:8000/mongo/read" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "limit": 10
  }'
```

**Expected:** 200 OK with array of documents

---

#### Step 5: Generate Schema

```bash
curl -X POST "http://localhost:8000/schema/generate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "sample_size": 1000
  }'
```

**Save the schema** from response for use in job creation.

---

#### Step 6: Create Stream Job

```bash
curl -X POST "http://localhost:8000/jobs/stream/create" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "meeting_cdc_realtime",
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "hudi_table_name": "meetings",
    "hudi_base_path": "/tmp/hudi_test/meetings",
    "polling_interval_seconds": 5,
    "batch_size": 1000,
    "schedule": {
      "type": "interval",
      "interval_seconds": 60
    },
    "description": "Real-time CDC for meetings"
  }'
```

**Save the job_id** from response.

---

#### Step 7: Start Job

```bash
export JOB_ID="<job_id_from_step_6>"

curl -X POST "http://localhost:8000/jobs/$JOB_ID/start" \
  -H "Authorization: Bearer $TOKEN"
```

**Expected:** 200 OK with execution_id

---

#### Step 8: Monitor Job Status

```bash
curl -X GET "http://localhost:8000/jobs/$JOB_ID/status" \
  -H "Authorization: Bearer $TOKEN"
```

**Check:**
- Job status (running, completed, failed)
- Records processed
- Execution history

---

#### Step 9: List Trino Tables

```bash
curl -X GET "http://localhost:8000/trino/tables?catalog=hive&schema=default" \
  -H "Authorization: Bearer $TOKEN"
```

**Expected:** 200 OK with list of tables

---

#### Step 10: Query Data via Trino

```bash
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT meeting_id, meeting_title, estimated_total_cost FROM hive.default.meetings LIMIT 10",
    "catalog": "hive",
    "schema": "default"
  }'
```

**Expected:** 200 OK with query results

---

## Complete Example Script

### Python Example

```python
#!/usr/bin/env python3
"""
Complete E2E API Example
"""

import requests
import time
import json

API_BASE_URL = "http://localhost:8000"

# Step 1: Register
register_data = {
    "username": "testuser",
    "email": "test@example.com",
    "password": "TestPass123!",
    "full_name": "Test User"
}

response = requests.post(f"{API_BASE_URL}/auth/register", json=register_data)
print(f"Registration: {response.status_code}")
print(response.json())

# Step 2: Login
login_data = {
    "username": "testuser",
    "password": "TestPass123!"
}

response = requests.post(f"{API_BASE_URL}/auth/login", json=login_data)
token_data = response.json()
token = token_data["access_token"]
print(f"Login: {response.status_code}")
print(f"Token: {token[:50]}...")

# Step 3: Store MongoDB Credentials
headers = {"Authorization": f"Bearer {token}"}
credentials_data = {
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings"
}

response = requests.post(
    f"{API_BASE_URL}/mongo/credentials",
    json=credentials_data,
    headers=headers
)
print(f"Credentials: {response.status_code}")
print(response.json())

# Step 4: Read Data
read_data = {
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "limit": 10
}

response = requests.post(
    f"{API_BASE_URL}/mongo/read",
    json=read_data,
    headers=headers
)
print(f"Read: {response.status_code}")
result = response.json()
print(f"Documents read: {result.get('count', 0)}")

# Step 5: Generate Schema
schema_data = {
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "sample_size": 100
}

response = requests.post(
    f"{API_BASE_URL}/schema/generate",
    json=schema_data,
    headers=headers
)
print(f"Schema: {response.status_code}")
schema_result = response.json()
print(f"Fields detected: {len(schema_result.get('schema', {}))}")

# Step 6: Create Stream Job
job_data = {
    "job_name": f"meeting_cdc_{int(time.time())}",
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "hudi_table_name": "meetings",
    "hudi_base_path": "/tmp/hudi_test/meetings",
    "polling_interval_seconds": 5,
    "batch_size": 1000,
    "schedule": {
        "type": "interval",
        "interval_seconds": 60
    }
}

response = requests.post(
    f"{API_BASE_URL}/jobs/stream/create",
    json=job_data,
    headers=headers
)
print(f"Job Creation: {response.status_code}")
job_result = response.json()
job_id = job_result.get("job_id")
print(f"Job ID: {job_id}")

# Step 7: Start Job
response = requests.post(
    f"{API_BASE_URL}/jobs/{job_id}/start",
    headers=headers
)
print(f"Start Job: {response.status_code}")
print(response.json())

# Step 8: Monitor Status
time.sleep(5)  # Wait a bit
response = requests.get(
    f"{API_BASE_URL}/jobs/{job_id}/status",
    headers=headers
)
print(f"Job Status: {response.status_code}")
print(json.dumps(response.json(), indent=2))

# Step 9: List Trino Tables
response = requests.get(
    f"{API_BASE_URL}/trino/tables?catalog=hive&schema=default",
    headers=headers
)
if response.status_code == 200:
    print(f"Trino Tables: {response.status_code}")
    tables = response.json()
    print(f"Tables found: {len(tables.get('tables', []))}")

# Step 10: Query Trino (if available)
query_data = {
    "sql": "SELECT COUNT(*) as total FROM hive.default.meetings",
    "max_rows": 10
}

response = requests.post(
    f"{API_BASE_URL}/trino/query",
    json=query_data,
    headers=headers
)
if response.status_code == 200:
    print(f"Trino Query: {response.status_code}")
    result = response.json()
    print(f"Rows returned: {result.get('rows_returned', 0)}")
else:
    print(f"Trino not available: {response.status_code}")
```

### Bash Example

```bash
#!/bin/bash

API_BASE_URL="http://localhost:8000"

# Step 1: Register
echo "Step 1: Registering user..."
REGISTER_RESPONSE=$(curl -s -X POST "$API_BASE_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "email": "test@example.com",
    "password": "TestPass123!",
    "full_name": "Test User"
  }')
echo "$REGISTER_RESPONSE"

# Step 2: Login
echo -e "\nStep 2: Logging in..."
LOGIN_RESPONSE=$(curl -s -X POST "$API_BASE_URL/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "TestPass123!"
  }')
TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.access_token')
echo "Token obtained: ${TOKEN:0:50}..."

# Step 3: Store Credentials
echo -e "\nStep 3: Storing MongoDB credentials..."
curl -s -X POST "$API_BASE_URL/mongo/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings"
  }' | jq '.'

# Step 4: Read Data
echo -e "\nStep 4: Reading data..."
curl -s -X POST "$API_BASE_URL/mongo/read" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "limit": 10
  }' | jq '.count'

# Step 5: Generate Schema
echo -e "\nStep 5: Generating schema..."
curl -s -X POST "$API_BASE_URL/schema/generate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "sample_size": 100
  }' | jq '.summary.total_fields'

# Step 6: Create Stream Job
echo -e "\nStep 6: Creating stream job..."
JOB_RESPONSE=$(curl -s -X POST "$API_BASE_URL/jobs/stream/create" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "meeting_cdc_test",
    "mongo_uri": "mongodb://localhost:27017/test_db",
    "database": "test_db",
    "collection": "meetings",
    "hudi_table_name": "meetings",
    "hudi_base_path": "/tmp/hudi_test/meetings",
    "polling_interval_seconds": 5,
    "batch_size": 1000,
    "schedule": {
      "type": "interval",
      "interval_seconds": 60
    }
  }')
JOB_ID=$(echo "$JOB_RESPONSE" | jq -r '.job_id')
echo "Job ID: $JOB_ID"

# Step 7: Start Job
echo -e "\nStep 7: Starting job..."
curl -s -X POST "$API_BASE_URL/jobs/$JOB_ID/start" \
  -H "Authorization: Bearer $TOKEN" | jq '.'

# Step 8: Check Status
echo -e "\nStep 8: Checking job status..."
sleep 5
curl -s -X GET "$API_BASE_URL/jobs/$JOB_ID/status" \
  -H "Authorization: Bearer $TOKEN" | jq '.job, .metrics'

# Step 9: List Trino Tables
echo -e "\nStep 9: Listing Trino tables..."
curl -s -X GET "$API_BASE_URL/trino/tables?catalog=hive&schema=default" \
  -H "Authorization: Bearer $TOKEN" | jq '.tables | length'

# Step 10: Query Trino
echo -e "\nStep 10: Querying Trino..."
curl -s -X POST "$API_BASE_URL/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT COUNT(*) as total FROM hive.default.meetings",
    "max_rows": 10
  }' | jq '.rows_returned'

echo -e "\n✅ E2E flow completed!"
```

---

## Error Handling

### Common HTTP Status Codes

- **200 OK**: Request successful
- **201 Created**: Resource created successfully
- **400 Bad Request**: Invalid request data
- **401 Unauthorized**: Authentication required or invalid token
- **403 Forbidden**: Insufficient permissions
- **404 Not Found**: Resource not found
- **422 Unprocessable Entity**: Validation error
- **500 Internal Server Error**: Server error

### Error Response Format

```json
{
  "detail": {
    "error": "error_code",
    "message": "Human-readable error message",
    "field": "field_name"  // Optional, for validation errors
  }
}
```

### Example Error Responses

**Authentication Error:**
```json
{
  "detail": "Could not validate credentials"
}
```

**Validation Error:**
```json
{
  "detail": [
    {
      "type": "missing",
      "loc": ["body", "mongo_uri"],
      "msg": "Field required"
    }
  ]
}
```

**Job Not Found:**
```json
{
  "detail": {
    "error": "job_not_found",
    "message": "Job not found or access denied"
  }
}
```

---

## Best Practices

### 1. Token Management

- **Store tokens securely**: Never commit tokens to version control
- **Handle expiration**: Tokens expire after 30 minutes, implement refresh logic
- **Use environment variables**: Store tokens in environment variables

```bash
export TOKEN="your_token_here"
```

### 2. Error Handling

- **Always check status codes**: Don't assume requests succeed
- **Handle network errors**: Implement retry logic for transient failures
- **Log errors**: Include error details in logs for debugging

```python
try:
    response = requests.post(url, json=data, headers=headers)
    response.raise_for_status()  # Raises exception for 4xx/5xx
    return response.json()
except requests.exceptions.HTTPError as e:
    logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
    raise
except requests.exceptions.RequestException as e:
    logger.error(f"Request failed: {e}")
    raise
```

### 3. Rate Limiting

- **Respect rate limits**: Don't overwhelm the API with requests
- **Implement backoff**: Use exponential backoff for retries
- **Batch operations**: Combine multiple operations when possible

### 4. Data Validation

- **Validate before sending**: Check data types and required fields
- **Handle schema evolution**: Be prepared for schema changes
- **Test with sample data**: Use small datasets for testing

### 5. Monitoring

- **Track job status**: Regularly check job execution status
- **Monitor metrics**: Watch execution counts, success rates, durations
- **Set up alerts**: Configure alerts for job failures

### 6. Security

- **Use HTTPS in production**: Never send tokens over unencrypted connections
- **Rotate credentials**: Regularly update passwords and tokens
- **Limit permissions**: Use least-privilege principle for API access

---

## Testing with Data Generator

To test CDC functionality, use the data generator script:

```bash
# Generate continuous data changes
python scripts/cdc_data_generator.py \
  --mongo-uri "mongodb://localhost:27017/test_db" \
  --database test_db \
  --collection meetings \
  --duration 300 \
  --ops-per-sec 2.0 \
  --insert-weight 0.5 \
  --update-weight 0.4 \
  --delete-weight 0.1
```

This will:
- Insert new meeting documents
- Update existing documents
- Delete documents (soft delete)
- Generate schema evolution scenarios

Monitor the stream job status to see CDC processing in real-time.

---

## Additional Resources

- [API Reference](./API.md) - Complete API documentation
- [Components Guide](./COMPONENTS.md) - System architecture
- [Quick Start](./QUICK_START_DOCKER.md) - Docker setup guide
- [E2E Test Guide](./END_TO_END_TEST.md) - Testing guide

---

## Support

For issues or questions:
1. Check the error response details
2. Review the logs: `/tmp/morphix_api.log`
3. Verify all prerequisites are met
4. Check API server health: `GET /health`

---

**Last Updated:** November 9, 2025

