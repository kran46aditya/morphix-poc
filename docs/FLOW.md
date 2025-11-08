# System Flow & Architecture

Complete overview of the Morphix ETL Platform architecture, data flow, and system design.

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Data Flow](#data-flow)
4. [Component Details](#component-details)
5. [Module Interactions](#module-interactions)
6. [ETL Pipeline](#etl-pipeline)
7. [Workflow Examples](#workflow-examples)

---

## System Overview

The Morphix ETL Platform is a comprehensive data processing system that:
- Extracts data from MongoDB
- Transforms and validates data using schemas
- Loads data into Hudi tables (data lake format)
- Provides SQL querying via Trino
- Manages batch and streaming jobs
- Offers RESTful API for all operations

**Key Technologies:**
- FastAPI (Python web framework)
- MongoDB (source data)
- PySpark (distributed processing)
- Apache Hudi (data lake storage)
- Trino (SQL query engine)
- PostgreSQL (metadata storage)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Layer (FastAPI)                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │  Auth    │  │ MongoDB  │  │ Schema   │  │  Jobs    │      │
│  │  API     │  │   API    │  │   API    │  │   API    │      │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │
│  ┌──────────┐  ┌──────────┐                                   │
│  │  Hudi    │  │  Trino   │                                   │
│  │   API    │  │   API    │                                   │
│  └──────────┘  └──────────┘                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   MongoDB    │    │  PostgreSQL  │    │  Trino       │
│   (Source)   │    │  (Metadata)  │    │  (Query)     │
└──────────────┘    └──────────────┘    └──────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ETL Processing Layer                         │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  Module 1:  │→ │  Module 2:   │→ │  Module 3:   │         │
│  │  MongoDB    │  │  Data        │  │  Hudi        │         │
│  │  Reader     │  │  Transformer │  │  Writer      │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│        │                  │                  │                 │
│        │                  ▼                  │                 │
│        │          ┌──────────────┐           │                 │
│        │          │  Schema      │           │                 │
│        └─────────→│  Generator   │←──────────┘                 │
│                   └──────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────┐
                    │  Hudi Tables │
                    │  (Parquet)   │
                    └──────────────┘
                              │
                              ▼
                    ┌──────────────┐
                    │  Trino Query  │
                    │  (SQL Access) │
                    └──────────────┘
```

---

## Data Flow

### 1. Complete ETL Pipeline Flow

```
MongoDB Collection
    │
    │ [API: POST /mongo/credentials]
    ▼
PostgreSQL (Store Credentials)
    │
    │ [API: POST /mongo/read]
    ▼
Module 1: MongoDB Reader
    ├─ PyMongo (small datasets)
    └─ PySpark (large datasets)
    │
    │ Raw DataFrame
    ▼
Module 2: Data Transformer
    ├─ Flatten nested structures
    ├─ Clean data
    └─ Type conversion
    │
    │ [API: POST /schema/generate]
    ▼
Schema Generator
    ├─ Analyze sample data
    ├─ Infer types
    └─ Generate constraints
    │
    │ Schema Definition
    ▼
Data Transformer (with Schema)
    ├─ Apply schema validation
    ├─ Transform types
    └─ Validate constraints
    │
    │ Validated DataFrame
    ▼
Module 3: Hudi Writer
    ├─ Create Hudi table
    ├─ Write data (UPSERT)
    └─ Commit changes
    │
    │ Parquet Files (Hudi format)
    ▼
Hudi Table Storage
    │
    │ [Trino configured with Hive connector]
    ▼
Trino Query Engine
    │
    │ [API: POST /trino/query]
    ▼
SQL Results (JSON)
```

### 2. Job Execution Flow

```
Job Scheduler (Background Thread)
    │
    │ Scheduled Trigger (Cron/Interval)
    ▼
Job Manager
    ├─ Fetch job configuration
    ├─ Load MongoDB credentials
    ├─ Load Hudi config
    └─ Execute pipeline
    │
    ├─→ Module 1: Read from MongoDB
    ├─→ Module 2: Transform data
    ├─→ Module 3: Write to Hudi
    │
    ▼
Job Execution Record
    ├─ Status (running/success/failed)
    ├─ Execution time
    ├─ Records processed
    └─ Error logs
    │
    ▼
PostgreSQL (Job History)
```

---

## Component Details

### Module 1: MongoDB Reader

**Purpose:** Extract data from MongoDB into DataFrames

**Input:**
- MongoDB connection (URI or credentials)
- Database and collection names
- Query filter (optional)
- Limit (optional)

**Output:**
- Pandas DataFrame (PyMongo) or Spark DataFrame (PySpark)

**Key Features:**
- Support for both PyMongo and PySpark connectors
- Query filtering and projection
- BSON type serialization
- Connection pooling

**Files:**
- `src/etl/mongo_api_reader.py`
- `src/mongodb/connection.py`

### Module 2: Data Transformer

**Purpose:** Transform, clean, and validate data

**Input:**
- DataFrame (from Module 1)
- Schema definition (optional)
- Transformation options

**Output:**
- Cleaned and validated DataFrame

**Key Features:**
- Flatten nested JSON structures
- Type conversion and validation
- Constraint checking (min/max, patterns)
- Default value injection
- Error collection (strict/lenient modes)

**Files:**
- `src/etl/data_transformer.py`
- `src/etl/schema_generator.py`

### Module 3: Hudi Writer

**Purpose:** Write data to Hudi tables (data lake)

**Input:**
- DataFrame (from Module 2)
- Hudi table configuration
- Write options

**Output:**
- Hudi table (Parquet format)
- Metadata updates

**Key Features:**
- Upsert operations (update existing, insert new)
- Partition management
- Schema evolution support
- Transaction management
- Table statistics

**Files:**
- `src/hudi_writer/writer.py`
- `src/hudi_writer/table_manager.py`

### Schema Management System

**Purpose:** Generate, store, and apply data schemas

**Components:**
1. **Schema Generator**
   - Analyzes MongoDB documents
   - Infers data types
   - Detects constraints
   - Generates schema JSON

2. **Schema Loader**
   - Loads Avro schemas
   - Loads JSON schemas
   - Validates schema format

3. **Schema Application**
   - Validates data against schema
   - Transforms types
   - Applies constraints
   - Reports validation errors

**Files:**
- `src/etl/schema_generator.py`

### Job Management System

**Purpose:** Schedule and execute ETL jobs

**Components:**
1. **Job Manager**
   - CRUD operations for jobs
   - Job configuration storage
   - Execution history

2. **Job Scheduler**
   - Background thread scheduler
   - Cron and interval scheduling
   - Job execution orchestration

3. **Job Processors**
   - Batch job processor
   - Stream job processor

**Files:**
- `src/jobs/job_manager.py`
- `src/jobs/scheduler.py`
- `src/jobs/batch_jobs.py`
- `src/jobs/stream_jobs.py`

### Trino Integration

**Purpose:** SQL querying of Hudi tables

**Components:**
1. **Trino Client**
   - Connection management
   - Query execution
   - Result handling

2. **Catalog Discovery**
   - List catalogs
   - List schemas
   - List tables
   - Describe tables

**Files:**
- `src/trino/client.py`
- `src/api/trino_api.py`

---

## Module Interactions

### Authentication Flow

```
User Request
    │
    │ [No Token] → 401 Unauthorized
    │
    │ [Valid Token] → Extract user info
    ▼
Dependency Injection (FastAPI)
    │
    ▼
Endpoint Handler
    │
    ▼
Business Logic
```

### Schema Generation Flow

```
MongoDB Collection
    │
    ▼
Sample Documents (N documents)
    │
    ▼
Field Analysis
    ├─ Type inference
    ├─ Nullability detection
    ├─ Value range detection
    └─ Pattern detection
    │
    ▼
Schema Assembly
    ├─ Field definitions
    ├─ Constraints
    └─ Metadata
    │
    ▼
Schema JSON
```

### Data Transformation Flow

```
Raw DataFrame
    │
    ├─→ Flattening (if enabled)
    │   ├─ Nested objects → flat fields
    │   ├─ Arrays → count + first element
    │   └─ Map structures → key_value fields
    │
    ├─→ Data Cleaning
    │   ├─ Trim whitespace
    │   ├─ Normalize nulls
    │   └─ Remove empty rows
    │
    └─→ Schema Application (if enabled)
        ├─ Type conversion
        ├─ Constraint validation
        ├─ Default values
        └─ Error collection
    │
    ▼
Transformed DataFrame
```

### Hudi Write Flow

```
DataFrame
    │
    ▼
Hudi Write Options
    ├─ Table path
    ├─ Record key field
    ├─ Precombine field
    ├─ Partition fields
    └─ Operation type (UPSERT/INSERT)
    │
    ▼
Spark Hudi Write
    ├─ Data processing
    ├─ Parquet file generation
    ├─ Metadata updates
    └─ Commit creation
    │
    ▼
Hudi Table (Parquet + Metadata)
```

---

## ETL Pipeline

### Standard Pipeline

1. **Extract**
   - Connect to MongoDB
   - Apply query filter
   - Read documents
   - Convert to DataFrame

2. **Transform**
   - Flatten nested structures
   - Clean data
   - Generate/load schema
   - Apply schema validation
   - Type conversion

3. **Load**
   - Create/update Hudi table
   - Write data (UPSERT)
   - Commit transaction
   - Update table statistics

### Streaming Pipeline

1. **Continuous Read**
   - Monitor MongoDB change streams
   - Capture inserts/updates
   - Buffer records

2. **Micro-batch Processing**
   - Process buffered records
   - Transform in batches
   - Write to Hudi incrementally

3. **Checkpoint Management**
   - Track processed offsets
   - Resume from checkpoint on failure
   - Maintain exactly-once semantics

---

## Workflow Examples

### Example 1: Simple ETL Job

**Goal:** Extract products from MongoDB and load to Hudi

```bash
# 1. Store credentials
POST /mongo/credentials
{
  "user_id": "prod_sync",
  "mongo_uri": "mongodb://...",
  "database": "ecommerce",
  "collection": "products"
}

# 2. Generate schema
POST /schema/generate
{
  "user_id": "prod_sync",
  "sample_size": 1000
}

# 3. Transform and write (manual)
POST /mongo/transform
{
  "user_id": "prod_sync",
  "apply_schema": true
}

# Or create scheduled job
POST /jobs/batch/create
{
  "job_name": "product_sync",
  "mongo_credentials": {"user_id": "prod_sync"},
  "hudi_config": {"table_name": "products"},
  "schedule": {"type": "cron", "expression": "0 */6 * * *"}
}
```

### Example 2: Query Hudi Table via Trino

**Goal:** Query transformed data using SQL

```bash
# 1. Ensure Trino is running
GET /trino/health

# 2. List tables
GET /trino/tables?catalog=hive&schema=default

# 3. Query data
POST /trino/query
{
  "sql": "SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM hive.default.products GROUP BY category",
  "max_rows": 50
}
```

### Example 3: Complex Transformation

**Goal:** Transform nested order data with schema validation

```bash
# 1. Read nested data
POST /mongo/read
{
  "database": "ecommerce",
  "collection": "orders",
  "query": {"status": "completed"}
}

# 2. Generate schema from nested structure
POST /schema/generate
{
  "database": "ecommerce",
  "collection": "orders",
  "sample_size": 500
}

# 3. Transform with flattening
POST /mongo/transform
{
  "database": "ecommerce",
  "collection": "orders",
  "flatten_data": true,
  "apply_schema": true,
  "strict_schema": false
}

# Result: Nested structures like:
#   order.items[0].product_id → order_items_0_product_id
#   order.shipping.address.city → order_shipping_address_city
```

---

## Data Storage Architecture

### Hudi Table Structure

```
/hudi/base/path/
  ├─ database/
  │   ├─ table_name/
  │   │   ├─ .hoodie/
  │   │   │   ├─ commits/
  │   │   │   ├─ metadata/
  │   │   │   └─ timeline/
  │   │   ├─ partition1/
  │   │   │   ├─ file1.parquet
  │   │   │   └─ file2.parquet
  │   │   └─ partition2/
  │   │       └─ file3.parquet
```

### Metadata Storage

**PostgreSQL Tables:**
- `mongodb_credentials` - Stored MongoDB connections
- `jobs` - Job configurations
- `job_executions` - Execution history
- `users` - Authentication data

---

## Performance Considerations

### PyMongo vs PySpark

- **PyMongo**: Fast for small datasets (< 1GB), single-threaded
- **PySpark**: Required for large datasets, distributed processing

### Schema Validation

- **Strict mode**: Fails on first error, fastest
- **Lenient mode**: Collects all errors, slower but more informative

### Hudi Write Operations

- **UPSERT**: Updates existing records, slower
- **INSERT**: Appends only, faster
- **Partitioning**: Improves query performance

---

## Error Handling

### Pipeline Error Recovery

1. **Connection Errors**: Retry with exponential backoff
2. **Validation Errors**: Collect and report (lenient mode)
3. **Write Errors**: Rollback transaction, retry
4. **Job Failures**: Log to database, notify user

### Error Propagation

```
Module Error
    │
    ▼
Exception Handling
    ├─ Log error details
    ├─ Update job status
    └─ Return error response
    │
    ▼
API Error Response (JSON)
```

---

## Security

### Authentication Flow

```
Request
    │
    ▼
JWT Token Validation
    ├─ Signature verification
    ├─ Expiration check
    └─ User lookup
    │
    ▼
Authorized Request
```

### Data Security

- Credentials encrypted in PostgreSQL
- MongoDB connections use authentication
- Trino connections require authentication
- API endpoints require JWT tokens

---

## Scalability

### Horizontal Scaling

- API servers can be load balanced
- Trino supports multiple workers
- Spark can scale to cluster

### Vertical Scaling

- Increase Spark executor memory
- Tune Hudi write parallelism
- Optimize PostgreSQL connection pool

---

This architecture supports both batch and streaming ETL operations with comprehensive schema management, job scheduling, and SQL query capabilities.

