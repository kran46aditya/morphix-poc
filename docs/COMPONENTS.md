# Morphix ETL Platform - Component Documentation

Complete documentation for all major components of the Morphix ETL Platform.

## Table of Contents

1. [Core Components](#core-components)
   - [Volume Router](#volume-router)
   - [ETL Pipeline](#etl-pipeline)
   - [Schema Generator](#schema-generator)
   - [Data Transformer](#data-transformer)

2. [Data Processing](#data-processing)
   - [MongoDB Reader](#mongodb-reader)
   - [Hudi Writer](#hudi-writer)
   - [Iceberg Writer](#iceberg-writer)
   - [Dual Destination Writer](#dual-destination-writer)

3. [Quality & Monitoring](#quality--monitoring)
   - [Quality Rules Engine](#quality-rules-engine)
   - [Cost Tracker](#cost-tracker)

4. [Job Management](#job-management)
   - [Job Manager](#job-manager)
   - [Job Scheduler](#job-scheduler)
   - [Batch Job Processor](#batch-job-processor)
   - [Stream Job Processor](#stream-job-processor)

5. [Embeddings](#embeddings)
   - [Local Embedder](#local-embedder)
   - [Embedding Cache](#embedding-cache)

6. [API Layer](#api-layer)
   - [MongoDB API](#mongodb-api)
   - [Trino API](#trino-api)
   - [Authentication](#authentication)

---

## Core Components

### Volume Router

**Location:** `src/core/volume_router.py`

**Purpose:** Automatically routes data to the optimal storage sink (Hudi or Iceberg) based on estimated daily volume.

**Key Features:**
- Volume-based routing (threshold: 10M records/day)
- Manual override support
- Lazy writer instantiation

**Usage:**
```python
from src.core.volume_router import VolumeRouter

router = VolumeRouter(threshold=10_000_000)
sink_type = router.determine_sink(job_config)
writer = router.get_writer_instance(sink_type)
```

**Routing Logic:**
- **High Volume (>10M/day)** → Hudi (optimized for large-scale writes)
- **Low Volume (<10M/day)** → Iceberg (optimized for analytics)
- **Manual Override** → Respects `force_sink_type` in job config

**Configuration:**
- `VOLUME_THRESHOLD`: Default threshold (10M records/day)
- Custom threshold can be set during initialization

---

### ETL Pipeline

**Location:** `src/etl/pipeline.py`

**Purpose:** Orchestrates the complete ETL process from MongoDB to data lake.

**Components:**
1. **Extract**: Read data from MongoDB
2. **Transform**: Apply schema, validate, clean data
3. **Load**: Write to Hudi or Iceberg tables

**Usage:**
```python
from src.etl import ETLPipeline, create_pipeline_from_credentials

pipeline = create_pipeline_from_credentials(
    user_id="alice",
    mongo_uri="mongodb://...",
    database="mydb",
    collection="mycollection"
)

result = pipeline.run(
    query={},
    limit=1000,
    hudi_config=HudiWriteConfig(...)
)
```

**Features:**
- Automatic schema inference
- Data validation and cleaning
- Error handling and reporting
- Support for both batch and streaming

---

### Schema Generator

**Location:** `src/etl/schema_generator.py`

**Purpose:** Generates and manages data schemas for validation and transformation.

**Key Features:**
- **Auto-inference**: Analyzes sample data to infer types
- **MongoDB Integration**: Direct schema inference from MongoDB collections
- **Flattening Suggestions**: Recommends strategies for nested structures
- **Breaking Change Detection**: Identifies incompatible schema changes
- **Quality Issue Detection**: Flags data quality problems

**Usage:**
```python
from src.etl.schema_generator import SchemaGenerator

# From DataFrame
schema = SchemaGenerator.generate_from_dataframe(df)

# From MongoDB
result = SchemaGenerator.infer_from_mongodb(
    mongo_uri="mongodb://...",
    database="mydb",
    collection="mycollection",
    sample_size=1000
)

# Flattening suggestions
suggestions = SchemaGenerator.suggest_flattening_strategy(schema)

# Breaking changes
changes = SchemaGenerator.detect_breaking_changes(old_schema, new_schema)
```

**Schema Format:**
```json
{
  "field_name": {
    "type": "integer|string|float|boolean|datetime",
    "nullable": true|false,
    "min_value": 0,
    "max_value": 100,
    "pattern": "^regex$"
  }
}
```

---

### Data Transformer

**Location:** `src/etl/data_transformer.py`

**Purpose:** Transforms and validates data according to schemas.

**Key Features:**
- Schema validation
- Type conversion
- Nested structure flattening
- Constraint checking
- Error collection

**Usage:**
```python
from src.etl.data_transformer import DataTransformer

transformer = DataTransformer(schema=my_schema)
transformed_df = transformer.transform(df, flatten=True)
```

**Transformation Options:**
- `flatten`: Flatten nested objects/arrays
- `strict`: Fail on validation errors
- `defaults`: Apply default values for missing fields

---

## Data Processing

### MongoDB Reader

**Location:** `src/etl/mongo_api_reader.py`, `src/mongodb/connection.py`

**Purpose:** Extracts data from MongoDB into pandas or Spark DataFrames.

**Key Features:**
- PyMongo support (small datasets)
- PySpark support (large datasets)
- Query filtering and projection
- Connection pooling
- Credential management

**Usage:**
```python
from src.etl.mongo_api_reader import MongoDataReader

reader = MongoDataReader(
    mongo_uri="mongodb://...",
    database="mydb",
    collection="mycollection"
)

df = reader.read(query={"status": "active"}, limit=1000)
```

**Connection Methods:**
1. Direct URI
2. Stored credentials (via API)
3. Connection info object

---

### Hudi Writer

**Location:** `src/hudi_writer/writer.py`

**Purpose:** Writes data to Apache Hudi tables (optimized for high-volume writes).

**Key Features:**
- Upsert operations
- Partition management
- Schema evolution
- ACID transactions
- Time travel queries

**Usage:**
```python
from src.hudi_writer import HudiWriter, HudiWriteConfig, HudiTableConfig

writer = HudiWriter()
config = HudiWriteConfig(
    table_name="my_table",
    base_path="s3://bucket/hudi/",
    record_key="id",
    partition_path="date"
)

result = writer.write_dataframe(df, config)
```

**Configuration:**
- `record_key`: Primary key for upserts
- `partition_path`: Partitioning strategy
- `precombine_key`: Conflict resolution field
- `table_type`: COPY_ON_WRITE or MERGE_ON_READ

---

### Iceberg Writer

**Location:** `src/lake/iceberg_writer.py`

**Purpose:** Writes data to Apache Iceberg tables (optimized for analytics).

**Key Features:**
- Schema evolution
- Partition evolution
- Hidden partitioning
- Time travel queries
- ACID transactions

**Usage:**
```python
from src.lake.iceberg_writer import IcebergWriter

writer = IcebergWriter()
result = writer.write_dataframe(
    df=df,
    table_name="my_table",
    database="analytics"
)
```

**Advantages:**
- Better for analytical queries
- Efficient for low-volume, high-complexity data
- Advanced partitioning strategies

---

### Dual Destination Writer

**Location:** `src/destinations/dual_writer.py`

**Purpose:** Writes data simultaneously to both Vector DB and Data Warehouse.

**Key Features:**
- Parallel writes
- Embedding generation
- Error isolation
- Progress tracking

**Usage:**
```python
from src.destinations.dual_writer import DualDestinationWriter

writer = DualDestinationWriter()
result = writer.write_dual(
    df=df,
    job_config=job_config,
    text_column="description"
)
```

**Workflow:**
1. Generate embeddings for text columns
2. Write to Vector DB (with embeddings)
3. Write to Warehouse (Hudi/Iceberg) in parallel
4. Return combined results

---

## Quality & Monitoring

### Quality Rules Engine

**Location:** `src/quality/rules_engine.py`

**Purpose:** Validates data quality using configurable rules.

**Rule Types:**
1. **NULL_THRESHOLD**: Maximum percentage of null values
2. **TYPE_CHECK**: Validate data types
3. **RANGE_CHECK**: Min/max value validation
4. **PATTERN_MATCH**: Regex pattern validation
5. **UNIQUENESS**: Ensure unique values
6. **FRESHNESS**: Data age validation

**Usage:**
```python
from src.quality.rules_engine import QualityRulesEngine, QualityRule, RuleType

engine = QualityRulesEngine()

rules = [
    QualityRule(
        rule_id="email_format",
        rule_type=RuleType.PATTERN_MATCH,
        column="email",
        parameters={"pattern": r'^[^@]+@[^@]+\.[^@]+$'},
        severity="error"
    )
]

results = engine.apply_rules(df, rules)
score = engine.calculate_quality_score(results)
report = engine.generate_report(results)
```

**Severity Levels:**
- `error`: Critical issues that block processing
- `warning`: Issues that should be reviewed
- `info`: Informational quality metrics

---

### Cost Tracker

**Location:** `src/monitoring/cost_tracker.py`

**Purpose:** Tracks and estimates infrastructure costs.

**Cost Components:**
- Embedding compute (CPU/GPU hours)
- Storage (GB/month)
- Vector DB storage
- Warehouse storage
- Cache savings

**Usage:**
```python
from src.monitoring.cost_tracker import CostTracker

tracker = CostTracker()

# Execution cost
cost = tracker.calculate_execution_cost(
    job_execution=job_exec,
    embeddings_generated=10000,
    cache_hits=7000,
    storage_gb=5.0
)

# Monthly estimate
estimate = tracker.estimate_monthly_cost(job_config)
```

**Pricing Constants:**
- `CPU_HOUR_COST`: $0.05/hour
- `GPU_HOUR_COST`: $0.50/hour
- `STORAGE_GB_MONTH`: $0.023/GB
- `VECTOR_DB_GB_MONTH`: $0.50/GB

---

## Job Management

### Job Manager

**Location:** `src/jobs/job_manager.py`

**Purpose:** Manages job configurations and execution history.

**Key Features:**
- CRUD operations for jobs
- Job status tracking
- Execution history
- PostgreSQL storage

**Usage:**
```python
from src.jobs import JobManager
from src.jobs.models import BatchJobConfig

manager = JobManager()

# Create job
job = manager.create_job(batch_config)

# Get job
job = manager.get_job(job_id)

# Update job
manager.update_job(job_id, updated_config)

# Delete job
manager.delete_job(job_id)
```

---

### Job Scheduler

**Location:** `src/jobs/scheduler.py`

**Purpose:** Schedules and executes jobs based on triggers.

**Trigger Types:**
- `MANUAL`: Execute on demand
- `INTERVAL`: Execute at fixed intervals
- `CRON`: Execute on schedule

**Usage:**
```python
from src.jobs import JobScheduler

scheduler = JobScheduler()
scheduler.start()

# Jobs are automatically executed based on their schedule
```

**Features:**
- Background thread execution
- Concurrent job execution
- Error handling and retries
- Status updates

---

### Batch Job Processor

**Location:** `src/jobs/batch_jobs.py`

**Purpose:** Processes batch ETL jobs.

**Workflow:**
1. Read data from MongoDB
2. Apply volume routing
3. Generate embeddings (if needed)
4. Apply quality rules
5. Write to warehouse (Hudi/Iceberg)
6. Track costs

**Usage:**
```python
from src.jobs.batch_jobs import BatchJobProcessor
from src.jobs.models import BatchJobConfig

processor = BatchJobProcessor()
result = processor.process_batch_job(job_config)
```

**Integration:**
- Volume Router (sink selection)
- Quality Rules Engine
- Cost Tracker
- Dual Destination Writer

---

### Stream Job Processor

**Location:** `src/jobs/stream_jobs.py`

**Purpose:** Processes streaming ETL jobs using MongoDB change streams.

**Key Features:**
- Real-time change detection
- Resume token management
- Batch processing
- Error recovery

**Usage:**
```python
from src.jobs.stream_jobs import StreamJobProcessor
from src.jobs.models import StreamJobConfig

processor = StreamJobProcessor()
result = processor.process_stream_job(job_config)
```

**Change Stream Operations:**
- `insert`: New documents
- `update`: Modified documents
- `delete`: Removed documents
- `replace`: Replaced documents

**Resume Tokens:**
- Stored in PostgreSQL
- Enables crash recovery
- Maintains stream position

---

## Embeddings

### Local Embedder

**Location:** `src/embeddings/local_embedder.py`

**Purpose:** Generates embeddings locally using sentence-transformers.

**Key Features:**
- Zero API costs
- Redis caching
- Batch processing
- Model management

**Usage:**
```python
from src.embeddings.local_embedder import LocalEmbedder

embedder = LocalEmbedder(model_name="all-MiniLM-L6-v2")
embeddings = embedder.embed_batch(["text1", "text2", "text3"])
```

**Models:**
- Default: `all-MiniLM-L6-v2` (384 dimensions)
- Custom models supported
- Automatic model download

---

### Embedding Cache

**Location:** `src/embeddings/embedding_cache.py`

**Purpose:** Manages Redis cache for embeddings.

**Key Features:**
- Cache hit rate tracking
- TTL management
- Size monitoring
- Cleanup utilities

**Usage:**
```python
from src.embeddings.embedding_cache import EmbeddingCache

cache = EmbeddingCache()
hit_rate = cache.get_cache_hit_rate()
size_mb = cache.get_cache_size_mb()
cleaned = cache.cleanup_old_entries(days=30)
```

**Benefits:**
- Reduces compute costs
- Faster processing
- Deduplication

---

## API Layer

### MongoDB API

**Location:** `src/api/mongo_api.py`

**Endpoints:**
- `POST /credentials` - Store MongoDB credentials
- `GET /credentials` - Retrieve credentials
- `GET /test-connection` - Test MongoDB connection
- `GET /read` - Read data from MongoDB
- `POST /schema/generate` - Generate schema
- `POST /transform` - Transform data

**Authentication:** Required (JWT token)

---

### Trino API

**Location:** `src/api/trino_api.py`

**Endpoints:**
- `GET /health` - Health check
- `GET /catalogs` - List catalogs
- `GET /schemas` - List schemas
- `GET /tables` - List tables
- `POST /query` - Execute SQL query
- `GET /describe/{table}` - Describe table schema

**Authentication:** Optional (configurable)

---

### Authentication

**Location:** `src/auth/endpoints.py`

**Endpoints:**
- `POST /register` - User registration
- `POST /login` - User login
- `GET /me` - Get current user
- `POST /refresh` - Refresh token

**Security:**
- JWT tokens
- Password hashing (bcrypt)
- Token expiration

---

## Component Interactions

### Typical ETL Flow

```
User Request
    ↓
API Layer (FastAPI)
    ↓
Job Manager (creates/retrieves job)
    ↓
Volume Router (determines sink)
    ↓
MongoDB Reader (extracts data)
    ↓
Schema Generator (infers/validates schema)
    ↓
Data Transformer (transforms data)
    ↓
Quality Rules Engine (validates quality)
    ↓
Dual Destination Writer
    ├─→ Local Embedder (generates embeddings)
    ├─→ Vector DB (writes with embeddings)
    └─→ Warehouse Writer (Hudi/Iceberg)
    ↓
Cost Tracker (tracks costs)
    ↓
Job Manager (updates status)
    ↓
Response to User
```

---

## Configuration

All components can be configured via `config/settings.py`:

- **Volume Settings**: Routing thresholds
- **Embedding Settings**: Model selection, cache TTL
- **Iceberg Settings**: Catalog configuration
- **Quality Settings**: Default rules, thresholds
- **Database Settings**: PostgreSQL, MongoDB connections

---

## Error Handling

All components follow consistent error handling:

1. **Validation Errors**: Return detailed error messages
2. **Connection Errors**: Retry with exponential backoff
3. **Processing Errors**: Log and continue or fail based on mode
4. **API Errors**: Return appropriate HTTP status codes

---

## Performance Considerations

- **Volume Router**: O(1) routing decision
- **Schema Generator**: O(n) where n is sample size
- **Quality Rules**: O(n*m) where n=rows, m=rules
- **Embeddings**: O(n) with caching reducing actual compute
- **Writers**: Optimized for batch writes

---

## Testing

All components have comprehensive unit tests:
- `tests/test_volume_router.py`
- `tests/test_quality_rules_engine.py`
- `tests/test_cost_tracker.py`
- And more...

Run tests with:
```bash
pytest tests/ -v
```

