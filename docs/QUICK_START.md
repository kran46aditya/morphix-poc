# Morphix Platform - Quick Start Guide

Get up and running with Morphix in 5 minutes.

## Prerequisites

```bash
# Python 3.8+
python --version

# MongoDB running (default: localhost:27017)
# PostgreSQL running (default: localhost:5432)
```

## Installation

```bash
# Clone repository
cd morphix-poc

# Install dependencies
pip install -r requirements.txt

# Optional: Quality checks
pip install great-expectations

# Optional: ClickHouse
pip install clickhouse-connect
```

## Quick Example

### 1. Read and Transform Data

```python
from src.etl.mongo_api_reader import MongoDataReader
from src.etl.schema_generator import SchemaGenerator
from src.etl.data_transformer import DataTransformer

# Read from MongoDB
reader = MongoDataReader(
    mongo_uri="mongodb://localhost:27017",
    database="testdb",
    collection="users"
)
df = reader.read_to_pandas(limit=1000)

# Generate schema
schema_gen = SchemaGenerator()
schema = schema_gen.generate_from_dataframe(df, collection="users", save_to_metadata=True)

# Transform data
transformer = DataTransformer(schema=schema)
transformed_df = transformer.transform(df)

print(f"✅ Transformed {len(transformed_df)} records")
```

### 2. Create and Run a Job

```python
from src.jobs.job_manager import JobManager
from src.jobs.models import BatchJobConfig, JobSchedule, JobTrigger, JobType

# Create job
job_config = BatchJobConfig(
    job_id="my_first_job",
    job_name="My First ETL Job",
    job_type=JobType.BATCH,
    user_id=1,
    mongo_uri="mongodb://localhost:27017",
    database="testdb",
    collection="users",
    hudi_table_name="users_table",
    hudi_base_path="/data/hudi/users",
    schedule=JobSchedule(trigger=JobTrigger.MANUAL),
    created_by="user@example.com"
)

# Create and run
with JobManager() as manager:
    job_id = manager.create_job(job_config)
    execution_id = manager.start_job(job_id)
    print(f"✅ Job started: {execution_id}")
```

### 3. Use CLI for Transform Plans

```bash
# Create a transform plan
morphix plan create users --sample-size 1000

# Test the plan
morphix plan test users

# Apply the plan
morphix plan apply users --approve
```

### 4. Quality Checks

```python
from src.quality.gx_builder import generate_suite
from src.quality.gx_runner import run_suite

# Generate quality suite
suite = generate_suite(df, suite_name="users_quality")

# Run checks
result = run_suite(suite, df, collection="users", save_results=True)

if result["passed"]:
    print("✅ All quality checks passed")
else:
    print(f"❌ {len(result['failed_expectations'])} checks failed")
```

## Environment Setup

Create `.env` file:

```bash
# Required
METADATA_BASE=/metadata

# Optional
AIMODE=local
ATLAS_URL=http://localhost:21000
CLICKHOUSE_HOST=localhost
```

## Common Commands

```bash
# View job status
python -c "from src.jobs.job_manager import JobManager; \
    m = JobManager(); \
    print(m.get_job('my_first_job'))"

# Export metadata
python -c "from src.metadata.export import PilotExporter; \
    e = PilotExporter(); \
    e.export_all('/tmp/export')"

# Check audit trail
python -c "from src.metadata.audit import AuditTrail; \
    a = AuditTrail(); \
    print(a.get_audit_records(limit=10))"
```

## Next Steps

- Read [END_TO_END_GUIDE.md](END_TO_END_GUIDE.md) for detailed workflows
- Check [API.md](API.md) for API documentation
- Review [COMPONENTS.md](COMPONENTS.md) for architecture details

## Troubleshooting

**Issue**: `ModuleNotFoundError: No module named 'src'`
- **Solution**: Run from project root or add to PYTHONPATH: `export PYTHONPATH=$PWD/src:$PYTHONPATH`

**Issue**: Metadata directory not found
- **Solution**: Create directory: `mkdir -p /metadata` or set `METADATA_BASE` env var

**Issue**: Great Expectations not available
- **Solution**: Install with `pip install great-expectations` or use `AIMODE=local`

For more help, see the full [END_TO_END_GUIDE.md](END_TO_END_GUIDE.md).

