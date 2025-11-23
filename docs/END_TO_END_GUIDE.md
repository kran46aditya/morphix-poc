# Morphix Platform - End-to-End Usage Guide

This guide walks you through using the Morphix platform from data ingestion to transformation, quality assurance, and export.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Setup](#setup)
3. [Basic Workflow](#basic-workflow)
4. [Schema Management](#schema-management)
5. [Job Management](#job-management)
6. [Quality Assurance](#quality-assurance)
7. [AI-Powered Repairs](#ai-powered-repairs)
8. [Transform Plans](#transform-plans)
9. [Export and Reporting](#export-and-reporting)
10. [Advanced Features](#advanced-features)

## Prerequisites

- Python 3.8+
- MongoDB instance (local or remote)
- PostgreSQL (for job metadata)
- Docker and Docker Compose (for full stack)

## Setup

### 1. Install Dependencies

```bash
# Install core dependencies
pip install -r requirements.txt

# Optional: Install Great Expectations for quality checks
pip install great-expectations

# Optional: Install ClickHouse connector
pip install clickhouse-connect
```

### 2. Configure Environment

Create a `.env` file:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=morphix
DB_PASSWORD=your_password
DB_DATABASE=morphix

# MongoDB
MONGO_DEFAULT_HOST=localhost
MONGO_DEFAULT_PORT=27017
MONGO_DEFAULT_DATABASE=testdb

# Metadata
METADATA_BASE=/metadata

# AI Mode (local = rule-based, remote = external API)
AIMODE=local

# Apache Atlas (optional)
ATLAS_URL=http://localhost:21000
ATLAS_USERNAME=admin
ATLAS_PASSWORD=admin

# ClickHouse (optional)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=default
```

### 3. Start Services

```bash
# Start with Docker Compose
docker-compose up -d

# Or start individual services
# MongoDB, PostgreSQL, etc.
```

## Basic Workflow

### Step 1: Read Data from MongoDB

```python
from src.etl.mongo_api_reader import MongoDataReader
from src.utils.logging import CorrelationContext, set_correlation_id

# Set correlation ID for tracing
with CorrelationContext():
    # Create reader
    reader = MongoDataReader(
        mongo_uri="mongodb://localhost:27017",
        database="testdb",
        collection="users"
    )
    
    # Read data
    df = reader.read_to_pandas(limit=1000)
    print(f"Read {len(df)} records")
    print(f"Columns: {df.columns.tolist()}")
```

**What happens:**
- Schema fingerprint is computed and logged
- Count, latency, and error events are emitted
- All logs include correlation ID for tracing

### Step 2: Generate Schema

```python
from src.etl.schema_generator import SchemaGenerator

# Generate schema from DataFrame
schema_gen = SchemaGenerator()
schema = schema_gen.generate_from_dataframe(
    df,
    collection="users",
    save_to_metadata=True  # Saves to /metadata/schemas/users/<timestamp>.json
)

print(f"Schema has {len(schema)} fields")
```

**What happens:**
- Schema is inferred from DataFrame
- Saved to `/metadata/schemas/users/<timestamp>.json`
- Includes field types, nullability, constraints

### Step 3: Transform Data

```python
from src.etl.data_transformer import DataTransformer

# Create transformer with schema
transformer = DataTransformer(schema=schema)

# Transform data
transformed_df = transformer.transform(
    df,
    flatten=True,      # Flatten nested structures
    clean=True,        # Clean data
    apply_schema=True  # Apply schema validation
)

print(f"Transformed: {len(transformed_df)} rows, {len(transformed_df.columns)} columns")
```

## Schema Management

### Compare Schemas

```python
from src.etl.schema_generator import SchemaGenerator

# Load previous schema
old_schema = {...}  # Load from /metadata/schemas/users/<old_timestamp>.json

# Generate new schema
new_schema = schema_gen.generate_from_dataframe(df, collection="users")

# Compute differences
diffs = schema_gen.diff_schemas(old_schema, new_schema)

for diff in diffs:
    print(f"{diff['action']}: {diff['field']} ({diff.get('old_type')} -> {diff.get('new_type')})")
```

### Detect Breaking Changes

```python
breaking_changes = schema_gen.detect_breaking_changes(old_schema, new_schema)

if breaking_changes["has_breaking_changes"]:
    print("⚠️ Breaking changes detected:")
    for change in breaking_changes["breaking_changes"]:
        print(f"  - {change['message']}")
else:
    print("✅ No breaking changes")
```

## Job Management

### Create a Batch Job

```python
from src.jobs.job_manager import JobManager
from src.jobs.models import BatchJobConfig, JobSchedule, JobTrigger

# Create job configuration
job_config = BatchJobConfig(
    job_id="job_users_etl",
    job_name="Users ETL Job",
    job_type=JobType.BATCH,
    user_id=1,
    mongo_uri="mongodb://localhost:27017",
    database="testdb",
    collection="users",
    hudi_table_name="users_table",
    hudi_base_path="/data/hudi/users",
    schedule=JobSchedule(
        trigger=JobTrigger.SCHEDULED,
        cron_expression="0 2 * * *"  # Daily at 2 AM
    ),
    created_by="admin"
)

# Create job
with JobManager() as manager:
    job_id = manager.create_job(job_config)
    print(f"Created job: {job_id}")
```

### Run Job with Backfill

```python
# Full backfill (scans all data)
execution_id = manager.run(job_id, backfill=True)

# Incremental mode (uses timestamp or change token)
execution_id = manager.run(
    job_id,
    backfill=False,
    timestamp_field="updated_at",
    change_token={"resume_token": "..."}
)
```

### Monitor Job Execution

```python
# Get job executions
executions = manager.get_job_executions(job_id, limit=10)

for execution in executions:
    print(f"Execution {execution.execution_id}: {execution.status}")
    print(f"  Started: {execution.started_at}")
    print(f"  Completed: {execution.completed_at}")
    if execution.result:
        print(f"  Records processed: {execution.result.records_processed}")

# Get job metrics
metrics = manager.get_job_metrics(job_id, days=30)
print(f"Success rate: {100 - metrics.error_rate}%")
print(f"Avg duration: {metrics.average_duration_seconds}s")
```

## Quality Assurance

### Generate Great Expectations Suite

```python
from src.quality.gx_builder import generate_suite

# Generate baseline suite from sample data
suite = generate_suite(
    sample_df=df,
    suite_name="users_quality_suite"
)

print(f"Generated suite with {len(suite['expectations'])} expectations")
```

### Run Quality Checks

```python
from src.quality.gx_runner import run_suite

# Run suite against data
result = run_suite(
    suite=suite,
    df=df,
    collection="users",
    save_results=True  # Saves to /metadata/quality/users/<timestamp>.json
)

if result["passed"]:
    print("✅ All quality checks passed")
else:
    print(f"❌ {len(result['failed_expectations'])} expectations failed")
    for failed in result["failed_expectations"]:
        print(f"  - {failed['expectation_type']}: {failed.get('error', 'N/A')}")
```

### Quality Checks in Job Pipeline

Quality checks are automatically run in `job_manager.before_run()`:

```python
# When starting a job, validation happens automatically
execution_id = manager.start_job(job_id)

# If validation fails, job state becomes VALIDATION_FAILED
# Check job run status
job_run_file = f"/metadata/job_runs/{job_id}/{execution_id}.json"
# Read to see validation results
```

## AI-Powered Repairs

### Generate Repair Suggestions

```python
from src.ai.repair_suggester import RepairSuggester
from src.etl.schema_generator import SchemaGenerator

# Get schema diff
old_schema = {...}  # Load previous schema
new_schema = schema_gen.generate_from_dataframe(df, collection="users")
diffs = schema_gen.diff_schemas(old_schema, new_schema)

# Generate repair suggestions
suggester = RepairSuggester(ai_mode="local")  # or "remote" for external AI
suggestions = suggester.suggest_repairs(diffs, sample_docs=df.to_dict('records'))

for suggestion in suggestions:
    print(f"Field: {suggestion['field']}")
    print(f"  Suggestion: {suggestion['suggestion']}")
    print(f"  Rationale: {suggestion['rationale']}")
```

### Test Repair in Sandbox

```python
from src.ai.sandbox_runner import SandboxRunner

sandbox = SandboxRunner()

# Apply suggestion to sample
transformed_df = sandbox.apply_suggestion_to_sample(
    suggestion=suggestions[0],
    sample_docs=df.to_dict('records')
)

# Validate with GX
gx_result = sandbox.run_gx_suite_on_transformed(
    transformed_df=transformed_df,
    collection="users"
)

if gx_result["passed"]:
    print("✅ Repair suggestion validated successfully")
```

### Record and Approve Repair

```python
from src.metadata.audit import AuditTrail

audit = AuditTrail()

# Record suggestion
audit_id = audit.record_suggestion(
    job_id="job_users_etl",
    suggestion=suggestions[0],
    gx_report=gx_result,
    approved_by=None  # Not yet approved
)

# Approve repair (requires manual approval)
audit.record_approval(
    audit_id=audit_id,
    approved_by="admin@example.com"
)

# Verify audit record integrity
verification = audit.verify_record(audit_id)
if verification["valid"]:
    print("✅ Audit record is valid (not tampered)")
```

### Apply Approved Repair Plan

```python
from src.etl.data_transformer import DataTransformer

# Create repair plan (must be approved)
repair_plan = {
    "plan_id": "plan_001",
    "approved": True,  # Must be True
    "approved_by": "admin@example.com",
    "operations": [
        {
            "type": "type_conversion",
            "field": "age",
            "target_type": "integer",
            "original_type": "string"
        }
    ]
}

# Apply repair plan
transformer = DataTransformer()
try:
    repaired_df = transformer.apply_repair_plan(repair_plan, df)
    print("✅ Repair plan applied successfully")
except ValueError as e:
    print(f"❌ {e}")  # Plan not approved
```

## Transform Plans

### Create Transform Plan

```bash
# Using CLI
morphix plan create users \
    --mongo-uri mongodb://localhost:27017 \
    --database testdb \
    --sample-size 1000
```

Or programmatically:

```python
from src.transform_plans.plan_manager import PlanManager
from src.etl.schema_generator import SchemaGenerator

plan_manager = PlanManager()

# Get schema hashes
input_schema = schema_gen.generate_from_dataframe(df)
input_hash = SchemaGenerator._compute_schema_hash(input_schema)

# Define output schema (after transformations)
output_schema = {...}  # Your target schema
output_hash = SchemaGenerator._compute_schema_hash(output_schema)

# Create operations list
operations = [
    {
        "type": "type_conversion",
        "field": "age",
        "target_type": "integer"
    },
    {
        "type": "add_field",
        "field": "full_name",
        "default_value": ""
    }
]

# Create plan
plan = plan_manager.create_plan(
    collection="users",
    input_schema_hash=input_hash,
    output_schema_hash=output_hash,
    operations_list=operations
)

print(f"Created plan: {plan['plan_id']} (version {plan['version']})")
print(f"Rollback plan: {len(plan['rollback_plan'])} operations")
```

### Test Transform Plan

```bash
# Test plan on sample data
morphix plan test users --sample-size 1000
```

Or programmatically:

```python
# Get plan
plan = plan_manager.get_plan("users", version=1)

# Test on sample
transformer = DataTransformer()
test_df = transformer.apply_repair_plan(plan, df)

print(f"Test: {len(df)} -> {len(test_df)} rows")
```

### Apply Transform Plan

```bash
# Approve and apply
morphix plan apply users --approve --approved-by admin@example.com
```

Or programmatically:

```python
# Approve plan
plan_manager.approve_plan(
    collection="users",
    version=1,
    approved_by="admin@example.com"
)

# Apply plan
transformer = DataTransformer()
transformed_df = transformer.apply_repair_plan(plan, df)

# Mark as applied
plan_manager.mark_plan_applied("users", version=1)
```

### Rollback Transform Plan

```bash
# Rollback to previous version
morphix plan rollback users 1
```

Or programmatically:

```python
# Get plan with rollback operations
plan = plan_manager.get_plan("users", version=1)
rollback_plan = plan["rollback_plan"]

# Apply rollback
rollback_plan_dict = {
    "plan_id": f"rollback_{plan['plan_id']}",
    "approved": True,
    "approved_by": "admin@example.com",
    "operations": rollback_plan
}

transformer = DataTransformer()
rolled_back_df = transformer.apply_repair_plan(rollback_plan_dict, transformed_df)
```

## Export and Reporting

### Export All Metadata

```python
from src.metadata.export import PilotExporter

exporter = PilotExporter()

# Export everything
summary = exporter.export_all(
    output_dir=Path("/exports/morphix_export"),
    collection="users"  # Optional: filter by collection
)

print("Exported:")
print(f"  - Lineage: {summary['exports'].get('lineage')}")
print(f"  - GX Summaries: {summary['exports'].get('gx_summaries')}")
print(f"  - Audit Traces: {summary['exports'].get('audit_traces')}")
print(f"  - Applied Plans: {summary['exports'].get('applied_plans')}")
print(f"  - Rollback Plans: {summary['exports'].get('rollback_plans')}")
```

## Advanced Features

### Policy Enforcement

```python
from src.policy.policy_engine import PolicyEngine

policy = PolicyEngine()

# Check if operation is allowed
result = policy.enforce_policy(
    operation="write",
    fields=["name", "email", "ssn"],  # ssn is blocked
    collection="users",
    approved=False
)

if not result["allowed"]:
    print("❌ Operation blocked:")
    for violation in result["violations"]:
        print(f"  - {violation['type']}: {violation['details']}")
```

### Apache Atlas Lineage

```python
from src.lineage.atlas_client import AtlasClient

atlas = AtlasClient()

# Push dataset
atlas.push_dataset(
    entity_name="users_collection",
    schema_url="/metadata/schemas/users/latest.json"
)

# Push lineage
atlas.push_lineage(
    source="mongodb://testdb/users",
    transform="etl_transform_users",
    target="hudi://default/users_table"
)
```

### ClickHouse Writer

```python
from src.connectors.clickhouse_writer import ClickHouseWriter

writer = ClickHouseWriter()

# Validate schema
validation = writer.validate_target_schema("users_table", df)
if not validation["valid"]:
    print(f"Schema issues: {validation['type_issues']}")

# Write data
result = writer.write_batch(
    table_name="users_table",
    df=df,
    validate_schema=True,
    batch_size=10000
)

print(f"Wrote {result['rows_written']} rows in {result['batches']} batches")
```

## Complete End-to-End Example

```python
"""
Complete ETL pipeline with quality checks and repairs
"""

from src.etl.mongo_api_reader import MongoDataReader
from src.etl.schema_generator import SchemaGenerator
from src.etl.data_transformer import DataTransformer
from src.quality.gx_builder import generate_suite
from src.quality.gx_runner import run_suite
from src.jobs.job_manager import JobManager
from src.utils.logging import CorrelationContext

# 1. Set correlation ID
with CorrelationContext() as correlation_id:
    print(f"Correlation ID: {correlation_id}")
    
    # 2. Read data
    reader = MongoDataReader(
        mongo_uri="mongodb://localhost:27017",
        database="testdb",
        collection="users"
    )
    df = reader.read_to_pandas(limit=10000)
    
    # 3. Generate and save schema
    schema_gen = SchemaGenerator()
    schema = schema_gen.generate_from_dataframe(
        df,
        collection="users",
        save_to_metadata=True
    )
    
    # 4. Generate quality suite
    suite = generate_suite(df, suite_name="users_suite")
    
    # 5. Run quality checks
    quality_result = run_suite(
        suite,
        df,
        collection="users",
        save_results=True
    )
    
    if not quality_result["passed"]:
        print("⚠️ Quality checks failed - generating repair suggestions")
        # ... repair workflow ...
    
    # 6. Transform data
    transformer = DataTransformer(schema=schema)
    transformed_df = transformer.transform(df)
    
    # 7. Create and run job
    # ... job creation and execution ...
    
    print("✅ Pipeline completed successfully")
```

## Troubleshooting

### Common Issues

1. **Metadata directory not found**
   - Set `METADATA_BASE` environment variable
   - Ensure directory exists and is writable

2. **Great Expectations not available**
   - Install: `pip install great-expectations`
   - Or set `AIMODE=local` to use rule-based repairs only

3. **Job validation fails**
   - Check GX results in `/metadata/quality/<collection>/`
   - Review job run logs in `/metadata/job_runs/<job_id>/`

4. **Repair plan not approved**
   - Ensure `plan["approved"] = True`
   - Use `plan_manager.approve_plan()` before applying

## Next Steps

- Review generated schemas in `/metadata/schemas/`
- Check quality reports in `/metadata/quality/`
- Monitor job executions via `JobManager.get_job_executions()`
- Export metadata for reporting and analysis

For more details, see:
- [API Documentation](API.md)
- [Component Details](COMPONENTS.md)
- [Docker Setup](DOCKER_SETUP.md)

