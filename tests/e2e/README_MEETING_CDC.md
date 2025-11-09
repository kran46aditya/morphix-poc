# Meeting CDC End-to-End Test

Complete end-to-end test for the Meeting CDC pipeline using the provided meeting data sample.

## Overview

This test suite validates the entire platform flow:
1. User registration and login via API
2. MongoDB credentials storage via API
3. Stream job creation via API
4. Continuous data generation (inserts/updates/deletes)
5. CDC processing
6. Data querying via Trino API

## Prerequisites

1. **API Server Running**
   ```bash
   uvicorn src.api.mongo_api:app --reload --host 0.0.0.0 --port 8000
   ```

2. **MongoDB Running**
   - Local MongoDB accessible at `localhost:27017`
   - Database: `test_db`
   - Collection: `meetings`

3. **PostgreSQL Running** (for CDC checkpoints)
   - Accessible at `localhost:5432`
   - Database: `morphix`

4. **Trino Server** (optional, for query testing)
   - Accessible at configured host/port

## Running the Tests

### Option 1: Run Complete E2E Script

```bash
# Make sure API server is running first
python scripts/run_e2e_meeting_test.py
```

This script will:
- Register/login user
- Store MongoDB credentials
- Create stream job
- Generate data changes
- Query via Trino

### Option 2: Run Pytest Tests

```bash
pytest tests/e2e/test_meeting_cdc_e2e.py -v -s
```

### Option 3: Run Data Generator Only

```bash
# Generate data for 60 seconds at 2 ops/sec
python scripts/cdc_data_generator.py \
  --mongo-uri "mongodb://localhost:27017/test_db" \
  --database test_db \
  --collection meetings \
  --duration 60 \
  --ops-per-sec 2.0 \
  --insert-weight 0.5 \
  --update-weight 0.4 \
  --delete-weight 0.1
```

## Data Generator Options

```bash
python scripts/cdc_data_generator.py --help
```

Options:
- `--mongo-uri`: MongoDB connection URI
- `--database`: Database name (default: `test_db`)
- `--collection`: Collection name (default: `meetings`)
- `--ops-per-sec`: Operations per second (default: 1.0)
- `--duration`: Duration in seconds (0 = infinite, default: 300)
- `--insert-weight`: Weight for insert operations (0-1, default: 0.5)
- `--update-weight`: Weight for update operations (0-1, default: 0.4)
- `--delete-weight`: Weight for delete operations (0-1, default: 0.1)

## Test Data Sample

The generator uses the meeting data template with:
- Meeting IDs, dates, locations
- Attendees, costs, workflow status
- Random variations for realistic testing

## Expected Output

The E2E test will show:
- ✅ User registration and login
- ✅ MongoDB credentials stored
- ✅ Stream job created
- ✅ Data generation statistics
- ✅ Trino query results (if available)

## Troubleshooting

### API Not Available
- Ensure API server is running: `uvicorn src.api.mongo_api:app --reload`
- Check API_BASE_URL environment variable

### MongoDB Connection Failed
- Verify MongoDB is running: `mongosh --eval "db.adminCommand('ping')"`
- Check MONGO_URI environment variable

### CDC Not Processing
- Verify MongoDB replica set is enabled (required for changestreams)
- Check PostgreSQL is accessible for checkpoint storage
- Review stream job status via API

### Trino Queries Fail
- Verify Trino server is running
- Check Trino configuration in settings
- Ensure Hudi tables are accessible via Trino

## Environment Variables

```bash
export API_BASE_URL="http://localhost:8000"
export MONGO_URI="mongodb://localhost:27017/test_db"
export MONGO_DATABASE="test_db"
export MONGO_COLLECTION="meetings"
export DATABASE_URL="postgresql://user:pass@localhost:5432/morphix"
```

