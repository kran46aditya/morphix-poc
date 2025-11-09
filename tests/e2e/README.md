# End-to-End Tests

## Setup

### 1. MongoDB Replica Set (Required for CDC)

```bash
# Start MongoDB with replica set
docker run -d --name mongo-test \
  -p 27017:27017 \
  mongo:6.0 --replSet rs0

# Initialize replica set
docker exec mongo-test mongosh --eval "rs.initiate({
  _id: 'rs0',
  members: [{_id: 0, host: 'localhost:27017'}]
})"

# Create admin user
docker exec -it mongo-test mongosh admin --eval "
db.createUser({
  user: 'myUserAdmin',
  pwd: 'NewStrongPass!234',
  roles: [{role: 'root', db: 'admin'}]
})
"
```

### 2. Insert Test Data into Product Collection

```bash
docker exec -it mongo-test mongosh \
  -u myUserAdmin \
  -p 'NewStrongPass!234' \
  --authenticationDatabase admin \
  test_db --eval "
db.Product.insertMany([
  {
    name: 'Laptop Pro',
    price: 1299.99,
    category: 'Electronics',
    specs: {
      cpu: 'Intel i7',
      ram: '16GB',
      storage: '512GB SSD'
    },
    tags: ['computer', 'work', 'portable'],
    inStock: true,
    createdAt: new Date()
  },
  {
    name: 'Wireless Mouse',
    price: 29.99,
    category: 'Accessories',
    specs: {
      type: 'Optical',
      connectivity: 'Bluetooth',
      batteryLife: '6 months'
    },
    tags: ['peripheral', 'wireless'],
    inStock: true,
    createdAt: new Date()
  },
  {
    name: 'USB-C Hub',
    price: 49.99,
    category: 'Accessories',
    specs: {
      ports: ['HDMI', 'USB-A', 'USB-C', 'Ethernet'],
      powerDelivery: '100W'
    },
    tags: ['adapter', 'connectivity'],
    inStock: false,
    createdAt: new Date()
  }
])
"

# Verify data
docker exec -it mongo-test mongosh \
  -u myUserAdmin \
  -p 'NewStrongPass!234' \
  --authenticationDatabase admin \
  test_db --eval "db.Product.countDocuments()"
```

### 3. PostgreSQL Setup

```bash
# Start PostgreSQL
docker run -d --name postgres-test \
  -e POSTGRES_PASSWORD=pass \
  -e POSTGRES_USER=user \
  -e POSTGRES_DB=morphix \
  -p 5432:5432 \
  postgres:15

# Run migrations
export DATABASE_URL="postgresql://user:pass@localhost:5432/morphix"
psql $DATABASE_URL -f migrations/001_initial_schema.sql 2>/dev/null || true
psql $DATABASE_URL -f migrations/004_cdc_checkpoints.sql
```

### 4. Install Dependencies

```bash
pip install pytest pytest-cov testcontainers
```

## Run Tests

### Run all E2E tests

```bash
pytest tests/e2e/test_local_product_etl.py -v -s
```

### Run specific test

```bash
pytest tests/e2e/test_local_product_etl.py::TestProductETLEndToEnd::test_4_batch_etl_pipeline -v -s
```

### Run with coverage

```bash
pytest tests/e2e/test_local_product_etl.py --cov=src --cov-report=html
```

### Run standalone (without pytest)

```bash
python tests/e2e/test_local_product_etl.py
```

## Expected Output

```
TEST 1: MongoDB Connection
==============================================================
Available collections: ['Product']
✅ Can read from Product: 3 documents

TEST 2: Schema Inference
==============================================================
DataFrame shape: (3, 8)
Columns: ['_id', 'name', 'price', 'category', 'specs', 'tags', 'inStock', 'createdAt']
Generating schema...
✅ Schema generated: 15 fields
  - _id: string (required)
  - name: string (required)
  - price: double (required)
  - category: string (required)
  - specs: object (required)
📦 Nested fields detected: ['specs', 'tags']

TEST 3: Data Flattening
==============================================================
Original shape: (3, 8)
Flattening nested structures...
Flattened shape: (3, 14)
Columns added: 6
📋 New flattened columns:
  - specs_cpu
  - specs_ram
  - specs_storage
  - specs_type
  - specs_connectivity
  - tags_count
✅ Flattening successful

TEST 4: Batch ETL Pipeline
==============================================================
Creating ETL pipeline...
Running ETL pipeline...
✅ ETL completed in 15.23s
   Records processed: 3
   Records written: 3
   Hudi path: /tmp/hudi_test/test_db/product
📁 Hudi structure created:
   - .hoodie
   - default

TEST 5: Hudi Data Validation
==============================================================
MongoDB: 3 documents
Reading from Hudi...
Hudi: 3 records
✅ All MongoDB records found in Hudi
MongoDB fields: 8
Hudi fields: 14
✅ Data validation passed

TEST 6: CDC Real-Time Sync [SKIPPED if CDC not implemented]

TEST 7: Schema Evolution Detection
==============================================================
Schema v1: 15 fields
Schema v2: 17 fields
Detecting schema changes...
✅ Detected changes:
   Breaking changes: 0
   Non-breaking changes: 2
   Has breaking changes: False
   Compatible: True

TEST 8: Performance Benchmark
==============================================================
Benchmarking 100 records...
  Duration: 12.34s
  Throughput: 8 records/sec

Benchmarking 500 records...
  Duration: 45.67s
  Throughput: 11 records/sec

Benchmarking 1000 records...
  Duration: 78.90s
  Throughput: 13 records/sec

📊 Performance Summary:
    100 records:  12.34s (     8 rec/sec)
    500 records:  45.67s (    11 rec/sec)
   1000 records:  78.90s (    13 rec/sec)
✅ Average throughput: 11 records/sec

==============================================================
✅ ALL TESTS PASSED
==============================================================
```

## Troubleshooting

### MongoDB Connection Issues

```bash
# Check MongoDB is running
docker ps | grep mongo-test

# Check replica set status
docker exec mongo-test mongosh --eval "rs.status()"

# Check authentication
docker exec -it mongo-test mongosh \
  -u myUserAdmin \
  -p 'NewStrongPass!234' \
  --authenticationDatabase admin \
  --eval "db.runCommand({connectionStatus: 1})"
```

### No Data in Collection

```bash
# Re-insert test data (see step 2 above)
```

### Hudi Write Fails

```bash
# Check Spark is installed
pyspark --version

# Check write permissions
ls -la /tmp/hudi_test/

# Clean up and retry
rm -rf /tmp/hudi_test/
```

### PostgreSQL Connection Issues

```bash
# Check PostgreSQL is running
docker ps | grep postgres-test

# Test connection
psql postgresql://user:pass@localhost:5432/morphix -c "SELECT 1"
```

### CDC Test Fails

```bash
# Ensure replica set is initialized
docker exec mongo-test mongosh --eval "rs.status()"

# Check checkpoint store is accessible
psql postgresql://user:pass@localhost:5432/morphix -c "SELECT * FROM cdc_checkpoints LIMIT 1;"
```

## Test Coverage

The E2E test suite covers:

1. ✅ **MongoDB Connection** - Authentication and read permissions
2. ✅ **Schema Inference** - Automatic schema generation from data
3. ✅ **Data Flattening** - Nested structure flattening
4. ✅ **Batch ETL Pipeline** - Complete ETL flow with Hudi write
5. ✅ **Data Validation** - MongoDB vs Hudi data comparison
6. ✅ **CDC Real-Time Sync** - Change stream processing (if implemented)
7. ✅ **Schema Evolution** - Breaking change detection
8. ✅ **Performance Benchmark** - Throughput measurement

## Notes

- Tests require MongoDB replica set for CDC functionality
- Hudi writes require Spark to be installed and configured
- Performance benchmarks are conservative (10+ records/sec minimum)
- All tests clean up after themselves (test data, Hudi files)

