# End-to-End API Testing Guide

Complete step-by-step guide to test the Morphix ETL Platform APIs from user registration to data querying.

## Prerequisites

1. **API Server Running**: Make sure your API server is running
   ```bash
   uvicorn src.api.mongo_api:app --reload --host 0.0.0.0 --port 8000
   ```

2. **MongoDB Accessible**: Your MongoDB should be running and accessible at `localhost:27017`

3. **Docker Services Running**: All Docker Compose services should be up
   ```bash
   docker compose ps
   ```

---

## Step 1: User Registration

Register a new user account for the platform.

### Request

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

### Expected Response (201 Created)

```json
{
  "id": "user-id-here",
  "username": "alice",
  "email": "alice@example.com",
  "full_name": "Alice User",
  "is_active": true,
  "is_superuser": false,
  "created_at": "2025-11-02T11:00:00Z"
}
```

### Notes
- Save the user `id` if you need it later
- The username and password will be used for authentication

---

## Step 2: User Login

Login to get an access token for authenticated requests.

### Request

```bash
curl -X POST "http://localhost:8000/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=alice&password=StrongPass123!"
```

### Expected Response (200 OK)

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "user": {
    "id": "user-id-here",
    "username": "alice",
    "email": "alice@example.com",
    "full_name": "Alice User"
  }
}
```

### Save the Token

```bash
# Save token to environment variable
export TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

### Notes
- Copy the `access_token` value
- This token will be used in the `Authorization: Bearer <token>` header for all subsequent requests
- Token typically expires in 30 minutes (configurable)

---

## Step 3: Store MongoDB Credentials

Store MongoDB connection credentials for the user to use in ETL operations.

### Request

```bash
curl -X POST "http://localhost:8000/mongo/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "username": "myUserAdmin",
    "password": "M0ndee12",
    "host": "localhost",
    "port": 27017,
    "database": "test_db",
    "collection": "Product"
  }'
```

### Expected Response (200 OK or 201 Created)

```json
{
  "status": "success",
  "message": "Credentials saved successfully",
  "user_id": "alice"
}
```

### Notes
- Credentials are stored securely in PostgreSQL
- The `user_id` should match your username or be a unique identifier
- These credentials will be used for all MongoDB operations for this user

### Verify Credentials Stored

```bash
curl -X GET "http://localhost:8000/mongo/credentials/alice" \
  -H "Authorization: Bearer $TOKEN"
```

---

## Step 4: Test MongoDB Connection

Verify that the stored credentials can successfully connect to MongoDB.

### Request

```bash
curl -X POST "http://localhost:8000/mongo/test-connection" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice"
  }'
```

### Expected Response (200 OK)

```json
{
  "status": "success",
  "connected": true,
  "message": "Successfully connected to MongoDB",
  "server_info": {
    "version": "7.0.x",
    "host": "localhost:27017"
  }
}
```

### Error Response (If Connection Fails)

```json
{
  "status": "error",
  "connected": false,
  "message": "Connection failed: ...",
  "error": "error details"
}
```

### Notes
- Fix any connection issues before proceeding
- Verify MongoDB is running and accessible
- Check firewall rules if connection fails

---

## Step 5: Read Sample Data from MongoDB

Read a small sample of data to verify the connection and see the data structure.

### Request

```bash
curl -X POST "http://localhost:8000/mongo/read" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "limit": 5
  }'
```

### Expected Response (200 OK)

```json
{
  "status": "success",
  "records_read": 5,
  "data": [
    {
      "_id": "...",
      "field1": "value1",
      "field2": "value2",
      ...
    },
    ...
  ],
  "sample_size": 5
}
```

### Notes
- This helps you understand the data structure
- Use this to verify your collection has data
- Note the field names for schema generation

---

## Step 6: Generate Schema

Automatically generate a schema based on sample data from MongoDB.

### Request

```bash
curl -X POST "http://localhost:8000/schema/generate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "sample_size": 1000
  }'
```

### Expected Response (200 OK)

```json
{
  "status": "success",
  "schema_generated": true,
  "user_id": "alice",
  "schema": {
    "type": "record",
    "name": "Product",
    "fields": [
      {
        "name": "_id",
        "type": ["string", "null"]
      },
      {
        "name": "product_name",
        "type": ["string", "null"]
      },
      {
        "name": "price",
        "type": ["double", "null"]
      },
      ...
    ]
  },
  "sample_size": 1000,
  "records_analyzed": 1000
}
```

### Notes
- Larger `sample_size` gives better schema accuracy
- Schema is generated in Avro format
- Save the schema if you want to review/modify it later

### Get Stored Schema

```bash
curl -X GET "http://localhost:8000/schema/alice" \
  -H "Authorization: Bearer $TOKEN"
```

---

## Step 7: Transform and Load Data to Hudi

Extract data from MongoDB, transform it, and load it into Hudi tables. This is the core ETL operation.

### Request (Using Pandas - Smaller Datasets)

```bash
curl -X POST "http://localhost:8000/mongo/transform" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "flatten_data": true,
    "apply_schema": true,
    "use_spark": false,
    "limit": 10000
  }'
```

### Request (Using Spark - Larger Datasets)

```bash
curl -X POST "http://localhost:8000/mongo/transform" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "flatten_data": true,
    "apply_schema": true,
    "use_spark": true,
    "limit": null
  }'
```

### Expected Response (200 OK)

```json
{
  "status": "success",
  "transformation_complete": true,
  "records_processed": 1000,
  "records_written": 1000,
  "hudi_table_path": "/tmp/hudi/product_table",
  "table_name": "product",
  "execution_time_seconds": 15.5,
  "spark_mode": false
}
```

### Notes
- `use_spark: false` - Uses Pandas (faster for small datasets)
- `use_spark: true` - Uses Spark cluster (better for large datasets)
- `flatten_data: true` - Flattens nested JSON structures
- `apply_schema: true` - Applies the generated schema validation
- `limit: null` - Processes all data (use with caution)

### Monitor Progress

For large datasets, the operation may take time. You can check the API logs or Spark UI (http://localhost:8080) if using Spark.

---

## Step 8: Check Hudi Table Info

Verify that the Hudi table was created successfully and check its metadata.

### Request

```bash
curl -X GET "http://localhost:8000/hudi/table/info?table_name=product&user_id=alice" \
  -H "Authorization: Bearer $TOKEN"
```

### Expected Response (200 OK)

```json
{
  "status": "success",
  "table_name": "product",
  "table_path": "/tmp/hudi/product_table",
  "records_count": 1000,
  "last_updated": "2025-11-02T11:45:00Z",
  "table_type": "COPY_ON_WRITE",
  "schema": {...}
}
```

### Notes
- Verifies the table exists
- Shows record count
- Confirms table path for Trino queries

---

## Step 9: List Tables in Trino

Before querying, check what tables are available in Trino.

### Request

```bash
curl -X GET "http://localhost:8000/trino/tables?catalog=hive&schema=default" \
  -H "Authorization: Bearer $TOKEN"
```

### Expected Response (200 OK)

```json
{
  "status": "ok",
  "catalog": "hive",
  "schema": "default",
  "tables": [
    {
      "catalog": "hive",
      "schema": "default",
      "table": "product",
      "table_type": "TABLE",
      "comment": null
    }
  ]
}
```

### Notes
- Catalog should be `hive`
- Schema is typically `default`
- Your Hudi table should appear here

---

## Step 10: Describe Table Schema in Trino

Get the column information for your table.

### Request

```bash
curl -X GET "http://localhost:8000/trino/table/product/describe?catalog=hive&schema=default" \
  -H "Authorization: Bearer $TOKEN"
```

### Expected Response (200 OK)

```json
{
  "status": "ok",
  "catalog": "hive",
  "schema": "default",
  "table": "product",
  "columns": [
    {
      "name": "_id",
      "type": "varchar",
      "comment": null
    },
    {
      "name": "product_name",
      "type": "varchar",
      "comment": null
    },
    {
      "name": "price",
      "type": "double",
      "comment": null
    },
    ...
  ]
}
```

### Notes
- Use this to understand column names and types
- Helps construct correct SQL queries

---

## Step 11: Query Data via Trino

Execute SQL queries against your Hudi table through Trino.

### Example 1: Simple SELECT

```bash
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM hive.default.product LIMIT 10",
    "catalog": "hive",
    "schema": "default",
    "max_rows": 10
  }'
```

### Expected Response (200 OK)

```json
{
  "status": "ok",
  "query": "SELECT * FROM hive.default.product LIMIT 10",
  "columns": ["_id", "product_name", "price", "category", ...],
  "data": [
    ["id1", "Product A", 29.99, "Electronics", ...],
    ["id2", "Product B", 49.99, "Clothing", ...],
    ...
  ],
  "rows_returned": 10,
  "execution_time_ms": 245.5,
  "query_id": "20251102_113045_00001_xyz",
  "error": null
}
```

### Example 2: Aggregate Query

```bash
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM hive.default.product GROUP BY category",
    "catalog": "hive",
    "schema": "default"
  }'
```

### Example 3: Filter Query

```bash
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM hive.default.product WHERE price > 50 ORDER BY price DESC LIMIT 20",
    "catalog": "hive",
    "schema": "default",
    "max_rows": 20
  }'
```

### Example 4: Complex Query

```bash
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT category, MIN(price) as min_price, MAX(price) as max_price, AVG(price) as avg_price, COUNT(*) as total FROM hive.default.product WHERE price IS NOT NULL GROUP BY category ORDER BY avg_price DESC",
    "catalog": "hive",
    "schema": "default"
  }'
```

### Notes
- Use standard SQL syntax
- Catalog and schema are required: `hive.default.table_name`
- Large queries may take time
- Use `max_rows` to limit results
- Query results are returned as arrays (column-first format)

---

## Step 12: Check Trino Health

Verify Trino is operating correctly.

### Request

```bash
curl -X GET "http://localhost:8000/trino/health" \
  -H "Authorization: Bearer $TOKEN"
```

### Expected Response (200 OK)

```json
{
  "status": "ok",
  "message": "Trino server is reachable",
  "catalogs_count": 1
}
```

---

## Complete Testing Script

Here's a complete bash script you can save and run:

```bash
#!/bin/bash
# Complete End-to-End Test Script

API_URL="http://localhost:8000"
USERNAME="alice"
PASSWORD="StrongPass123!"

echo "=== Step 1: User Registration ==="
REGISTER_RESPONSE=$(curl -s -X POST "$API_URL/auth/register" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$USERNAME\",
    \"email\": \"alice@example.com\",
    \"password\": \"$PASSWORD\",
    \"full_name\": \"Alice User\"
  }")
echo "$REGISTER_RESPONSE" | jq .
echo ""

echo "=== Step 2: User Login ==="
LOGIN_RESPONSE=$(curl -s -X POST "$API_URL/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=$USERNAME&password=$PASSWORD")
TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.access_token')
echo "Token: ${TOKEN:0:50}..."
echo ""

echo "=== Step 3: Store MongoDB Credentials ==="
curl -s -X POST "$API_URL/mongo/credentials" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "username": "myUserAdmin",
    "password": "M0ndee12",
    "host": "localhost",
    "port": 27017,
    "database": "test_db",
    "collection": "Product"
  }' | jq .
echo ""

echo "=== Step 4: Test MongoDB Connection ==="
curl -s -X POST "$API_URL/mongo/test-connection" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice"}' | jq .
echo ""

echo "=== Step 5: Read Sample Data ==="
curl -s -X POST "$API_URL/mongo/read" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "limit": 5}' | jq .
echo ""

echo "=== Step 6: Generate Schema ==="
curl -s -X POST "$API_URL/schema/generate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "alice", "sample_size": 1000}' | jq .
echo ""

echo "=== Step 7: Transform and Load to Hudi ==="
curl -s -X POST "$API_URL/mongo/transform" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "alice",
    "flatten_data": true,
    "apply_schema": true,
    "use_spark": false,
    "limit": 10000
  }' | jq .
echo ""

echo "=== Step 8: List Trino Tables ==="
curl -s -X GET "$API_URL/trino/tables?catalog=hive&schema=default" \
  -H "Authorization: Bearer $TOKEN" | jq .
echo ""

echo "=== Step 9: Describe Table ==="
curl -s -X GET "$API_URL/trino/table/product/describe?catalog=hive&schema=default" \
  -H "Authorization: Bearer $TOKEN" | jq .
echo ""

echo "=== Step 10: Query Data ==="
curl -s -X POST "$API_URL/trino/query" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM hive.default.product LIMIT 10",
    "catalog": "hive",
    "schema": "default",
    "max_rows": 10
  }' | jq .
echo ""

echo "=== Test Complete! ==="
```

Save this as `test_e2e.sh`, make it executable, and run it:
```bash
chmod +x test_e2e.sh
./test_e2e.sh
```

---

## Troubleshooting

### Issue: Authentication Fails
- Verify username and password are correct
- Check token hasn't expired
- Re-login to get a fresh token

### Issue: MongoDB Connection Fails
- Verify MongoDB is running: `mongosh --eval "db.version()"`
- Check credentials are correct
- Verify network connectivity

### Issue: Schema Generation Fails
- Ensure collection has data
- Try smaller `sample_size`
- Check MongoDB collection name is correct

### Issue: Transform Fails
- Check Spark cluster is running if using `use_spark: true`
- Verify Hudi base path is writable
- Check disk space

### Issue: Trino Queries Return No Results
- Verify table exists in Trino: `/trino/tables`
- Check table path matches Hudi table location
- Ensure Hive Metastore is running and connected
- Verify table was written to correct schema

### Issue: Trino Can't See Tables
- Restart Hive Metastore: `docker compose restart hive-metastore`
- Restart Trino: `docker compose restart trino`
- Check Hive Metastore logs for errors
- Verify table was created in the expected location

---

## Additional Useful Endpoints

### List All Catalogs in Trino
```bash
curl -X GET "http://localhost:8000/trino/catalogs" \
  -H "Authorization: Bearer $TOKEN"
```

### List Schemas
```bash
curl -X GET "http://localhost:8000/trino/schemas?catalog=hive" \
  -H "Authorization: Bearer $TOKEN"
```

### Get User Profile
```bash
curl -X GET "http://localhost:8000/auth/me" \
  -H "Authorization: Bearer $TOKEN"
```

---

## Next Steps After Testing

1. **Schedule Jobs**: Set up batch or streaming jobs for automated ETL
2. **Optimize Queries**: Create indexes or optimize Hudi table configuration
3. **Set Up Monitoring**: Monitor job execution and query performance
4. **Scale Resources**: Adjust Spark worker resources if needed
5. **Backup Data**: Set up regular backups of Hudi tables and metadata

---

**Happy Testing!** 🚀

