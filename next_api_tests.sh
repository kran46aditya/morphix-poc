#!/bin/bash

# Next API Tests After MongoDB Credentials Setup
# MongoDB Connection Details:
#   username: myUserAdmin
#   password: M0ndee12
#   host: localhost
#   port: 27017
#   database: test_db
#   collection: Product

TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoyLCJ1c2VybmFtZSI6ImFsaWNlIiwicm9sZSI6InVzZXIiLCJleHAiOjE3NjIwNzg0MzF9.SzjfNyIW5l-2geclkO7_AWf5D0cPF-7H0ctzVqN9aFc"
BASE_URL="http://localhost:8000"

# MongoDB connection details
MONGO_USER="myUserAdmin"
MONGO_PASS="M0ndee12"
MONGO_HOST="localhost"
MONGO_PORT=27017
MONGO_DB="test_db"
MONGO_COLLECTION="Product"
USER_ID="2"

echo "=== 1. Read MongoDB Data (using stored credentials via user_id) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/mongo/read" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"user_id\": \"$USER_ID\"
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== 1b. Read MongoDB Data (with direct connection details) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/mongo/read" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$MONGO_USER\",
    \"password\": \"$MONGO_PASS\",
    \"host\": \"$MONGO_HOST\",
    \"port\": $MONGO_PORT,
    \"database\": \"$MONGO_DB\",
    \"collection\": \"$MONGO_COLLECTION\",
    \"use_pyspark\": false
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== 2. Generate Schema from Collection (using stored credentials) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/schema/generate" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"user_id\": \"$USER_ID\",
    \"sample_size\": 100,
    \"include_constraints\": true
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== 2b. Generate Schema (with direct connection details) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/schema/generate" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$MONGO_USER\",
    \"password\": \"$MONGO_PASS\",
    \"host\": \"$MONGO_HOST\",
    \"port\": $MONGO_PORT,
    \"database\": \"$MONGO_DB\",
    \"collection\": \"$MONGO_COLLECTION\",
    \"sample_size\": 100,
    \"include_constraints\": true
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== 3. Transform Data (using stored credentials, flatten only) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/mongo/transform" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"user_id\": \"$USER_ID\",
    \"limit\": 10,
    \"flatten_data\": true,
    \"apply_schema\": false
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== 3b. Transform Data (with direct connection details) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/mongo/transform" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$MONGO_USER\",
    \"password\": \"$MONGO_PASS\",
    \"host\": \"$MONGO_HOST\",
    \"port\": $MONGO_PORT,
    \"database\": \"$MONGO_DB\",
    \"collection\": \"$MONGO_COLLECTION\",
    \"limit\": 10,
    \"flatten_data\": true,
    \"apply_schema\": false
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== 4. Transform Data with Schema Validation (using stored credentials) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/mongo/transform" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"user_id\": \"$USER_ID\",
    \"limit\": 10,
    \"flatten_data\": true,
    \"apply_schema\": true,
    \"strict_schema\": false
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== 4b. Transform Data with Schema Validation (direct connection) ==="
RESPONSE=$(curl -s -X POST "$BASE_URL/mongo/transform" \
  -H "accept: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"username\": \"$MONGO_USER\",
    \"password\": \"$MONGO_PASS\",
    \"host\": \"$MONGO_HOST\",
    \"port\": $MONGO_PORT,
    \"database\": \"$MONGO_DB\",
    \"collection\": \"$MONGO_COLLECTION\",
    \"limit\": 10,
    \"flatten_data\": true,
    \"apply_schema\": true,
    \"strict_schema\": false
  }")
echo "$RESPONSE" | jq '.' 2>/dev/null || echo "$RESPONSE"

echo -e "\n\n=== Test Summary ==="
echo "Tested MongoDB connection with:"
echo "   Database: $MONGO_DB"
echo "   Collection: $MONGO_COLLECTION"
echo "   Host: $MONGO_HOST:$MONGO_PORT"

