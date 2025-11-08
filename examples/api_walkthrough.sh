#!/bin/bash
# End-to-End API Walkthrough Script for Morphix ETL Platform
# This script demonstrates a complete user journey through all APIs

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
API_URL="${API_URL:-http://localhost:8000}"
USERNAME="${USERNAME:-alice}"
PASSWORD="${PASSWORD:-StrongPass123!}"
EMAIL="${EMAIL:-alice@example.com}"
FULL_NAME="${FULL_NAME:-Alice User}"

# MongoDB Configuration
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
MONGO_DB="${MONGO_DB:-testdb}"
MONGO_COLLECTION="${MONGO_COLLECTION:-testcollection}"

# Variables to store tokens and IDs
AUTH_TOKEN=""
USER_ID=""
JOB_ID=""

# Helper functions
print_step() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

check_api_server() {
    print_step "Checking API Server"
    if curl -s -f "${API_URL}/docs" > /dev/null 2>&1; then
        print_success "API server is running at ${API_URL}"
    else
        print_error "API server is not running at ${API_URL}"
        echo "Please start the API server:"
        echo "  uvicorn src.api.mongo_api:app --host 0.0.0.0 --port 8000"
        exit 1
    fi
}

# Step 1: User Registration
register_user() {
    print_step "Step 1: User Registration"
    
    RESPONSE=$(curl -s -X POST "${API_URL}/register" \
        -H "Content-Type: application/json" \
        -d "{
            \"username\": \"${USERNAME}\",
            \"password\": \"${PASSWORD}\",
            \"email\": \"${EMAIL}\",
            \"full_name\": \"${FULL_NAME}\"
        }")
    
    if echo "$RESPONSE" | grep -q "already exists"; then
        print_info "User already exists, will proceed to login..."
    elif echo "$RESPONSE" | grep -q "user_id"; then
        USER_ID=$(echo "$RESPONSE" | grep -o '"user_id":"[^"]*' | cut -d'"' -f4)
        print_success "User registered successfully"
        print_info "User ID: ${USER_ID}"
    else
        print_error "Registration failed: ${RESPONSE}"
        exit 1
    fi
}

# Step 2: User Login
login_user() {
    print_step "Step 2: User Login"
    
    RESPONSE=$(curl -s -X POST "${API_URL}/login" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=${USERNAME}&password=${PASSWORD}")
    
    if echo "$RESPONSE" | grep -q "access_token"; then
        AUTH_TOKEN=$(echo "$RESPONSE" | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)
        print_success "Login successful"
        print_info "Token: ${AUTH_TOKEN:0:50}..."
    else
        print_error "Login failed: ${RESPONSE}"
        exit 1
    fi
}

# Step 3: Store MongoDB Credentials
store_mongodb_credentials() {
    print_step "Step 3: Store MongoDB Credentials"
    
    RESPONSE=$(curl -s -X POST "${API_URL}/credentials" \
        -H "Authorization: Bearer ${AUTH_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{
            \"mongo_uri\": \"${MONGO_URI}\",
            \"database\": \"${MONGO_DB}\",
            \"collection\": \"${MONGO_COLLECTION}\"
        }")
    
    if echo "$RESPONSE" | grep -q "status.*ok"; then
        print_success "MongoDB credentials stored"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_error "Failed to store credentials: ${RESPONSE}"
        exit 1
    fi
}

# Step 4: Test MongoDB Connection
test_mongodb_connection() {
    print_step "Step 4: Test MongoDB Connection"
    
    RESPONSE=$(curl -s -X GET "${API_URL}/test-connection" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "connected\|success"; then
        print_success "MongoDB connection successful"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_error "MongoDB connection failed: ${RESPONSE}"
        exit 1
    fi
}

# Step 5: Read Sample Data
read_sample_data() {
    print_step "Step 5: Read Sample Data from MongoDB"
    
    RESPONSE=$(curl -s -X GET "${API_URL}/read?limit=10" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "data\|records"; then
        print_success "Read sample data successfully"
        RECORD_COUNT=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('data', [])))" 2>/dev/null || echo "N/A")
        print_info "Records retrieved: ${RECORD_COUNT}"
    else
        print_info "No data available or empty collection"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    fi
}

# Step 6: Generate Schema
generate_schema() {
    print_step "Step 6: Generate Schema from MongoDB Data"
    
    RESPONSE=$(curl -s -X POST "${API_URL}/schema/generate?sample_size=100" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "schema"; then
        print_success "Schema generated successfully"
        FIELD_COUNT=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('schema', {})))" 2>/dev/null || echo "N/A")
        print_info "Schema contains ${FIELD_COUNT} fields"
        # Save schema for later use
        echo "$RESPONSE" > /tmp/morphix_schema.json
    else
        print_error "Schema generation failed: ${RESPONSE}"
        exit 1
    fi
}

# Step 7: Transform Data
transform_data() {
    print_step "Step 7: Transform Data"
    
    RESPONSE=$(curl -s -X POST "${API_URL}/transform?limit=100&flatten=true" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "data\|transformed"; then
        print_success "Data transformation successful"
        ROWS=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('rows_processed', 'N/A'))" 2>/dev/null || echo "N/A")
        COLS=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('columns', 'N/A'))" 2>/dev/null || echo "N/A")
        print_info "Rows processed: ${ROWS}"
        print_info "Columns: ${COLS}"
    else
        print_error "Data transformation failed: ${RESPONSE}"
        exit 1
    fi
}

# Step 8: Create Batch Job
create_batch_job() {
    print_step "Step 8: Create Batch Job"
    
    JOB_ID="batch_job_$(date +%s)"
    
    RESPONSE=$(curl -s -X POST "${API_URL}/jobs/batch/create" \
        -H "Authorization: Bearer ${AUTH_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{
            \"job_id\": \"${JOB_ID}\",
            \"job_name\": \"Test Batch Job\",
            \"mongo_uri\": \"${MONGO_URI}\",
            \"database\": \"${MONGO_DB}\",
            \"collection\": \"${MONGO_COLLECTION}\",
            \"query\": {},
            \"date_field\": \"created_at\",
            \"hudi_table_name\": \"test_table\",
            \"hudi_base_path\": \"s3://test-bucket/hudi/\",
            \"estimated_daily_volume\": 5000000,
            \"schedule\": {
                \"trigger\": \"manual\"
            }
        }")
    
    if echo "$RESPONSE" | grep -q "job_id\|status"; then
        print_success "Batch job created successfully"
        print_info "Job ID: ${JOB_ID}"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_error "Failed to create batch job: ${RESPONSE}"
        exit 1
    fi
}

# Step 9: Get Job Status
get_job_status() {
    print_step "Step 9: Get Job Status"
    
    if [ -z "$JOB_ID" ]; then
        print_info "No job ID available, skipping"
        return
    fi
    
    RESPONSE=$(curl -s -X GET "${API_URL}/jobs/${JOB_ID}/status" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "status\|job_id"; then
        print_success "Job status retrieved"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_info "Job status endpoint may not be available"
    fi
}

# Step 10: Start Job Execution
start_job() {
    print_step "Step 10: Start Job Execution"
    
    if [ -z "$JOB_ID" ]; then
        print_info "No job ID available, skipping"
        return
    fi
    
    RESPONSE=$(curl -s -X POST "${API_URL}/jobs/${JOB_ID}/start" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "execution_id\|status"; then
        print_success "Job started successfully"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
        print_info "Job is running in background..."
    else
        print_info "Job start endpoint may not be available or job already running"
    fi
}

# Step 11: List All Jobs
list_jobs() {
    print_step "Step 11: List All Jobs"
    
    RESPONSE=$(curl -s -X GET "${API_URL}/jobs" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "jobs\|\[\]"; then
        print_success "Jobs retrieved"
        JOB_COUNT=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('jobs', [])))" 2>/dev/null || echo "0")
        print_info "Total jobs: ${JOB_COUNT}"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_info "Jobs endpoint may not be available"
    fi
}

# Step 12: Trino Health Check
trino_health_check() {
    print_step "Step 12: Trino Health Check"
    
    RESPONSE=$(curl -s -X GET "${API_URL}/trino/health")
    
    if echo "$RESPONSE" | grep -q "healthy\|state"; then
        print_success "Trino is healthy"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_info "Trino may not be available or configured"
    fi
}

# Step 13: List Trino Catalogs
list_trino_catalogs() {
    print_step "Step 13: List Trino Catalogs"
    
    RESPONSE=$(curl -s -X GET "${API_URL}/trino/catalogs" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "catalogs\|\[\]"; then
        print_success "Catalogs retrieved"
        CATALOG_COUNT=$(echo "$RESPONSE" | python3 -c "import sys, json; data=json.load(sys.stdin); print(len(data.get('catalogs', [])))" 2>/dev/null || echo "0")
        print_info "Total catalogs: ${CATALOG_COUNT}"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_info "Trino catalogs endpoint may not be available"
    fi
}

# Step 14: Execute Trino Query
execute_trino_query() {
    print_step "Step 14: Execute Trino Query"
    
    # Try a simple query
    RESPONSE=$(curl -s -X POST "${API_URL}/trino/query" \
        -H "Authorization: Bearer ${AUTH_TOKEN}" \
        -H "Content-Type: application/json" \
        -d '{
            "sql": "SELECT 1 as test",
            "catalog": null,
            "schema": null
        }')
    
    if echo "$RESPONSE" | grep -q "data\|results"; then
        print_success "Trino query executed successfully"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_info "Trino query endpoint may not be available or query failed"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    fi
}

# Step 15: Get Current User Info
get_current_user() {
    print_step "Step 15: Get Current User Info"
    
    RESPONSE=$(curl -s -X GET "${API_URL}/me" \
        -H "Authorization: Bearer ${AUTH_TOKEN}")
    
    if echo "$RESPONSE" | grep -q "username\|user_id"; then
        print_success "User info retrieved"
        echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
    else
        print_info "User info endpoint may not be available"
    fi
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║   Morphix ETL Platform - End-to-End API Walkthrough        ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    check_api_server
    
    # Authentication flow
    register_user
    login_user
    
    # MongoDB operations
    store_mongodb_credentials
    test_mongodb_connection
    read_sample_data
    
    # Schema and transformation
    generate_schema
    transform_data
    
    # Job management
    create_batch_job
    get_job_status
    start_job
    list_jobs
    
    # Trino operations
    trino_health_check
    list_trino_catalogs
    execute_trino_query
    
    # User operations
    get_current_user
    
    # Summary
    echo -e "\n${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║   API Walkthrough Complete!                                  ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    print_info "All API endpoints have been tested"
    print_info "Check the output above for any errors or warnings"
    echo ""
    print_info "Next Steps:"
    echo "  1. Check job execution status: GET /jobs/{job_id}/status"
    echo "  2. Query transformed data: POST /trino/query"
    echo "  3. View API documentation: ${API_URL}/docs"
    echo ""
}

# Run main function
main "$@"

