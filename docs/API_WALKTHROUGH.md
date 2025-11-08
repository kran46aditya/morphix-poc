# API Walkthrough Guide

Complete end-to-end walkthrough of all Morphix ETL Platform APIs.

## Overview

The `api_walkthrough.sh` script demonstrates a complete user journey through all available APIs, from user registration to data transformation and querying.

## Prerequisites

1. **API Server Running**
   ```bash
   uvicorn src.api.mongo_api:app --host 0.0.0.0 --port 8000
   ```

2. **MongoDB Available** (optional, for full functionality)
   - MongoDB instance running
   - Test database and collection

3. **Docker Services** (optional, for Trino queries)
   ```bash
   docker-compose up -d
   ```

## Running the Walkthrough

### Basic Usage

```bash
./examples/api_walkthrough.sh
```

### Custom Configuration

```bash
# Set custom values
export API_URL="http://localhost:8000"
export USERNAME="myuser"
export PASSWORD="mypassword"
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="mydb"
export MONGO_COLLECTION="mycollection"

# Run walkthrough
./examples/api_walkthrough.sh
```

## Walkthrough Steps

### 1. User Registration
- **Endpoint**: `POST /register`
- **Purpose**: Create a new user account
- **Request**: Username, password, email, full name
- **Response**: User ID and confirmation

### 2. User Login
- **Endpoint**: `POST /login`
- **Purpose**: Authenticate and get access token
- **Request**: Username and password
- **Response**: JWT access token

### 3. Store MongoDB Credentials
- **Endpoint**: `POST /credentials`
- **Purpose**: Securely store MongoDB connection details
- **Request**: MongoDB URI, database, collection
- **Response**: Confirmation status

### 4. Test MongoDB Connection
- **Endpoint**: `GET /test-connection`
- **Purpose**: Verify MongoDB connectivity
- **Request**: None (uses stored credentials)
- **Response**: Connection status

### 5. Read Sample Data
- **Endpoint**: `GET /read`
- **Purpose**: Retrieve sample data from MongoDB
- **Request**: Optional limit parameter
- **Response**: Array of documents

### 6. Generate Schema
- **Endpoint**: `POST /schema/generate`
- **Purpose**: Infer schema from MongoDB data
- **Request**: Optional sample size
- **Response**: Schema definition with field types

### 7. Transform Data
- **Endpoint**: `POST /transform`
- **Purpose**: Transform and validate data
- **Request**: Optional limit and flatten options
- **Response**: Transformed data and statistics

### 8. Create Batch Job
- **Endpoint**: `POST /jobs/batch/create`
- **Purpose**: Create a scheduled ETL job
- **Request**: Job configuration (MongoDB, Hudi, schedule)
- **Response**: Job ID and status

### 9. Get Job Status
- **Endpoint**: `GET /jobs/{job_id}/status`
- **Purpose**: Check job execution status
- **Request**: Job ID
- **Response**: Current status and execution details

### 10. Start Job Execution
- **Endpoint**: `POST /jobs/{job_id}/start`
- **Purpose**: Manually trigger job execution
- **Request**: Job ID
- **Response**: Execution ID and status

### 11. List All Jobs
- **Endpoint**: `GET /jobs`
- **Purpose**: Retrieve all user's jobs
- **Request**: None
- **Response**: Array of job configurations

### 12. Trino Health Check
- **Endpoint**: `GET /trino/health`
- **Purpose**: Check Trino server status
- **Request**: None
- **Response**: Health status

### 13. List Trino Catalogs
- **Endpoint**: `GET /trino/catalogs`
- **Purpose**: Get available Trino catalogs
- **Request**: None
- **Response**: List of catalogs

### 14. Execute Trino Query
- **Endpoint**: `POST /trino/query`
- **Purpose**: Run SQL query on data lake
- **Request**: SQL query, optional catalog/schema
- **Response**: Query results

### 15. Get Current User Info
- **Endpoint**: `GET /me`
- **Purpose**: Retrieve current authenticated user
- **Request**: None (uses auth token)
- **Response**: User information

## Expected Output

The script provides color-coded output:
- **Green (✓)**: Successful operations
- **Blue (===)**: Step headers
- **Yellow (ℹ)**: Informational messages
- **Red (✗)**: Errors

## Error Handling

The script handles common scenarios:
- User already exists (continues to login)
- Missing services (skips optional steps)
- API errors (displays error messages)

## Customization

### Environment Variables

```bash
# API Configuration
export API_URL="http://localhost:8000"

# User Credentials
export USERNAME="alice"
export PASSWORD="StrongPass123!"
export EMAIL="alice@example.com"
export FULL_NAME="Alice User"

# MongoDB Configuration
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="testdb"
export MONGO_COLLECTION="testcollection"
```

### Modifying Steps

Edit `examples/api_walkthrough.sh` to:
- Add custom steps
- Modify request payloads
- Change validation logic
- Add additional endpoints

## Troubleshooting

### API Server Not Running
```bash
# Start the API server
uvicorn src.api.mongo_api:app --host 0.0.0.0 --port 8000
```

### MongoDB Connection Failed
- Verify MongoDB is running
- Check connection URI
- Ensure credentials are correct

### Trino Endpoints Failing
- Start Docker services: `docker-compose up -d`
- Wait for services to be healthy
- Check Trino configuration

### Authentication Errors
- Verify username/password
- Check token expiration
- Re-login if needed

## Next Steps

After completing the walkthrough:

1. **Explore API Documentation**
   - Visit `${API_URL}/docs` for interactive Swagger UI
   - Try endpoints directly in the browser

2. **Create Production Jobs**
   - Set up scheduled batch jobs
   - Configure streaming jobs
   - Monitor job execution

3. **Query Transformed Data**
   - Use Trino SQL queries
   - Explore data lake tables
   - Build analytics dashboards

4. **Integrate with Applications**
   - Use API tokens for authentication
   - Build custom integrations
   - Automate workflows

## Related Documentation

- [API Reference](API.md) - Complete API documentation
- [Component Documentation](COMPONENTS.md) - Component details
- [Setup Guide](SETUP.md) - Installation and configuration

