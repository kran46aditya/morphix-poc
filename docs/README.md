# Morphix ETL Platform Documentation

Complete documentation for the Morphix ETL Platform.

## Quick Links

- **[API Reference](API.md)** - Complete API documentation with examples
- **[Component Documentation](COMPONENTS.md)** - Detailed documentation for all major components
- **[API Walkthrough](API_WALKTHROUGH.md)** - Step-by-step guide to using all APIs
- **[Setup Guide](SETUP.md)** - Installation and configuration
- **[Architecture & Flow](FLOW.md)** - System design and data flow
- **[Docker Setup](DOCKER_SETUP.md)** - Docker-based deployment
- **[Troubleshooting](DOCKER_TROUBLESHOOTING.md)** - Common issues and solutions

## Documentation Structure

### Getting Started
1. **Setup Guide** - Install and configure the platform
2. **API Walkthrough** - Run the end-to-end walkthrough script
3. **API Reference** - Explore all available endpoints

### Understanding the System
1. **Architecture & Flow** - Learn how the system works
2. **Component Documentation** - Deep dive into each component
3. **Docker Setup** - Deploy using Docker

### Operations
1. **Troubleshooting** - Solve common problems
2. **API Reference** - Find specific endpoints
3. **Component Documentation** - Understand component behavior

## Quick Start

1. **Setup the platform:**
   ```bash
   # Follow SETUP.md for detailed instructions
   pip install -r requirements.txt
   docker-compose up -d
   ```

2. **Start the API server:**
   ```bash
   uvicorn src.api.mongo_api:app --host 0.0.0.0 --port 8000
   ```

3. **Run the API walkthrough:**
   ```bash
   ./examples/api_walkthrough.sh
   ```

4. **Explore the API:**
   - Interactive docs: http://localhost:8000/docs
   - API reference: See [API.md](API.md)

## Component Overview

### Core Components
- **Volume Router** - Routes data to optimal storage (Hudi/Iceberg)
- **ETL Pipeline** - Orchestrates extract, transform, load processes
- **Schema Generator** - Infers and manages data schemas
- **Data Transformer** - Transforms and validates data

### Data Processing
- **MongoDB Reader** - Extracts data from MongoDB
- **Hudi Writer** - Writes to Apache Hudi tables
- **Iceberg Writer** - Writes to Apache Iceberg tables
- **Dual Destination Writer** - Writes to both Vector DB and Warehouse

### Quality & Monitoring
- **Quality Rules Engine** - Validates data quality
- **Cost Tracker** - Tracks infrastructure costs

### Job Management
- **Job Manager** - Manages job configurations
- **Job Scheduler** - Schedules and executes jobs
- **Batch Processor** - Processes batch ETL jobs
- **Stream Processor** - Processes streaming ETL jobs

### Embeddings
- **Local Embedder** - Generates embeddings locally
- **Embedding Cache** - Manages Redis cache for embeddings

## API Endpoints

### Authentication
- `POST /register` - Register new user
- `POST /login` - User login
- `GET /me` - Get current user

### MongoDB Operations
- `POST /credentials` - Store MongoDB credentials
- `GET /credentials` - Retrieve credentials
- `GET /test-connection` - Test MongoDB connection
- `GET /read` - Read data from MongoDB

### Schema Management
- `POST /schema/generate` - Generate schema from data
- `POST /transform` - Transform data

### Job Management
- `POST /jobs/batch/create` - Create batch job
- `POST /jobs/stream/create` - Create stream job
- `GET /jobs` - List all jobs
- `GET /jobs/{job_id}/status` - Get job status
- `POST /jobs/{job_id}/start` - Start job execution
- `POST /jobs/{job_id}/stop` - Stop job execution

### Trino Operations
- `GET /trino/health` - Health check
- `GET /trino/catalogs` - List catalogs
- `GET /trino/schemas` - List schemas
- `GET /trino/tables` - List tables
- `POST /trino/query` - Execute SQL query

## Examples

### Python Client Example
```python
import requests

# Login
response = requests.post("http://localhost:8000/login", data={
    "username": "alice",
    "password": "password"
})
token = response.json()["access_token"]

# Store credentials
headers = {"Authorization": f"Bearer {token}"}
requests.post("http://localhost:8000/credentials", headers=headers, json={
    "mongo_uri": "mongodb://localhost:27017",
    "database": "mydb",
    "collection": "mycollection"
})

# Generate schema
response = requests.post(
    "http://localhost:8000/schema/generate",
    headers=headers
)
schema = response.json()["schema"]
```

### Shell Script Example
```bash
# Run the complete walkthrough
./examples/api_walkthrough.sh

# Or customize
export API_URL="http://localhost:8000"
export USERNAME="myuser"
./examples/api_walkthrough.sh
```

## Support

For issues, questions, or contributions:
1. Check [Troubleshooting](DOCKER_TROUBLESHOOTING.md)
2. Review [Component Documentation](COMPONENTS.md)
3. Explore [API Reference](API.md)

## Version

Documentation for Morphix ETL Platform v1.0.0
