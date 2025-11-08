# Morphix ETL Platform

A comprehensive ETL (Extract, Transform, Load) platform for processing data from MongoDB into Apache Hudi tables with SQL query capabilities via Trino.

## Overview

The Morphix ETL Platform provides a complete solution for:
- **Extracting** data from MongoDB (PyMongo and PySpark support)
- **Transforming** data with schema validation and flattening
- **Loading** data into Apache Hudi tables (data lake format)
- **Querying** transformed data via Trino SQL
- **Scheduling** batch and streaming ETL jobs
- **Managing** schemas, credentials, and job configurations

## Quick Start

1. **Setup**: Follow the [SETUP.md](SETUP.md) guide for installation and configuration
2. **API Usage**: See [API.md](API.md) for all available endpoints and examples
3. **Understanding Flow**: Review [FLOW.md](FLOW.md) for architecture and data flow

## Documentation

### [SETUP.md](SETUP.md) - Installation & Configuration
Complete setup guide including:
- Prerequisites and system requirements
- Python environment setup
- Database configuration (PostgreSQL)
- MongoDB setup
- Trino server installation
- Environment configuration
- Troubleshooting

### [API.md](API.md) - API Reference
Complete API documentation including:
- Authentication endpoints
- MongoDB operations
- Schema management
- Data transformation
- Hudi table management
- Job scheduling and management
- Trino query API
- Error handling

### [FLOW.md](FLOW.md) - Architecture & Data Flow
System design and workflows including:
- Architecture diagrams
- Data flow explanations
- Component details
- Module interactions
- ETL pipeline workflows
- Example use cases
- Performance considerations

## Key Features

- **Multiple Data Sources**: Support for MongoDB with flexible connection options
- **Dual Processing Engines**: PyMongo for small datasets, PySpark for large-scale processing
- **Schema Management**: Auto-generation, validation, and Avro/JSON schema support
- **Data Lake Storage**: Apache Hudi for ACID transactions and time travel queries
- **SQL Querying**: Trino integration for standard SQL access to Hudi tables
- **Job Scheduling**: Automated batch and streaming job execution
- **RESTful API**: Comprehensive REST API for all operations
- **Authentication**: JWT-based security for API access

## Technology Stack

- **FastAPI** - Python web framework
- **MongoDB** - Source data storage
- **PostgreSQL** - Metadata and job storage
- **Apache Spark** - Distributed data processing
- **Apache Hudi** - Data lake storage format
- **Trino** - Distributed SQL query engine
- **Pandas** - Data manipulation
- **Pydantic** - Data validation

## Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL 12+
- MongoDB
- Docker (optional, for Trino)
- Java 11+ (for Spark/Hudi)

### Installation

```bash
# Clone repository
git clone <repository-url>
cd morphix-poc

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Start API server
uvicorn src.api.mongo_api:app --reload --port 8000
```

See [SETUP.md](SETUP.md) for detailed instructions.

## Basic Usage Example

```bash
# 1. Register and login
curl -X POST "http://localhost:8000/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "user",
    "email": "user@example.com",
    "password": "SecurePass123!",
    "full_name": "Test User"
  }'

# 2. Store MongoDB credentials
curl -X POST "http://localhost:8000/mongo/credentials" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "my_source",
    "mongo_uri": "mongodb://localhost:27017/testdb",
    "database": "testdb",
    "collection": "products"
  }'

# 3. Generate schema
curl -X POST "http://localhost:8000/schema/generate" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "my_source", "sample_size": 1000}'

# 4. Transform and load data
curl -X POST "http://localhost:8000/mongo/transform" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "my_source",
    "flatten_data": true,
    "apply_schema": true
  }'

# 5. Query via Trino (if configured)
curl -X POST "http://localhost:8000/trino/query" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT * FROM hive.default.products LIMIT 10"
  }'
```

## Project Structure

```
morphix-poc/
├── src/
│   ├── api/              # FastAPI endpoints
│   ├── auth/             # Authentication system
│   ├── etl/              # ETL modules (reader, transformer)
│   ├── hudi_writer/      # Hudi table operations
│   ├── jobs/             # Job management and scheduling
│   ├── mongodb/          # MongoDB utilities
│   ├── postgres/         # PostgreSQL utilities
│   └── trino/            # Trino client
├── config/               # Configuration management
├── tests/                # Test suite
├── docs/                 # Additional documentation
├── SETUP.md              # Setup guide
├── API.md                # API reference
├── FLOW.md               # Architecture guide
└── README.md             # This file
```

## API Documentation

Interactive API documentation is available when the server is running:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## Contributing

1. Review the architecture in [FLOW.md](FLOW.md)
2. Follow setup instructions in [SETUP.md](SETUP.md)
3. Refer to [API.md](API.md) for API specifications
4. Write tests for new features
5. Update documentation as needed

## Support

For issues, questions, or contributions:
1. Check [SETUP.md](SETUP.md) for configuration issues
2. Review [API.md](API.md) for API usage questions
3. See [FLOW.md](FLOW.md) for architecture understanding

## License

[Specify your license here]

---

**Quick Links:**
- [Setup Guide](SETUP.md)
- [API Reference](API.md)
- [Architecture & Flow](FLOW.md)

