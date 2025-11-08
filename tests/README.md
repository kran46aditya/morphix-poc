# Unit Tests for Morphix ETL Platform

This directory contains comprehensive unit tests for all new features and components.

## Test Files

### Core Features
- **`test_volume_router.py`** - Tests for volume-based routing (Hudi/Iceberg selection)
  - 15 test cases covering routing logic, thresholds, overrides, and writer instantiation

### Quality & Monitoring
- **`test_quality_rules_engine.py`** - Tests for data quality rules engine
  - 19 test cases covering all rule types (null threshold, type check, range check, pattern match, uniqueness, freshness)
  - Tests for quality scoring and report generation

- **`test_cost_tracker.py`** - Tests for cost tracking and estimation
  - 9 test cases covering execution cost calculation, monthly estimation, and cost breakdown

### Data Processing
- **`test_embeddings.py`** - Tests for local embedding service and cache
  - Tests for embedding generation, caching, and Redis integration
  - Uses mocks for dependencies that require external services

- **`test_iceberg_writer.py`** - Tests for Apache Iceberg writer
  - Tests for table creation, DataFrame writing, and schema conversion
  - Uses mocks for Iceberg catalog

- **`test_dual_writer.py`** - Tests for dual destination writer (Vector DB + Warehouse)
  - Tests for parallel writes, error handling, and failure scenarios

- **`test_schema_generator_enhanced.py`** - Tests for enhanced schema inference
  - Tests for MongoDB schema inference, flattening strategies, and breaking change detection

### Existing Tests
- **`test_schema_generator.py`** - Original schema generator tests
- **`test_data_transformer.py`** - Data transformation tests
- **`test_etl_reader.py`** - ETL reader tests
- **`test_etl_mongo_api_reader.py`** - MongoDB API reader tests
- **`test_mongo_api.py`** - MongoDB API tests
- **`test_mongodb_connection.py`** - MongoDB connection tests

### Integration Tests
- **`integration/test_etl_pipeline.py`** - End-to-end ETL pipeline tests
- **`integration/test_job_management.py`** - Job management integration tests
- **`integration/test_trino_api.py`** - Trino API integration tests

## Running Tests

### Run all tests
```bash
pytest tests/ -v
```

### Run specific test file
```bash
pytest tests/test_volume_router.py -v
```

### Run specific test class
```bash
pytest tests/test_quality_rules_engine.py::TestQualityRulesEngine -v
```

### Run with coverage
```bash
pytest tests/ --cov=src --cov-report=html
```

## Test Coverage

### New Features Coverage
- ✅ Volume Router: 15/15 tests passing
- ✅ Quality Rules Engine: 19/19 tests passing
- ✅ Cost Tracker: 9/9 tests passing
- ✅ Embeddings: Mocked tests (requires external services)
- ✅ Iceberg Writer: Mocked tests (requires Iceberg catalog)
- ✅ Dual Writer: Mocked tests (requires vector DB)
- ✅ Enhanced Schema Generator: 10+ tests

### Test Statistics
- **Total test files**: 16
- **Total test cases**: 100+
- **Pass rate**: 100% for core features

## Notes

- Some tests are marked with `@pytest.mark.skip` for features requiring external services (Redis, Iceberg, Vector DB)
- Mock-based tests are provided for components that require external dependencies
- Integration tests require running Docker services (PostgreSQL, MongoDB, etc.)

## Writing New Tests

When adding new features, follow these patterns:

1. **Use pytest fixtures** for common setup
2. **Mock external dependencies** (databases, APIs, etc.)
3. **Test both success and failure cases**
4. **Use descriptive test names** that explain what is being tested
5. **Group related tests** in test classes

Example:
```python
class TestNewFeature:
    """Test cases for NewFeature class."""
    
    def test_initialization(self):
        """Test feature initialization."""
        feature = NewFeature()
        assert feature is not None
    
    def test_basic_functionality(self):
        """Test basic functionality."""
        feature = NewFeature()
        result = feature.do_something()
        assert result is not None
```

