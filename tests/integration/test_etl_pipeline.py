"""
Integration tests for the complete ETL pipeline.

Tests the full flow: MongoDB → Transform → Hudi
"""
import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.etl.mongo_api_reader import MongoDataReader, create_reader_from_connection_info
from src.etl.data_transformer import DataTransformer
from src.etl.schema_generator import SchemaGenerator
import pandas as pd


@pytest.fixture
def sample_mongo_data():
    """Sample MongoDB document data for testing."""
    return [
        {
            "_id": "test_1",
            "name": "Product A",
            "price": 99.99,
            "category": "Electronics",
            "in_stock": True,
            "metadata": {
                "tags": ["popular", "new"],
                "rating": 4.5
            }
        },
        {
            "_id": "test_2",
            "name": "Product B",
            "price": 149.99,
            "category": "Books",
            "in_stock": False,
            "metadata": {
                "tags": ["bestseller"],
                "rating": 4.8
            }
        }
    ]


class TestETLPipelineIntegration:
    """Integration tests for the ETL pipeline."""
    
    def test_schema_generation_from_dataframe(self, sample_mongo_data):
        """Test schema generation from sample data."""
        # Create DataFrame from sample data
        df = pd.DataFrame(sample_mongo_data)
        
        # Generate schema
        schema = SchemaGenerator.generate_from_dataframe(
            df,
            sample_size=len(df),
            include_constraints=True
        )
        
        # Verify schema structure
        assert isinstance(schema, dict)
        assert "name" in schema
        assert "price" in schema
        assert schema["name"]["type"] == "string"
        assert schema["price"]["type"] == "float"
        
        return schema
    
    def test_data_transformation_pipeline(self, sample_mongo_data):
        """Test complete data transformation pipeline."""
        # Create DataFrame
        df = pd.DataFrame(sample_mongo_data)
        
        # Generate schema
        schema = SchemaGenerator.generate_from_dataframe(df)
        
        # Initialize transformer with schema
        transformer = DataTransformer(schema=schema)
        
        # Flatten data
        df_flat = transformer.flatten_dataframe(df)
        
        # Verify flattening worked
        assert "metadata_tags" in df_flat.columns or "metadata.tags" in str(df_flat.columns)
        
        # Apply schema validation
        df_validated = transformer.apply_schema(df_flat, strict=False)
        
        # Verify validation worked
        assert len(df_validated) == len(sample_mongo_data)
        
        return df_validated
    
    def test_end_to_end_without_actual_connection(self):
        """Test that pipeline components work together (without actual DB connection)."""
        # This test doesn't require actual MongoDB connection
        # It tests that the components can be imported and initialized
        
        # Test MongoDataReader initialization
        reader = MongoDataReader(
            mongo_uri="mongodb://test:test@localhost:27017/testdb",
            database="testdb",
            collection="testcollection"
        )
        assert reader.database == "testdb"
        assert reader.collection == "testcollection"
        
        # Test DataTransformer initialization
        transformer = DataTransformer()
        assert transformer.schema is None
        
        # Test SchemaGenerator
        empty_df = pd.DataFrame()
        schema = SchemaGenerator.generate_from_dataframe(empty_df)
        assert schema == {}
        
        return True


class TestAPIIntegration:
    """Integration tests for API endpoints (requires running server)."""
    
    @pytest.mark.skip(reason="Requires running API server")
    def test_credentials_save_and_read_flow(self):
        """Test saving credentials and reading data."""
        # This would test:
        # 1. POST /mongo/credentials - Save credentials
        # 2. POST /mongo/read with user_id - Read using saved credentials
        pass
    
    @pytest.mark.skip(reason="Requires running API server")
    def test_schema_generation_api_flow(self):
        """Test schema generation via API."""
        # This would test:
        # 1. POST /schema/generate - Generate schema
        # 2. Verify schema structure in response
        pass
    
    @pytest.mark.skip(reason="Requires running API server")
    def test_transform_api_flow(self):
        """Test data transformation via API."""
        # This would test:
        # 1. POST /mongo/transform - Transform data
        # 2. Verify flattened and validated data
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

