"""
Unit tests for Apache Iceberg writer.
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestIcebergWriter:
    """Test cases for IcebergWriter class."""
    
    @pytest.mark.skip(reason="Requires Iceberg REST catalog and PyIceberg")
    def test_initialization(self):
        """Test writer initialization."""
        from src.lake.iceberg_writer import IcebergWriter
        
        writer = IcebergWriter()
        assert writer is not None
    
    @pytest.mark.skip(reason="Requires Iceberg REST catalog and PyIceberg")
    def test_create_table(self):
        """Test table creation."""
        from src.lake.iceberg_writer import IcebergWriter
        
        writer = IcebergWriter()
        result = writer.create_table(
            table_name="test_table",
            schema={"id": "int", "name": "string"},
            database="test_db"
        )
        
        assert result is not None
    
    @pytest.mark.skip(reason="Requires Iceberg REST catalog and PyIceberg")
    def test_write_dataframe(self):
        """Test writing DataFrame to Iceberg."""
        from src.lake.iceberg_writer import IcebergWriter
        
        writer = IcebergWriter()
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        result = writer.write_dataframe(
            df=df,
            table_name="test_table",
            database="test_db"
        )
        
        assert result is not None
        assert hasattr(result, 'rows_written')
    
    @patch('src.lake.iceberg_writer.load_catalog')
    def test_convert_pandas_schema(self, mock_catalog):
        """Test pandas schema conversion."""
        from src.lake.iceberg_writer import IcebergWriter
        from pyiceberg.types import StringType, IntegerType, BooleanType, DoubleType
        
        writer = IcebergWriter()
        
        # Test various pandas dtypes
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'str_col': ['a', 'b', 'c'],
            'bool_col': [True, False, True],
            'float_col': [1.1, 2.2, 3.3]
        })
        
        schema = writer._convert_pandas_schema(df)
        
        assert schema is not None
        assert len(schema) > 0
    
    @patch('src.lake.iceberg_writer.load_catalog')
    def test_write_dataframe_with_mock(self, mock_catalog):
        """Test writing DataFrame with mocked catalog."""
        from src.lake.iceberg_writer import IcebergWriter
        
        # Setup mocks
        mock_catalog_instance = MagicMock()
        mock_catalog.return_value = mock_catalog_instance
        
        mock_table = MagicMock()
        mock_catalog_instance.load_table.return_value = mock_table
        
        writer = IcebergWriter()
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        result = writer.write_dataframe(
            df=df,
            table_name="test_table",
            database="test_db"
        )
        
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

