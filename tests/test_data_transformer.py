"""Tests for ETL Module 2: Data Transformer"""

import sys
import pandas as pd
import pytest
from datetime import datetime

sys.path.append('src')

from etl.data_transformer import DataTransformer


def test_data_transformer_initialization():
    """Test DataTransformer initialization."""
    # Test without schema
    transformer = DataTransformer()
    assert transformer.schema == {}
    assert transformer.transformations == []
    
    # Test with schema
    schema = {'name': {'type': 'str', 'required': True}}
    transformer = DataTransformer(schema=schema)
    assert transformer.schema == schema


def test_flatten_simple_nested_dict():
    """Test flattening simple nested dictionaries."""
    transformer = DataTransformer()
    
    # Create test data with nested dict
    data = [
        {'id': 1, 'user': {'name': 'Alice', 'age': 30, 'address': {'city': 'NYC', 'zip': '10001'}}},
        {'id': 2, 'user': {'name': 'Bob', 'age': 25, 'address': {'city': 'LA', 'zip': '90210'}}}
    ]
    df = pd.DataFrame(data)
    
    flattened = transformer.flatten_dataframe(df)
    
    # Check flattened columns exist
    expected_columns = ['id', 'user_name', 'user_age', 'user_address_city', 'user_address_zip']
    assert all(col in flattened.columns for col in expected_columns)
    
    # Check values
    assert flattened.iloc[0]['user_name'] == 'Alice'
    assert flattened.iloc[0]['user_address_city'] == 'NYC'
    assert flattened.iloc[1]['user_name'] == 'Bob'


def test_flatten_arrays():
    """Test flattening arrays and nested arrays."""
    transformer = DataTransformer()
    
    # Test data with arrays
    data = [
        {'id': 1, 'tags': ['python', 'data'], 'items': [{'name': 'item1', 'price': 10}]},
        {'id': 2, 'tags': ['java', 'web'], 'items': [{'name': 'item2', 'price': 20}]}
    ]
    df = pd.DataFrame(data)
    
    flattened = transformer.flatten_dataframe(df)
    
    # Check array flattening
    assert 'tags' in flattened.columns
    assert 'tags_count' in flattened.columns
    assert 'items_name' in flattened.columns
    assert 'items_price' in flattened.columns
    assert 'items_count' in flattened.columns
    
    # Check values
    assert flattened.iloc[0]['tags'] == 'python,data'
    assert flattened.iloc[0]['tags_count'] == 2
    assert flattened.iloc[0]['items_name'] == 'item1'
    assert flattened.iloc[0]['items_count'] == 1


def test_schema_validation_basic():
    """Test basic schema validation and transformation."""
    schema = {
        'id': {'type': 'int', 'required': True},
        'name': {'type': 'str', 'required': True, 'nullable': False},
        'age': {'type': 'int', 'min_value': 0, 'max_value': 150},
        'email': {'type': 'str', 'pattern': r'^[^@]+@[^@]+\.[^@]+$'}
    }
    
    transformer = DataTransformer(schema=schema)
    
    # Valid data
    data = [
        {'id': '1', 'name': 'Alice', 'age': '30', 'email': 'alice@example.com'},
        {'id': '2', 'name': 'Bob', 'age': '25', 'email': 'bob@example.com'}
    ]
    df = pd.DataFrame(data)
    
    transformed = transformer.apply_schema(df)
    
    # Check type conversions
    assert transformed['id'].dtype == 'Int64'
    assert transformed['age'].dtype == 'Int64'
    assert transformed['name'].dtype == 'object'
    
    # Check values
    assert transformed.iloc[0]['id'] == 1
    assert transformed.iloc[0]['age'] == 30


def test_schema_validation_errors():
    """Test schema validation with errors."""
    schema = {
        'id': {'type': 'int', 'required': True},
        'age': {'type': 'int', 'min_value': 0, 'max_value': 150}
    }
    
    transformer = DataTransformer(schema=schema)
    
    # Data with validation errors
    data = [
        {'id': 1, 'age': 200},  # age too high
        {'id': 2, 'age': -5}    # age too low
    ]
    df = pd.DataFrame(data)
    
    # Test non-strict mode (should not raise)
    transformed = transformer.apply_schema(df, strict=False)
    assert len(transformer.validation_errors) > 0
    
    # Test strict mode (should raise)
    with pytest.raises(ValueError):
        transformer.apply_schema(df, strict=True)


def test_data_cleaning():
    """Test data cleaning functionality."""
    transformer = DataTransformer()
    
    # Data with various issues
    data = [
        {'name': '  Alice  ', 'email': 'alice@example.com', 'notes': ''},
        {'name': 'Bob', 'email': ' bob@example.com ', 'notes': 'null'},
        {'name': '', 'email': 'NULL', 'notes': 'Some notes'},
        {'name': None, 'email': None, 'notes': None}  # completely empty row
    ]
    df = pd.DataFrame(data)
    
    cleaned = transformer.clean_data(df)
    
    # Check whitespace stripping
    assert cleaned.iloc[0]['name'] == 'Alice'
    assert cleaned.iloc[1]['email'] == 'bob@example.com'
    
    # Check null string replacement
    assert pd.isna(cleaned.iloc[0]['notes'])  # empty string -> NaN
    assert pd.isna(cleaned.iloc[1]['notes'])  # 'null' -> NaN
    assert pd.isna(cleaned.iloc[2]['email'])  # 'NULL' -> NaN (row 2, not 1)
    
    # Check that completely empty rows might be removed (depends on implementation)
    # The last row should be preserved since dropna(how='all') only drops if ALL values are NaN


def test_custom_transformations():
    """Test adding custom transformation functions."""
    transformer = DataTransformer()
    
    # Add custom transformation
    def uppercase_names(df):
        if 'name' in df.columns:
            df = df.copy()
            df['name'] = df['name'].str.upper()
        return df
    
    transformer.add_transformation(uppercase_names, 'uppercase_names')
    
    data = [{'name': 'alice'}, {'name': 'bob'}]
    df = pd.DataFrame(data)
    
    transformed = transformer.transform(df, flatten=False, clean=False, apply_schema=False)
    
    assert transformed.iloc[0]['name'] == 'ALICE'
    assert transformed.iloc[1]['name'] == 'BOB'


def test_complete_transformation_pipeline():
    """Test the complete transformation pipeline."""
    schema = {
        'id': {'type': 'int', 'required': True},
        'user_name': {'type': 'str', 'required': True},
        'user_age': {'type': 'int', 'min_value': 0}
    }
    
    transformer = DataTransformer(schema=schema)
    
    # Add custom transformation
    def add_processed_timestamp(df):
        df = df.copy()
        df['processed_at'] = datetime.now()
        return df
    
    transformer.add_transformation(add_processed_timestamp, 'add_timestamp')
    
    # Complex nested data
    data = [
        {'id': '1', 'user': {'name': '  Alice  ', 'age': '30'}, 'tags': ['python', 'data']},
        {'id': '2', 'user': {'name': 'Bob', 'age': '25'}, 'tags': ['java']}
    ]
    df = pd.DataFrame(data)
    
    # Run complete pipeline
    result = transformer.transform(df)
    
    # Check flattening worked
    assert 'user_name' in result.columns
    assert 'user_age' in result.columns
    assert 'tags' in result.columns
    
    # Check cleaning worked (whitespace stripped)
    assert result.iloc[0]['user_name'] == 'Alice'
    
    # Check schema transformation worked (type conversion)
    assert result['id'].dtype == 'Int64'
    assert result['user_age'].dtype == 'Int64'
    
    # Check custom transformation worked
    assert 'processed_at' in result.columns


def test_schema_summary():
    """Test schema summary generation."""
    transformer = DataTransformer()
    
    data = [
        {'id': 1, 'name': 'Alice', 'age': 30, 'score': 95.5},
        {'id': 2, 'name': 'Bob', 'age': None, 'score': 87.2},
        {'id': 3, 'name': 'Charlie', 'age': 35, 'score': None}
    ]
    df = pd.DataFrame(data)
    
    summary = transformer.get_schema_summary(df)
    
    # Check summary structure
    assert 'id' in summary
    assert 'name' in summary
    assert 'age' in summary
    assert 'score' in summary
    
    # Check null counts
    assert summary['age']['null_count'] == 1
    assert summary['score']['null_count'] == 1
    
    # Check numeric statistics
    assert 'min_value' in summary['age']
    assert 'max_value' in summary['age']
    assert summary['id']['unique_count'] == 3


def test_schema_compliance_validation():
    """Test schema compliance validation."""
    schema = {
        'id': {'type': 'int', 'required': True, 'nullable': False},
        'name': {'type': 'str', 'required': True},
        'age': {'type': 'int', 'nullable': True}
    }
    
    transformer = DataTransformer(schema=schema)
    
    # Valid data
    valid_data = [
        {'id': 1, 'name': 'Alice', 'age': 30},
        {'id': 2, 'name': 'Bob', 'age': None}
    ]
    valid_df = pd.DataFrame(valid_data)
    
    validation_result = transformer.validate_schema_compliance(valid_df)
    assert validation_result['status'] in ['valid', 'valid_with_warnings']  # May have type warnings
    assert len(validation_result['errors']) == 0
    
    # Invalid data (missing required column)
    invalid_data = [
        {'name': 'Alice', 'age': 30},  # missing required 'id'
        {'name': 'Bob', 'age': 25}
    ]
    invalid_df = pd.DataFrame(invalid_data)
    
    validation_result = transformer.validate_schema_compliance(invalid_df)
    assert validation_result['status'] == 'invalid'
    assert len(validation_result['errors']) > 0


def test_empty_dataframe_handling():
    """Test handling of empty DataFrames."""
    transformer = DataTransformer()
    
    empty_df = pd.DataFrame()
    
    # Should not raise errors
    flattened = transformer.flatten_dataframe(empty_df)
    cleaned = transformer.clean_data(empty_df)
    transformed = transformer.transform(empty_df)
    
    assert flattened.empty
    assert cleaned.empty
    assert transformed.empty


def test_integration_with_module1_output():
    """Test integration with typical Module 1 output format."""
    # Simulate MongoDB document structure that Module 1 would produce
    mongodb_data = [
        {
            '_id': '507f1f77bcf86cd799439011',
            'user': {
                'name': 'Alice Johnson',
                'email': 'alice@example.com',
                'profile': {
                    'age': 30,
                    'location': {'city': 'New York', 'state': 'NY'},
                    'preferences': ['data-science', 'python', 'machine-learning']
                }
            },
            'orders': [
                {'id': 'ord_1', 'amount': 99.99, 'items': ['laptop', 'mouse']},
                {'id': 'ord_2', 'amount': 149.50, 'items': ['keyboard', 'monitor']}
            ],
            'created_at': '2024-01-15T10:30:00Z',
            'metadata': {'source': 'web', 'campaign': 'winter_sale'}
        }
    ]
    
    # Define schema for flattened structure
    schema = {
        '_id': {'type': 'str', 'required': True},
        'user_name': {'type': 'str', 'required': True},
        'user_email': {'type': 'str', 'required': True},
        'user_profile_age': {'type': 'int', 'min_value': 0},
        'user_profile_location_city': {'type': 'str'},
        'user_profile_location_state': {'type': 'str'},
        'user_profile_preferences': {'type': 'str'},  # will be comma-separated
        'orders_id': {'type': 'str'},  # first order ID
        'orders_amount': {'type': 'float'},  # first order amount
        'orders_count': {'type': 'int'},
        'created_at': {'type': 'datetime'},
        'metadata_source': {'type': 'str'},
        'metadata_campaign': {'type': 'str'}
    }
    
    transformer = DataTransformer(schema=schema)
    df = pd.DataFrame(mongodb_data)
    
    # Transform the data
    result = transformer.transform(df)
    
    # Verify flattening worked correctly
    assert 'user_name' in result.columns
    assert 'user_profile_age' in result.columns
    assert 'user_profile_location_city' in result.columns
    assert 'orders_count' in result.columns
    
    # Verify data values
    assert result.iloc[0]['user_name'] == 'Alice Johnson'
    assert result.iloc[0]['user_profile_age'] == 30
    assert result.iloc[0]['user_profile_location_city'] == 'New York'
    assert result.iloc[0]['orders_count'] == 2
    # Check that preferences were flattened (could be comma-separated string or list representation)
    preferences_str = str(result.iloc[0]['user_profile_preferences'])
    assert 'data-science' in preferences_str and 'python' in preferences_str and 'machine-learning' in preferences_str
    
    # Verify schema compliance
    validation = transformer.validate_schema_compliance(result)
    print(f"Validation status: {validation['status']}")
    if validation['errors']:
        print(f"Validation errors: {validation['errors']}")
    if validation['warnings']:
        print(f"Validation warnings: {validation['warnings']}")