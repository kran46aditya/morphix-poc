"""
Test suite for schema generation functionality.
"""

import pytest
import pandas as pd
import json
from datetime import datetime
from unittest.mock import patch, MagicMock

from etl.schema_generator import SchemaGenerator
from etl.data_transformer import DataTransformer


class TestSchemaGenerator:
    """Test cases for SchemaGenerator class."""
    
    def test_generate_from_dataframe_basic(self):
        """Test basic schema generation from DataFrame."""
        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'salary': [50000.0, 60000.5, 70000.25, 80000.0, 90000.75],
            'is_active': [True, False, True, True, False],
            'created_at': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01'])
        })
        
        schema = SchemaGenerator.generate_from_dataframe(df)
        
        # Check schema structure
        assert 'id' in schema
        assert 'name' in schema
        assert 'age' in schema
        assert 'salary' in schema
        assert 'is_active' in schema
        assert 'created_at' in schema
        
        # Check field types
        assert schema['id']['type'] == 'integer'
        assert schema['name']['type'] == 'string'
        assert schema['age']['type'] == 'integer'
        assert schema['salary']['type'] == 'float'
        assert schema['is_active']['type'] == 'boolean'
        assert schema['created_at']['type'] == 'datetime'
        
        # Check nullable fields
        assert not schema['id']['nullable']
        assert not schema['name']['nullable']
        
        # Check constraints
        assert 'min_value' in schema['id']
        assert 'max_value' in schema['id']
        assert schema['id']['min_value'] == 1
        assert schema['id']['max_value'] == 5
        
    def test_generate_from_dataframe_with_nulls(self):
        """Test schema generation with null values."""
        df = pd.DataFrame({
            'optional_field': [1, None, 3, None, 5],
            'required_field': [1, 2, 3, 4, 5]
        })
        
        schema = SchemaGenerator.generate_from_dataframe(df)
        
        assert schema['optional_field']['nullable']
        assert schema['optional_field']['null_percentage'] == 40.0
        assert not schema['required_field']['nullable']
        assert schema['required_field']['null_percentage'] == 0.0
        
    def test_generate_from_dataframe_categorical(self):
        """Test detection of categorical fields."""
        df = pd.DataFrame({
            'status': ['active', 'inactive', 'pending', 'active', 'inactive'] * 20,
            'category': ['A', 'B', 'C'] * 33 + ['A']
        })
        
        schema = SchemaGenerator.generate_from_dataframe(df)
        
        # Status should be detected as categorical (few unique values)
        assert schema['status']['is_categorical']
        assert 'suggested_values' in schema['status']
        assert set(schema['status']['suggested_values']) == {'active', 'inactive', 'pending'}
        
    def test_pattern_detection(self):
        """Test pattern detection in string fields."""
        df = pd.DataFrame({
            'email': ['user1@example.com', 'user2@test.org', 'user3@company.net'],
            'phone': ['+1-555-123-4567', '+1-555-987-6543', '+1-555-456-7890'],
            'url': ['https://example.com', 'https://test.org', 'https://company.net'],
            'text': ['some text', 'more text', 'random text']
        })
        
        schema = SchemaGenerator.generate_from_dataframe(df)
        
        assert 'email' in schema['email']['patterns']
        assert 'phone' in schema['phone']['patterns']
        assert 'url' in schema['url']['patterns']
        assert len(schema['text']['patterns']) == 0
        
    def test_parse_avro_schema_basic(self):
        """Test parsing basic Avro schema."""
        avro_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": ["null", "string"], "default": None},
                {"name": "age", "type": "int"}
            ]
        }
        
        schema = SchemaGenerator.parse_avro_schema(avro_schema)
        
        assert 'id' in schema
        assert 'name' in schema
        assert 'email' in schema
        assert 'age' in schema
        
        assert schema['id']['type'] == 'integer'
        assert schema['name']['type'] == 'string'
        assert schema['email']['type'] == 'string'
        assert schema['age']['type'] == 'integer'
        
        assert not schema['id']['nullable']
        assert not schema['name']['nullable']
        assert schema['email']['nullable']
        assert not schema['age']['nullable']
        
    def test_parse_avro_schema_with_complex_types(self):
        """Test parsing Avro schema with complex types."""
        avro_schema = {
            "type": "record",
            "name": "ComplexRecord",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "metadata", "type": {"type": "map", "values": "string"}},
                {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "INACTIVE"]}}
            ]
        }
        
        schema = SchemaGenerator.parse_avro_schema(avro_schema)
        
        assert schema['tags']['type'] == 'array'
        assert schema['tags']['items_type'] == 'string'
        assert schema['metadata']['type'] == 'object'
        assert schema['metadata']['values_type'] == 'string'
        assert schema['status']['type'] == 'string'
        assert schema['status']['enum_values'] == ['ACTIVE', 'INACTIVE']
        
    def test_parse_avro_schema_from_json_string(self):
        """Test parsing Avro schema from JSON string."""
        avro_json = json.dumps({
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        })
        
        schema = SchemaGenerator.parse_avro_schema(avro_json)
        
        assert 'field1' in schema
        assert 'field2' in schema
        assert schema['field1']['type'] == 'string'
        assert schema['field2']['type'] == 'integer'
        
    def test_schema_to_avro_conversion(self):
        """Test converting our schema format to Avro."""
        schema = {
            'id': {'type': 'integer', 'nullable': False},
            'name': {'type': 'string', 'nullable': True},
            'age': {'type': 'integer', 'nullable': False, 'min_value': 0, 'max_value': 150}
        }
        
        avro_schema = SchemaGenerator.schema_to_avro(schema, "TestRecord", "com.test")
        
        assert avro_schema['type'] == 'record'
        assert avro_schema['name'] == 'TestRecord'
        assert avro_schema['namespace'] == 'com.test'
        assert len(avro_schema['fields']) == 3
        
        # Find fields by name
        fields_by_name = {f['name']: f for f in avro_schema['fields']}
        
        assert fields_by_name['id']['type'] == 'long'
        assert fields_by_name['name']['type'] == ['null', 'string']
        assert fields_by_name['age']['type'] == 'long'
        
    def test_invalid_avro_schema(self):
        """Test handling of invalid Avro schema."""
        invalid_schema = {
            "type": "invalid_type",
            "fields": []
        }
        
        with pytest.raises(ValueError, match="Invalid Avro schema"):
            SchemaGenerator.parse_avro_schema(invalid_schema)
            
    def test_non_record_avro_schema(self):
        """Test handling of non-record Avro schema."""
        non_record_schema = {
            "type": "string"
        }
        
        with pytest.raises(ValueError, match="Only Avro record schemas are supported"):
            SchemaGenerator.parse_avro_schema(non_record_schema)


class TestDataTransformerSchemaEnhancements:
    """Test cases for DataTransformer schema enhancements."""
    
    def test_generate_schema_from_dataframe(self):
        """Test schema generation in DataTransformer."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        transformer = DataTransformer()
        schema = transformer.generate_schema_from_dataframe(df)
        
        assert transformer.schema == schema
        assert transformer.schema_source == "dataframe_generated"
        assert 'id' in schema
        assert 'name' in schema
        
    def test_load_schema_from_avro(self):
        """Test loading Avro schema in DataTransformer."""
        avro_schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"}
            ]
        }
        
        transformer = DataTransformer()
        schema = transformer.load_schema_from_avro(avro_schema)
        
        assert transformer.schema == schema
        assert transformer.schema_source == "avro_loaded"
        assert 'field1' in schema
        assert 'field2' in schema
        
    def test_export_schema_to_avro(self):
        """Test exporting schema to Avro format."""
        schema = {
            'id': {'type': 'integer', 'nullable': False},
            'name': {'type': 'string', 'nullable': True}
        }
        
        transformer = DataTransformer(schema=schema)
        avro_schema = transformer.export_schema_to_avro("TestRecord", "com.test")
        
        assert avro_schema['type'] == 'record'
        assert avro_schema['name'] == 'TestRecord'
        assert avro_schema['namespace'] == 'com.test'
        assert len(avro_schema['fields']) == 2
        
    def test_export_schema_to_avro_no_schema(self):
        """Test exporting schema when no schema exists."""
        transformer = DataTransformer()
        
        with pytest.raises(ValueError, match="No schema available to export"):
            transformer.export_schema_to_avro()
            
    def test_get_schema_summary(self):
        """Test getting schema summary."""
        schema = {
            'id': {'type': 'integer', 'nullable': False},
            'name': {'type': 'string', 'nullable': True},
            'age': {'type': 'integer', 'nullable': False}
        }
        
        transformer = DataTransformer(schema=schema)
        transformer.schema_source = "test_source"
        summary = transformer.get_schema_summary()
        
        assert summary['status'] == 'active'
        assert summary['source'] == 'test_source'
        assert summary['field_count'] == 3
        assert summary['nullable_fields'] == 1
        assert summary['field_types']['integer'] == 2
        assert summary['field_types']['string'] == 1
        assert set(summary['fields']) == {'id', 'name', 'age'}
        
    def test_get_schema_summary_no_schema(self):
        """Test getting schema summary when no schema exists."""
        transformer = DataTransformer()
        summary = transformer.get_schema_summary()
        
        assert summary['status'] == 'no_schema'
        assert summary['field_count'] == 0
        
    def test_merge_schemas_union(self):
        """Test merging schemas with union strategy."""
        schema1 = {
            'field1': {'type': 'string', 'nullable': False},
            'field2': {'type': 'integer', 'nullable': True}
        }
        schema2 = {
            'field2': {'type': 'integer', 'nullable': False, 'min_value': 0},
            'field3': {'type': 'float', 'nullable': True}
        }
        
        transformer = DataTransformer(schema=schema1)
        merged = transformer.merge_schemas(schema2, strategy="union")
        
        assert 'field1' in merged
        assert 'field2' in merged
        assert 'field3' in merged
        
        # field2 should be nullable (true wins)
        assert merged['field2']['nullable']
        assert merged['field2']['min_value'] == 0
        
    def test_merge_schemas_intersection(self):
        """Test merging schemas with intersection strategy."""
        schema1 = {
            'field1': {'type': 'string', 'nullable': False},
            'field2': {'type': 'integer', 'nullable': True}
        }
        schema2 = {
            'field2': {'type': 'integer', 'nullable': False, 'min_value': 0},
            'field3': {'type': 'float', 'nullable': True}
        }
        
        transformer = DataTransformer(schema=schema1)
        merged = transformer.merge_schemas(schema2, strategy="intersection")
        
        assert 'field1' not in merged
        assert 'field2' in merged
        assert 'field3' not in merged
        
    def test_merge_schemas_override(self):
        """Test merging schemas with override strategy."""
        schema1 = {
            'field1': {'type': 'string', 'nullable': False},
            'field2': {'type': 'integer', 'nullable': True}
        }
        schema2 = {
            'field2': {'type': 'float', 'nullable': False},
            'field3': {'type': 'boolean', 'nullable': True}
        }
        
        transformer = DataTransformer(schema=schema1)
        merged = transformer.merge_schemas(schema2, strategy="override")
        
        assert 'field1' in merged
        assert 'field2' in merged
        assert 'field3' in merged
        
        # field2 should be overridden
        assert merged['field2']['type'] == 'float'
        assert not merged['field2']['nullable']
        
    def test_merge_schemas_invalid_strategy(self):
        """Test merging schemas with invalid strategy."""
        schema = {'field1': {'type': 'string', 'nullable': False}}
        transformer = DataTransformer(schema=schema)
        
        with pytest.raises(ValueError, match="Unknown merge strategy"):
            transformer.merge_schemas({}, strategy="invalid")