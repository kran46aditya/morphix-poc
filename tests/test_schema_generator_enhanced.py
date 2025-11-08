"""
Unit tests for enhanced schema generator features (MongoDB inference, flattening, breaking changes).
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.etl.schema_generator import SchemaGenerator


class TestSchemaGeneratorEnhanced:
    """Test cases for enhanced schema generator features."""
    
    def test_suggest_flattening_strategy_simple(self):
        """Test flattening strategy suggestion for simple nested structure."""
        schema = {
            'id': {'type': 'integer'},
            'user': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'email': {'type': 'string'}
                }
            }
        }
        
        suggestions = SchemaGenerator.suggest_flattening_strategy(schema)
        
        assert isinstance(suggestions, list)
        assert len(suggestions) > 0
        assert any('user' in s['field'] for s in suggestions)
    
    def test_suggest_flattening_strategy_array(self):
        """Test flattening strategy for array fields."""
        schema = {
            'id': {'type': 'integer'},
            'tags': {
                'type': 'array',
                'items': {'type': 'string'}
            }
        }
        
        suggestions = SchemaGenerator.suggest_flattening_strategy(schema)
        
        assert isinstance(suggestions, list)
        # Should suggest aggregation for arrays
        array_suggestions = [s for s in suggestions if 'tags' in s['field']]
        assert len(array_suggestions) > 0
    
    def test_suggest_flattening_strategy_nested(self):
        """Test flattening strategy for deeply nested structures."""
        schema = {
            'id': {'type': 'integer'},
            'metadata': {
                'type': 'object',
                'properties': {
                    'nested': {
                        'type': 'object',
                        'properties': {
                            'deep': {'type': 'string'}
                        }
                    }
                }
            }
        }
        
        suggestions = SchemaGenerator.suggest_flattening_strategy(schema)
        
        assert isinstance(suggestions, list)
        # Should suggest flattening for nested objects
        nested_suggestions = [s for s in suggestions if 'metadata' in s['field']]
        assert len(nested_suggestions) > 0
    
    def test_detect_breaking_changes_type_change(self):
        """Test detection of breaking type changes."""
        old_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'}
        }
        
        new_schema = {
            'id': {'type': 'string'},  # Breaking change
            'name': {'type': 'string'}
        }
        
        changes = SchemaGenerator.detect_breaking_changes(old_schema, new_schema)
        
        assert changes['compatible'] is False
        assert len(changes['breaking_changes']) > 0
        assert any('id' in str(change) for change in changes['breaking_changes'])
    
    def test_detect_breaking_changes_field_removed(self):
        """Test detection of removed fields."""
        old_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'},
            'email': {'type': 'string'}
        }
        
        new_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'}
            # email removed
        }
        
        changes = SchemaGenerator.detect_breaking_changes(old_schema, new_schema)
        
        assert changes['compatible'] is False
        assert len(changes['breaking_changes']) > 0
    
    def test_detect_breaking_changes_field_added(self):
        """Test detection of added fields (non-breaking)."""
        old_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'}
        }
        
        new_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'},
            'email': {'type': 'string'}  # New field
        }
        
        changes = SchemaGenerator.detect_breaking_changes(old_schema, new_schema)
        
        # Adding fields is non-breaking
        assert len(changes['non_breaking_changes']) > 0
        assert changes['compatible'] is True
    
    def test_detect_breaking_changes_nullable_change(self):
        """Test detection of nullable to non-nullable change."""
        old_schema = {
            'id': {'type': 'integer', 'nullable': True},
            'name': {'type': 'string'}
        }
        
        new_schema = {
            'id': {'type': 'integer', 'nullable': False},  # Breaking change
            'name': {'type': 'string'}
        }
        
        changes = SchemaGenerator.detect_breaking_changes(old_schema, new_schema)
        
        assert changes['compatible'] is False
        assert len(changes['breaking_changes']) > 0
    
    def test_detect_breaking_changes_compatible(self):
        """Test compatible schema changes."""
        old_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'}
        }
        
        new_schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string'},
            'email': {'type': 'string'},  # Added
            'age': {'type': 'integer', 'nullable': True}  # Added nullable
        }
        
        changes = SchemaGenerator.detect_breaking_changes(old_schema, new_schema)
        
        assert changes['compatible'] is True
        assert len(changes['non_breaking_changes']) >= 2
    
    @patch('src.etl.schema_generator.pymongo.MongoClient')
    def test_infer_from_mongodb_basic(self, mock_mongo_client):
        """Test MongoDB schema inference with mocked client."""
        # Setup mock
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        
        # Mock sample documents
        mock_collection.find.return_value.limit.return_value = [
            {
                '_id': '1',
                'name': 'Alice',
                'age': 25,
                'email': 'alice@example.com'
            },
            {
                '_id': '2',
                'name': 'Bob',
                'age': 30,
                'email': 'bob@example.com'
            }
        ]
        
        result = SchemaGenerator.infer_from_mongodb(
            mongo_uri="mongodb://localhost:27017",
            database="test_db",
            collection="test_collection",
            sample_size=2
        )
        
        assert isinstance(result, dict)
        assert 'schema' in result
        assert 'metadata' in result
        assert 'quality_issues' in result
    
    @patch('src.etl.schema_generator.pymongo.MongoClient')
    def test_infer_from_mongodb_nested(self, mock_mongo_client):
        """Test MongoDB schema inference with nested documents."""
        # Setup mock
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        mock_db = MagicMock()
        mock_client.__getitem__.return_value = mock_db
        
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        
        # Mock nested documents
        mock_collection.find.return_value.limit.return_value = [
            {
                '_id': '1',
                'user': {
                    'name': 'Alice',
                    'email': 'alice@example.com'
                },
                'tags': ['tag1', 'tag2']
            }
        ]
        
        result = SchemaGenerator.infer_from_mongodb(
            mongo_uri="mongodb://localhost:27017",
            database="test_db",
            collection="test_collection",
            sample_size=1
        )
        
        assert isinstance(result, dict)
        assert 'schema' in result
        # Should detect nested structures
        assert 'nested_objects' in result.get('metadata', {}) or 'arrays' in result.get('metadata', {})
    
    def test_calculate_nesting_depth(self):
        """Test nesting depth calculation."""
        # Simple structure
        schema1 = {'id': {'type': 'integer'}}
        depth1 = SchemaGenerator._calculate_nesting_depth(schema1)
        assert depth1 == 0
        
        # Nested structure
        schema2 = {
            'id': {'type': 'integer'},
            'user': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'}
                }
            }
        }
        depth2 = SchemaGenerator._calculate_nesting_depth(schema2)
        assert depth2 >= 1
    
    def test_detect_quality_issues(self):
        """Test quality issue detection."""
        schema = {
            'id': {'type': 'integer'},
            'name': {'type': 'string', 'nullable': True},
            'email': {'type': 'string'}
        }
        
        issues = SchemaGenerator._detect_quality_issues(schema)
        
        assert isinstance(issues, list)
        # Should detect nullable name as potential issue
    
    def test_calculate_complexity_score(self):
        """Test complexity score calculation."""
        # Simple schema
        schema1 = {'id': {'type': 'integer'}}
        score1 = SchemaGenerator._calculate_complexity_score(schema1)
        assert score1 >= 0
        
        # Complex schema
        schema2 = {
            'id': {'type': 'integer'},
            'user': {
                'type': 'object',
                'properties': {
                    'nested': {
                        'type': 'object',
                        'properties': {
                            'deep': {'type': 'string'}
                        }
                    }
                }
            },
            'tags': {'type': 'array', 'items': {'type': 'string'}}
        }
        score2 = SchemaGenerator._calculate_complexity_score(schema2)
        assert score2 > score1  # More complex should have higher score


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

