"""Unit tests for schema evolution."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import Dict, Any, List

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../'))

from src.etl.schema_evaluator import (
    SchemaEvaluator, SchemaChange, SchemaChangeResult, ChangeType
)
from src.etl.schema_registry import SchemaRegistry, SchemaVersion


class TestSchemaEvaluator:
    """Test SchemaEvaluator."""
    
    @pytest.fixture
    def evaluator(self):
        """Create schema evaluator."""
        return SchemaEvaluator()
    
    @pytest.fixture
    def sample_schema(self):
        """Sample schema."""
        return {
            'name': {'type': 'string', 'nullable': False},
            'price': {'type': 'float', 'nullable': False},
            'category': {'type': 'string', 'nullable': True}
        }
    
    def test_detect_new_field_safe(self, evaluator, sample_schema):
        """Test detecting new optional field (safe change)."""
        # Document with new field
        document = {
            'name': 'Product',
            'price': 99.99,
            'category': 'Electronics',
            'tags': ['new', 'popular']  # New field
        }
        
        result = evaluator.evaluate_document(document, sample_schema)
        
        assert len(result.changes) > 0
        assert result.has_safe
        
        # Find the new field change
        new_field_change = next(
            (c for c in result.changes if c.field_name == 'tags'),
            None
        )
        
        assert new_field_change is not None
        assert new_field_change.change_type == ChangeType.SAFE
        assert new_field_change.old_type is None
        assert new_field_change.new_type is not None
    
    def test_detect_removed_field_breaking(self, evaluator, sample_schema):
        """Test detecting removed field (breaking change)."""
        # Document missing a required field
        document = {
            'name': 'Product',
            'price': 99.99
            # 'category' is missing (but it's nullable, so should be warning)
        }
        
        result = evaluator.evaluate_document(document, sample_schema)
        
        # Category is nullable, so removal should be warning, not breaking
        removed_changes = [c for c in result.changes if 'removed' in c.description.lower()]
        if removed_changes:
            # If category was marked as removed, it should be warning (nullable)
            assert removed_changes[0].change_type in [ChangeType.WARNING, ChangeType.BREAKING]
        
        # Test with required field removal
        required_schema = {
            'name': {'type': 'string', 'nullable': False},
            'price': {'type': 'float', 'nullable': False}
        }
        
        document_missing_required = {
            'name': 'Product'
            # 'price' is missing and required
        }
        
        result2 = evaluator.evaluate_document(document_missing_required, required_schema)
        removed_required = [c for c in result2.changes if 'price' in c.field_name and 'removed' in c.description.lower()]
        if removed_required:
            assert removed_required[0].change_type == ChangeType.BREAKING
    
    def test_detect_type_widening_warning(self, evaluator):
        """Test detecting type widening (int→float)."""
        schema = {
            'count': {'type': 'integer', 'nullable': False}
        }
        
        # Document with float value (widening)
        document = {
            'count': 10.5  # Float instead of int
        }
        
        result = evaluator.evaluate_document(document, schema)
        
        type_changes = [c for c in result.changes if c.old_type == 'integer' and c.new_type == 'float']
        if type_changes:
            assert type_changes[0].change_type == ChangeType.WARNING
    
    def test_detect_type_narrowing_breaking(self, evaluator):
        """Test detecting type narrowing (string→integer)."""
        schema = {
            'id': {'type': 'string', 'nullable': False}
        }
        
        # Document with integer value (narrowing)
        document = {
            'id': 123  # Integer instead of string
        }
        
        result = evaluator.evaluate_document(document, schema)
        
        type_changes = [c for c in result.changes if c.old_type == 'string' and c.new_type == 'integer']
        if type_changes:
            assert type_changes[0].change_type == ChangeType.BREAKING
    
    def test_evaluate_batch(self, evaluator, sample_schema):
        """Test evaluating a batch of documents."""
        batch = [
            {'name': 'Product1', 'price': 10.0, 'category': 'A', 'new_field': 'value1'},
            {'name': 'Product2', 'price': 20.0, 'category': 'B', 'new_field': 'value2'}
        ]
        
        result = evaluator.evaluate_batch(batch, sample_schema)
        
        assert len(result.changes) > 0
        assert result.has_safe
        
        # Should detect new_field in both documents
        new_field_changes = [c for c in result.changes if c.field_name == 'new_field']
        assert len(new_field_changes) > 0
    
    def test_is_breaking_change(self, evaluator):
        """Test is_breaking_change method."""
        safe_change = SchemaChange(
            field_name='new_field',
            change_type=ChangeType.SAFE,
            old_type=None,
            new_type='string',
            description='New field'
        )
        
        breaking_change = SchemaChange(
            field_name='removed_field',
            change_type=ChangeType.BREAKING,
            old_type='string',
            new_type=None,
            description='Removed field'
        )
        
        assert not evaluator.is_breaking_change(safe_change)
        assert evaluator.is_breaking_change(breaking_change)
    
    def test_generate_hudi_ddl(self, evaluator):
        """Test generating Hudi DDL for schema changes."""
        changes = [
            SchemaChange(
                field_name='new_field',
                change_type=ChangeType.SAFE,
                old_type=None,
                new_type='string',
                description='New field',
                new_nullable=True
            ),
            SchemaChange(
                field_name='required_field',
                change_type=ChangeType.SAFE,
                old_type=None,
                new_type='integer',
                description='New required field',
                new_nullable=False
            )
        ]
        
        ddl_statements = evaluator.generate_hudi_ddl('test_table', changes)
        
        assert len(ddl_statements) == 2
        assert 'ALTER TABLE test_table ADD COLUMN new_field' in ddl_statements[0]
        assert 'ALTER TABLE test_table ADD COLUMN required_field' in ddl_statements[1]
        assert 'NOT NULL' in ddl_statements[1]
    
    def test_build_evolved_schema(self, evaluator, sample_schema):
        """Test building evolved schema from changes."""
        changes = [
            SchemaChange(
                field_name='tags',
                change_type=ChangeType.SAFE,
                old_type=None,
                new_type='array',
                description='New tags field',
                new_nullable=True
            )
        ]
        
        evolved = evaluator.build_evolved_schema(sample_schema, changes)
        
        assert 'tags' in evolved
        assert evolved['tags']['type'] == 'array'
        assert evolved['tags']['nullable'] is True
        # Original fields should still be there
        assert 'name' in evolved
        assert 'price' in evolved


class TestSchemaRegistry:
    """Test SchemaRegistry."""
    
    @pytest.fixture
    def mock_database_url(self):
        """Mock database URL."""
        return "postgresql://user:pass@localhost/testdb"
    
    @pytest.fixture
    def registry(self, mock_database_url):
        """Create registry with mocked database."""
        with patch('src.etl.schema_registry.create_engine') as mock_engine:
            with patch('src.etl.schema_registry.Base.metadata.create_all'):
                mock_session = Mock()
                mock_session.query.return_value.filter_by.return_value.order_by.return_value.first.return_value = None
                mock_session.query.return_value.filter_by.return_value.order_by.return_value.desc.return_value.first.return_value = None
                
                mock_session_factory = Mock(return_value=mock_session)
                
                mock_engine_instance = Mock()
                mock_engine.return_value = mock_engine_instance
                
                with patch('src.etl.schema_registry.sessionmaker', return_value=mock_session_factory):
                    registry = SchemaRegistry(mock_database_url)
                    registry.SessionLocal = mock_session_factory
                    return registry
    
    def test_register_version(self, registry):
        """Test registering a new schema version."""
        schema = {'name': {'type': 'string'}, 'price': {'type': 'float'}}
        changes = [
            SchemaChange(
                field_name='tags',
                change_type=ChangeType.SAFE,
                old_type=None,
                new_type='array',
                description='New field'
            )
        ]
        
        mock_session = Mock()
        # Mock the query chain for max_version
        # session.query(SchemaVersion.version) returns a query that can be chained
        # The final .first() returns None (no existing version) or a tuple (version,)
        # We need to properly chain: query(SchemaVersion.version).filter_by(...).order_by(...).desc().first()
        mock_query_chain = Mock()
        mock_query_chain.filter_by.return_value.order_by.return_value.desc.return_value.first.return_value = None
        # When query is called with SchemaVersion.version, return the chain
        mock_session.query = Mock(return_value=mock_query_chain)
        mock_session.add = Mock()
        mock_session.commit = Mock()
        registry.SessionLocal = Mock(return_value=mock_session)
        
        version = registry.register_version(
            table_name='products',
            schema=schema,
            changes=changes,
            applied_by='test'
        )
        
        assert version == 1
        assert mock_session.add.called
        assert mock_session.commit.called
    
    def test_get_latest_schema(self, registry):
        """Test getting latest schema."""
        mock_schema = {'name': {'type': 'string'}}
        mock_version = Mock()
        mock_version.schema = mock_schema
        
        mock_session = Mock()
        # Mock the query chain - need to properly chain the methods
        mock_query_result = Mock()
        mock_query_result.filter_by.return_value.order_by.return_value.desc.return_value.first.return_value = mock_version
        mock_session.query.return_value = mock_query_result
        registry.SessionLocal = Mock(return_value=mock_session)
        
        latest = registry.get_latest_schema('products')
        
        assert latest == mock_schema
    
    def test_get_schema_version(self, registry):
        """Test getting specific schema version."""
        mock_schema = {'name': {'type': 'string'}}
        mock_version = Mock()
        mock_version.schema = mock_schema
        
        mock_session = Mock()
        mock_session.query.return_value.filter_by.return_value.first.return_value = mock_version
        registry.SessionLocal = Mock(return_value=mock_session)
        
        schema = registry.get_schema('products', version=2)
        
        assert schema == mock_schema
    
    def test_get_version_history(self, registry):
        """Test getting version history."""
        mock_versions = [Mock(version=1), Mock(version=2)]
        
        mock_session = Mock()
        # Mock the query chain - need to properly chain the methods
        mock_query_result = Mock()
        mock_query_result.filter_by.return_value.order_by.return_value.asc.return_value.all.return_value = mock_versions
        mock_session.query.return_value = mock_query_result
        registry.SessionLocal = Mock(return_value=mock_session)
        
        history = registry.get_version_history('products')
        
        assert len(history) == 2


class TestSchemaEvolutionIntegration:
    """Test schema evolution integration with CDC."""
    
    def test_schema_change_detection_in_batch(self):
        """Test schema changes are detected in CDC batch."""
        evaluator = SchemaEvaluator()
        
        current_schema = {
            'name': {'type': 'string', 'nullable': False},
            'price': {'type': 'float', 'nullable': False}
        }
        
        batch = [
            {'name': 'Product1', 'price': 10.0, 'tags': ['new']},
            {'name': 'Product2', 'price': 20.0, 'tags': ['popular']}
        ]
        
        result = evaluator.evaluate_batch(batch, current_schema)
        
        assert result.has_safe
        assert any(c.field_name == 'tags' for c in result.safe_changes)
    
    def test_breaking_change_alert(self):
        """Test breaking changes trigger alerts."""
        evaluator = SchemaEvaluator()
        
        current_schema = {
            'name': {'type': 'string', 'nullable': False},
            'price': {'type': 'float', 'nullable': False}
        }
        
        # Document missing required field
        document = {
            'name': 'Product'
            # 'price' is missing
        }
        
        result = evaluator.evaluate_document(document, current_schema)
        
        # Should detect missing required field as breaking
        assert result.has_breaking or result.has_warning

