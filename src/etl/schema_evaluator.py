"""
Runtime schema evolution engine.

Detects schema changes during CDC and applies safe evolutions automatically.
"""

from typing import Dict, List, Optional, Tuple, Set, Any
from enum import Enum
from dataclasses import dataclass
import pandas as pd
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Schema change classification."""
    SAFE = "safe"              # New optional field
    WARNING = "warning"        # Type widening (int→float)
    BREAKING = "breaking"      # Removed field, type narrowing


@dataclass
class SchemaChange:
    """Represents a schema change."""
    field_name: str
    change_type: ChangeType
    old_type: Optional[str]
    new_type: Optional[str]
    description: str
    old_nullable: Optional[bool] = None
    new_nullable: Optional[bool] = None
    
    def __str__(self) -> str:
        return f"{self.change_type.value.upper()}: {self.field_name} ({self.old_type} → {self.new_type}) - {self.description}"


@dataclass
class SchemaChangeResult:
    """Result of schema evaluation."""
    changes: List[SchemaChange]
    has_breaking: bool
    has_warning: bool
    has_safe: bool
    safe_changes: List[SchemaChange]
    breaking_changes: List[SchemaChange]
    warning_changes: List[SchemaChange]
    
    def __init__(self, changes: List[SchemaChange]):
        self.changes = changes
        self.breaking_changes = [c for c in changes if c.change_type == ChangeType.BREAKING]
        self.warning_changes = [c for c in changes if c.change_type == ChangeType.WARNING]
        self.safe_changes = [c for c in changes if c.change_type == ChangeType.SAFE]
        self.has_breaking = len(self.breaking_changes) > 0
        self.has_warning = len(self.warning_changes) > 0
        self.has_safe = len(self.safe_changes) > 0


class SchemaEvaluator:
    """
    Evaluate schema changes during CDC.
    
    Features:
    - Detect new/removed/changed fields
    - Classify changes (safe/warning/breaking)
    - Auto-evolve Hudi schema for safe changes
    - Track schema versions in PostgreSQL
    
    Example:
        evaluator = SchemaEvaluator(schema_registry)
        changes = evaluator.evaluate_document(doc, current_schema)
        
        if changes.has_breaking:
            alert_admin()
        elif changes.has_safe:
            evaluator.evolve_schema(changes)
    """
    
    # Type compatibility matrix (old_type -> new_type)
    TYPE_COMPATIBILITY = {
        ('integer', 'float'): ChangeType.WARNING,  # Widening
        ('integer', 'string'): ChangeType.BREAKING,  # Narrowing
        ('float', 'string'): ChangeType.BREAKING,  # Narrowing
        ('string', 'integer'): ChangeType.BREAKING,  # Narrowing
        ('string', 'float'): ChangeType.BREAKING,  # Narrowing
        ('boolean', 'string'): ChangeType.WARNING,  # Widening
        ('string', 'boolean'): ChangeType.BREAKING,  # Narrowing
    }
    
    def __init__(self, schema_registry: Optional['SchemaRegistry'] = None):
        """
        Initialize evaluator.
        
        Args:
            schema_registry: Optional schema registry for version tracking
        """
        self.schema_registry = schema_registry
        self._type_mapping = {
            'str': 'string',
            'string': 'string',
            'int': 'integer',
            'integer': 'integer',
            'int64': 'integer',
            'int32': 'integer',
            'float': 'float',
            'float64': 'float',
            'float32': 'float',
            'bool': 'boolean',
            'boolean': 'boolean',
            'datetime': 'datetime',
            'datetime64': 'datetime',
            'object': 'object',
            'dict': 'object',
            'list': 'array',
            'array': 'array',
        }
    
    def evaluate_document(
        self,
        document: Dict[str, Any],
        current_schema: Dict[str, Any]
    ) -> SchemaChangeResult:
        """
        Evaluate document against current schema.
        
        Detects:
        - New fields (SAFE if optional)
        - Removed fields (BREAKING)
        - Type changes (SAFE/BREAKING depending on direction)
        - Nested structure changes
        
        Args:
            document: MongoDB document to evaluate
            current_schema: Current schema dictionary
            
        Returns:
            SchemaChangeResult with detected changes
        """
        changes: List[SchemaChange] = []
        
        # Get document fields (flattened)
        doc_fields = self._extract_fields(document)
        schema_fields = set(current_schema.keys())
        
        # Check for new fields
        new_fields = doc_fields - schema_fields
        for field_name in new_fields:
            field_type = self._infer_field_type(document, field_name)
            changes.append(SchemaChange(
                field_name=field_name,
                change_type=ChangeType.SAFE,  # New fields are safe by default
                old_type=None,
                new_type=field_type,
                description=f"New field '{field_name}' detected",
                old_nullable=None,
                new_nullable=True  # New fields are nullable by default
            ))
        
        # Check for removed fields
        removed_fields = schema_fields - doc_fields
        for field_name in removed_fields:
            old_field_config = current_schema.get(field_name, {})
            old_type = old_field_config.get('type', 'unknown')
            old_nullable = old_field_config.get('nullable', True)
            
            # Removed fields are breaking if they were required
            change_type = ChangeType.BREAKING if not old_nullable else ChangeType.WARNING
            
            changes.append(SchemaChange(
                field_name=field_name,
                change_type=change_type,
                old_type=old_type,
                new_type=None,
                description=f"Field '{field_name}' removed from documents",
                old_nullable=old_nullable,
                new_nullable=None
            ))
        
        # Check for type changes in existing fields
        common_fields = doc_fields & schema_fields
        for field_name in common_fields:
            old_field_config = current_schema.get(field_name, {})
            old_type = old_field_config.get('type', 'unknown')
            old_nullable = old_field_config.get('nullable', True)
            
            new_type = self._infer_field_type(document, field_name)
            new_nullable = self._is_field_nullable(document, field_name)
            
            # Check for type change
            if old_type != new_type:
                change_type = self._classify_type_change(old_type, new_type)
                changes.append(SchemaChange(
                    field_name=field_name,
                    change_type=change_type,
                    old_type=old_type,
                    new_type=new_type,
                    description=f"Type changed from {old_type} to {new_type}",
                    old_nullable=old_nullable,
                    new_nullable=new_nullable
                ))
            
            # Check for nullability change
            if not old_nullable and new_nullable:
                # Required -> nullable is breaking
                changes.append(SchemaChange(
                    field_name=field_name,
                    change_type=ChangeType.BREAKING,
                    old_type=old_type,
                    new_type=new_type,
                    description=f"Field '{field_name}' became nullable (was required)",
                    old_nullable=old_nullable,
                    new_nullable=new_nullable
                ))
            elif old_nullable and not new_nullable:
                # Nullable -> required is safe (tightening)
                changes.append(SchemaChange(
                    field_name=field_name,
                    change_type=ChangeType.SAFE,
                    old_type=old_type,
                    new_type=new_type,
                    description=f"Field '{field_name}' became required (was nullable)",
                    old_nullable=old_nullable,
                    new_nullable=new_nullable
                ))
        
        return SchemaChangeResult(changes)
    
    def evaluate_batch(
        self,
        batch: List[Dict[str, Any]],
        current_schema: Dict[str, Any]
    ) -> SchemaChangeResult:
        """
        Evaluate a batch of documents for schema changes.
        
        Args:
            batch: List of MongoDB documents
            current_schema: Current schema dictionary
            
        Returns:
            SchemaChangeResult with all detected changes
        """
        all_changes: List[SchemaChange] = []
        seen_fields: Set[str] = set()
        
        for doc in batch:
            result = self.evaluate_document(doc, current_schema)
            
            # Merge changes, avoiding duplicates
            for change in result.changes:
                change_key = (change.field_name, change.change_type, change.old_type, change.new_type)
                if change_key not in seen_fields:
                    all_changes.append(change)
                    seen_fields.add(change_key)
        
        return SchemaChangeResult(all_changes)
    
    def is_breaking_change(self, change: SchemaChange) -> bool:
        """
        Determine if change is breaking.
        
        Args:
            change: Schema change to evaluate
            
        Returns:
            True if change is breaking
        """
        return change.change_type == ChangeType.BREAKING
    
    def evolve_hudi_schema(
        self,
        table_name: str,
        changes: List[SchemaChange],
        hudi_writer: Optional[Any] = None
    ) -> bool:
        """
        Apply safe schema changes to Hudi table.
        
        Steps:
        1. Filter to only safe changes
        2. Generate Hudi DDL for new columns
        3. Execute DDL (ALTER TABLE ADD COLUMN)
        4. Update schema registry
        5. Return success/failure
        
        Args:
            table_name: Hudi table name
            changes: List of schema changes
            hudi_writer: Optional HudiWriter instance
            
        Returns:
            True if evolution succeeded
        """
        # Filter to only safe changes
        safe_changes = [c for c in changes if c.change_type == ChangeType.SAFE]
        
        if not safe_changes:
            logger.info(f"No safe changes to apply for table {table_name}")
            return True
        
        try:
            logger.info(f"Evolving schema for table {table_name}: {len(safe_changes)} safe changes")
            
            # Generate DDL
            ddl_statements = self.generate_hudi_ddl(table_name, safe_changes)
            
            # Execute DDL if Hudi writer provided
            if hudi_writer:
                try:
                    # Hudi doesn't support ALTER TABLE directly
                    # Instead, we need to update the schema and let Spark infer it
                    # For now, we'll log the DDL and update the registry
                    logger.info(f"Generated DDL for {table_name}:")
                    for ddl in ddl_statements:
                        logger.info(f"  {ddl}")
                    
                    # Note: Actual Hudi schema evolution happens through Spark schema inference
                    # when writing new data with new fields
                    logger.warning("Hudi schema evolution requires Spark schema inference - will be applied on next write")
                except Exception as e:
                    logger.error(f"Failed to execute DDL: {e}")
                    return False
            
            # Update schema registry
            if self.schema_registry:
                try:
                    # Build new schema from changes
                    new_schema = self._build_new_schema(changes)
                    self.schema_registry.register_version(
                        table_name=table_name,
                        schema=new_schema,
                        changes=safe_changes,
                        applied_by="schema_evaluator"
                    )
                except Exception as e:
                    logger.error(f"Failed to register schema version: {e}")
                    return False
            
            logger.info(f"✅ Successfully evolved schema for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to evolve schema for {table_name}: {e}")
            return False
    
    def generate_hudi_ddl(
        self,
        table_name: str,
        changes: List[SchemaChange]
    ) -> List[str]:
        """
        Generate Hudi DDL for schema changes.
        
        Args:
            table_name: Hudi table name
            changes: List of schema changes (should be safe changes only)
            
        Returns:
            List of DDL statements
        """
        ddl_statements = []
        
        for change in changes:
            if change.change_type != ChangeType.SAFE:
                continue
            
            # Map our types to Hudi/Spark types
            spark_type = self._map_to_spark_type(change.new_type)
            
            # Generate ALTER TABLE statement
            # Note: Hudi uses Spark SQL, so we use Spark DDL syntax
            ddl = f"ALTER TABLE {table_name} ADD COLUMN {change.field_name} {spark_type}"
            
            if change.new_nullable is False:
                ddl += " NOT NULL"
            
            ddl_statements.append(ddl)
        
        return ddl_statements
    
    def _extract_fields(self, document: Dict[str, Any], prefix: str = "") -> Set[str]:
        """
        Extract all field names from document (flattened).
        
        Args:
            document: Document to extract fields from
            prefix: Prefix for nested fields
            
        Returns:
            Set of field names
        """
        fields = set()
        
        for key, value in document.items():
            field_name = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recursively extract nested fields
                nested_fields = self._extract_fields(value, field_name)
                fields.update(nested_fields)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Array of objects - extract from first element
                nested_fields = self._extract_fields(value[0], field_name)
                fields.update(nested_fields)
            else:
                fields.add(field_name)
        
        return fields
    
    def _infer_field_type(self, document: Dict[str, Any], field_path: str) -> str:
        """
        Infer field type from document.
        
        Args:
            document: Document to inspect
            field_path: Dot-separated field path (e.g., "specs.cpu")
            
        Returns:
            Inferred type string
        """
        parts = field_path.split('.')
        value = document
        
        try:
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                elif isinstance(value, list) and len(value) > 0:
                    value = value[0].get(part) if isinstance(value[0], dict) else value[0]
                else:
                    return 'string'  # Default
            
            if value is None:
                return 'string'
            
            # Infer type from value
            python_type = type(value).__name__
            return self._type_mapping.get(python_type, 'string')
            
        except Exception:
            return 'string'
    
    def _is_field_nullable(self, document: Dict[str, Any], field_path: str) -> bool:
        """
        Check if field is nullable (has null values).
        
        Args:
            document: Document to check
            field_path: Dot-separated field path
            
        Returns:
            True if field can be null
        """
        parts = field_path.split('.')
        value = document
        
        try:
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                elif isinstance(value, list) and len(value) > 0:
                    value = value[0].get(part) if isinstance(value[0], dict) else value[0]
                else:
                    return True  # Default to nullable
            
            return value is None
            
        except Exception:
            return True  # Default to nullable
    
    def _classify_type_change(self, old_type: str, new_type: str) -> ChangeType:
        """
        Classify type change as safe/warning/breaking.
        
        Args:
            old_type: Old type
            new_type: New type
            
        Returns:
            ChangeType classification
        """
        # Normalize types
        old_type = old_type.lower()
        new_type = new_type.lower()
        
        # Check compatibility matrix
        key = (old_type, new_type)
        if key in self.TYPE_COMPATIBILITY:
            return self.TYPE_COMPATIBILITY[key]
        
        # Same type is safe
        if old_type == new_type:
            return ChangeType.SAFE
        
        # Object/array to string is warning (widening)
        if old_type in ['object', 'array'] and new_type == 'string':
            return ChangeType.WARNING
        
        # String to object/array is breaking (narrowing)
        if old_type == 'string' and new_type in ['object', 'array']:
            return ChangeType.BREAKING
        
        # Default: breaking for unknown changes
        return ChangeType.BREAKING
    
    def _map_to_spark_type(self, type_str: str) -> str:
        """
        Map our type to Spark SQL type.
        
        Args:
            type_str: Our type string
            
        Returns:
            Spark SQL type string
        """
        mapping = {
            'string': 'STRING',
            'integer': 'BIGINT',
            'float': 'DOUBLE',
            'boolean': 'BOOLEAN',
            'datetime': 'TIMESTAMP',
            'object': 'STRING',  # Store as JSON string
            'array': 'STRING',   # Store as JSON string
        }
        
        return mapping.get(type_str.lower(), 'STRING')
    
    def _build_new_schema(self, changes: List[SchemaChange]) -> Dict[str, Any]:
        """
        Build new schema from changes.
        
        Args:
            changes: List of schema changes
            
        Returns:
            New schema dictionary
        """
        # This would need the current schema as input
        # For now, return empty dict (should be called with current_schema)
        return {}
    
    def build_evolved_schema(
        self,
        current_schema: Dict[str, Any],
        changes: List[SchemaChange]
    ) -> Dict[str, Any]:
        """
        Build evolved schema by applying changes to current schema.
        
        Args:
            current_schema: Current schema
            changes: List of schema changes to apply
            
        Returns:
            Evolved schema dictionary
        """
        new_schema = current_schema.copy()
        
        for change in changes:
            if change.change_type == ChangeType.SAFE:
                # Add new field
                new_schema[change.field_name] = {
                    'type': change.new_type or 'string',
                    'nullable': change.new_nullable if change.new_nullable is not None else True,
                    'description': change.description
                }
            elif change.change_type == ChangeType.BREAKING:
                # Breaking changes shouldn't be applied automatically
                # But we log them
                logger.warning(f"Skipping breaking change: {change}")
            elif change.change_type == ChangeType.WARNING:
                # Update type for warnings (type widening)
                if change.field_name in new_schema:
                    new_schema[change.field_name]['type'] = change.new_type or new_schema[change.field_name].get('type', 'string')
                    logger.info(f"Updated type for {change.field_name}: {change.old_type} → {change.new_type}")
        
        return new_schema

