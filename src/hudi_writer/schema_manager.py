"""
Hudi schema management for handling schema evolution and validation.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import json

from .models import HudiTableConfig, HudiTableInfo


class HudiSchemaManager:
    """Manager for Hudi schema operations."""
    
    def __init__(self):
        """Initialize Hudi schema manager."""
        pass
    
    def validate_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a schema for Hudi compatibility.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            Validation result with errors and warnings
        """
        errors = []
        warnings = []
        
        # Check for required fields
        if not schema:
            errors.append("Schema cannot be empty")
            return {"valid": False, "errors": errors, "warnings": warnings}
        
        # Check for record key field
        has_record_key = False
        for field_name, field_config in schema.items():
            if field_config.get('type') in ['string', 'integer']:
                has_record_key = True
                break
        
        if not has_record_key:
            warnings.append("No suitable record key field found. Consider adding an 'id' field.")
        
        # Check for precombine field
        has_timestamp = False
        for field_name, field_config in schema.items():
            if field_config.get('type') == 'datetime':
                has_timestamp = True
                break
        
        if not has_timestamp:
            warnings.append("No timestamp field found. Consider adding an 'updated_at' field for precombine.")
        
        # Validate field types
        for field_name, field_config in schema.items():
            field_type = field_config.get('type')
            if field_type not in ['string', 'integer', 'float', 'boolean', 'datetime', 'array', 'object']:
                errors.append(f"Invalid field type '{field_type}' for field '{field_name}'")
        
        # Check for nested objects (Hudi limitation)
        for field_name, field_config in schema.items():
            if field_config.get('type') == 'object':
                warnings.append(f"Field '{field_name}' is an object type. Consider flattening for better Hudi compatibility.")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    def optimize_schema_for_hudi(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize schema for Hudi best practices.
        
        Args:
            schema: Input schema
            
        Returns:
            Optimized schema
        """
        optimized_schema = schema.copy()
        
        # Ensure we have a record key field
        if not self._has_record_key_field(optimized_schema):
            optimized_schema['id'] = {
                'type': 'string',
                'required': True,
                'nullable': False,
                'description': 'Auto-generated record key'
            }
        
        # Ensure we have a precombine field
        if not self._has_precombine_field(optimized_schema):
            optimized_schema['updated_at'] = {
                'type': 'datetime',
                'required': True,
                'nullable': False,
                'description': 'Auto-generated precombine field'
            }
        
        # Add Hudi-specific metadata
        optimized_schema['_hudi_metadata'] = {
            'type': 'object',
            'required': False,
            'nullable': True,
            'description': 'Hudi internal metadata',
            'hudi_fields': {
                '_hoodie_commit_time': {'type': 'string'},
                '_hoodie_commit_seqno': {'type': 'string'},
                '_hoodie_record_key': {'type': 'string'},
                '_hoodie_partition_path': {'type': 'string'},
                '_hoodie_file_name': {'type': 'string'}
            }
        }
        
        return optimized_schema
    
    def create_hudi_table_config(
        self, 
        table_name: str, 
        schema: Dict[str, Any], 
        base_path: str,
        database: str = "default",
        partition_field: Optional[str] = None
    ) -> HudiTableConfig:
        """Create Hudi table configuration from schema.
        
        Args:
            table_name: Table name
            schema: Schema dictionary
            base_path: Base path for the table
            database: Database name
            partition_field: Optional partition field
            
        Returns:
            Hudi table configuration
        """
        # Optimize schema
        optimized_schema = self.optimize_schema_for_hudi(schema)
        
        # Find record key field
        record_key_field = self._find_record_key_field(optimized_schema)
        
        # Find precombine field
        precombine_field = self._find_precombine_field(optimized_schema)
        
        # Create table configuration
        config = HudiTableConfig(
            table_name=table_name,
            database=database,
            base_path=base_path,
            schema=optimized_schema,
            record_key_field=record_key_field,
            partition_field=partition_field,
            precombine_field=precombine_field
        )
        
        return config
    
    def compare_schemas(self, schema1: Dict[str, Any], schema2: Dict[str, Any]) -> Dict[str, Any]:
        """Compare two schemas for compatibility.
        
        Args:
            schema1: First schema
            schema2: Second schema
            
        Returns:
            Comparison result
        """
        fields1 = set(schema1.keys())
        fields2 = set(schema2.keys())
        
        common_fields = fields1.intersection(fields2)
        added_fields = fields2 - fields1
        removed_fields = fields1 - fields2
        
        # Check type compatibility for common fields
        type_changes = []
        for field in common_fields:
            type1 = schema1[field].get('type')
            type2 = schema2[field].get('type')
            if type1 != type2:
                type_changes.append({
                    'field': field,
                    'old_type': type1,
                    'new_type': type2
                })
        
        # Determine compatibility
        compatible = len(removed_fields) == 0 and len(type_changes) == 0
        
        return {
            'compatible': compatible,
            'common_fields': list(common_fields),
            'added_fields': list(added_fields),
            'removed_fields': list(removed_fields),
            'type_changes': type_changes,
            'total_fields_old': len(fields1),
            'total_fields_new': len(fields2)
        }
    
    def merge_schemas(self, schemas: List[Dict[str, Any]], strategy: str = "union") -> Dict[str, Any]:
        """Merge multiple schemas.
        
        Args:
            schemas: List of schemas to merge
            strategy: Merge strategy ("union", "intersection", "override")
            
        Returns:
            Merged schema
        """
        if not schemas:
            return {}
        
        if len(schemas) == 1:
            return schemas[0]
        
        merged_schema = schemas[0].copy()
        
        for schema in schemas[1:]:
            if strategy == "union":
                # Include all fields from all schemas
                for field_name, field_config in schema.items():
                    if field_name not in merged_schema:
                        merged_schema[field_name] = field_config
                    else:
                        # Merge field configurations
                        merged_config = merged_schema[field_name].copy()
                        merged_config.update(field_config)
                        # Handle nullable - true if either is nullable
                        if merged_schema[field_name].get('nullable') or field_config.get('nullable'):
                            merged_config['nullable'] = True
                        merged_schema[field_name] = merged_config
                        
            elif strategy == "intersection":
                # Only include fields present in all schemas
                common_fields = set(merged_schema.keys()).intersection(set(schema.keys()))
                merged_schema = {field: merged_schema[field] for field in common_fields}
                
            elif strategy == "override":
                # Later schemas override earlier ones
                merged_schema.update(schema)
        
        return merged_schema
    
    def export_schema_to_avro(self, schema: Dict[str, Any], record_name: str = "HudiRecord") -> Dict[str, Any]:
        """Export schema to Avro format.
        
        Args:
            schema: Schema dictionary
            record_name: Avro record name
            
        Returns:
            Avro schema dictionary
        """
        avro_schema = {
            "type": "record",
            "name": record_name,
            "namespace": "com.morphix.hudi",
            "fields": []
        }
        
        for field_name, field_config in schema.items():
            if field_name.startswith('_'):
                continue  # Skip metadata fields
                
            avro_field = {
                "name": field_name,
                "type": self._convert_type_to_avro(field_config.get('type', 'string'))
            }
            
            # Add nullable support
            if field_config.get('nullable', True):
                if isinstance(avro_field['type'], str):
                    avro_field['type'] = ['null', avro_field['type']]
                else:
                    avro_field['type'] = ['null'] + avro_field['type']
            
            # Add description if available
            if 'description' in field_config:
                avro_field['doc'] = field_config['description']
            
            avro_schema['fields'].append(avro_field)
        
        return avro_schema
    
    def _has_record_key_field(self, schema: Dict[str, Any]) -> bool:
        """Check if schema has a suitable record key field."""
        for field_config in schema.values():
            if field_config.get('type') in ['string', 'integer']:
                return True
        return False
    
    def _has_precombine_field(self, schema: Dict[str, Any]) -> bool:
        """Check if schema has a suitable precombine field."""
        for field_config in schema.values():
            if field_config.get('type') == 'datetime':
                return True
        return False
    
    def _find_record_key_field(self, schema: Dict[str, Any]) -> str:
        """Find the best record key field in schema."""
        # Look for common record key field names
        preferred_fields = ['id', 'record_id', 'key', 'uuid']
        
        for field_name in preferred_fields:
            if field_name in schema:
                field_config = schema[field_name]
                if field_config.get('type') in ['string', 'integer']:
                    return field_name
        
        # Look for any string or integer field
        for field_name, field_config in schema.items():
            if field_config.get('type') in ['string', 'integer']:
                return field_name
        
        # Default to 'id'
        return 'id'
    
    def _find_precombine_field(self, schema: Dict[str, Any]) -> str:
        """Find the best precombine field in schema."""
        # Look for common timestamp field names
        preferred_fields = ['updated_at', 'modified_at', 'timestamp', 'created_at', 'last_modified']
        
        for field_name in preferred_fields:
            if field_name in schema:
                field_config = schema[field_name]
                if field_config.get('type') == 'datetime':
                    return field_name
        
        # Look for any datetime field
        for field_name, field_config in schema.items():
            if field_config.get('type') == 'datetime':
                return field_name
        
        # Default to 'updated_at'
        return 'updated_at'
    
    def _convert_type_to_avro(self, field_type: str) -> str:
        """Convert schema type to Avro type."""
        type_mapping = {
            'string': 'string',
            'integer': 'long',
            'float': 'double',
            'boolean': 'boolean',
            'datetime': 'string',  # Avro doesn't have native datetime
            'array': 'string',     # Simplified for now
            'object': 'string'     # Simplified for now
        }
        
        return type_mapping.get(field_type, 'string')
