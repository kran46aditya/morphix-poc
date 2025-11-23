"""
Schema Generation Module

This module provides functionality to generate schemas from DataFrames
and parse Avro schema files for use in Module 2 data transformation.
"""

import json
import pandas as pd
from typing import Dict, Any, List, Optional, Union
import numpy as np
from datetime import datetime, date
import io
import avro.schema
import fastavro
import pymongo
import os
from pathlib import Path
from ..utils.logging import get_logger

logger = get_logger(__name__)


class SchemaGenerator:
    """Generates schemas from DataFrames or parses Avro schema files."""
    
    # Default metadata directory (configurable via METADATA_BASE env var)
    METADATA_BASE = Path(os.getenv("METADATA_BASE", "/metadata"))
    
    # Mapping from pandas dtypes to our schema types
    DTYPE_MAPPING = {
        'object': 'string',
        'int64': 'integer',
        'int32': 'integer', 
        'int16': 'integer',
        'int8': 'integer',
        'float64': 'float',
        'float32': 'float',
        'bool': 'boolean',
        'datetime64': 'datetime',
        'datetime64[ns]': 'datetime',
        'category': 'string',
    }
    
    @classmethod
    def generate_from_dataframe(cls, df: pd.DataFrame, sample_size: int = 1000, 
                              include_constraints: bool = True, 
                              collection: Optional[str] = None,
                              save_to_metadata: bool = True) -> Dict[str, Any]:
        """Generate schema by analyzing a DataFrame sample.
        
        Args:
            df: Input DataFrame to analyze
            sample_size: Number of rows to sample for analysis
            include_constraints: Whether to include min/max constraints
            collection: Collection name for saving schema (optional)
            save_to_metadata: Whether to save schema to metadata directory
            
        Returns:
            Schema dictionary compatible with DataTransformer
        """
        if df.empty:
            return {}
            
        # Sample the DataFrame if it's larger than sample_size
        if len(df) > sample_size:
            sample_df = df.sample(n=sample_size, random_state=42)
        else:
            sample_df = df
            
        schema = {}
        
        for column in sample_df.columns:
            col_data = sample_df[column]
            col_schema = cls._analyze_column(col_data, include_constraints)
            schema[column] = col_schema
        
        # Save schema to metadata if requested
        if save_to_metadata and collection:
            cls._save_schema_to_metadata(schema, collection)
            
        return schema
    
    @classmethod
    def _compute_schema_hash(cls, schema: Dict[str, Any]) -> str:
        """Compute hash of schema for versioning.
        
        Args:
            schema: Schema dictionary
            
        Returns:
            SHA256 hash of schema
        """
        import hashlib
        schema_str = json.dumps(schema, sort_keys=True, default=str)
        return hashlib.sha256(schema_str.encode('utf-8')).hexdigest()
    
    @classmethod
    def _save_schema_to_metadata(cls, schema: Dict[str, Any], collection: str):
        """Save inferred schema to metadata directory.
        
        Args:
            schema: Schema dictionary to save
            collection: Collection name for directory structure
        """
        try:
            # Create metadata directory structure
            metadata_dir = cls.METADATA_BASE / "schemas" / collection
            metadata_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate timestamp-based filename
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            schema_file = metadata_dir / f"{timestamp}.json"
            
            # Prepare schema document
            schema_doc = {
                "collection": collection,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "schema": schema,
                "field_count": len(schema)
            }
            
            # Write to file
            with open(schema_file, 'w') as f:
                json.dump(schema_doc, f, indent=2, default=str)
            
            logger.info(
                f"Schema saved to {schema_file}",
                extra={
                    'event_type': 'schema_saved',
                    'collection': collection,
                    'schema_file': str(schema_file),
                    'field_count': len(schema)
                }
            )
            
        except Exception as e:
            logger.warning(
                f"Failed to save schema to metadata: {e}",
                exc_info=True,
                extra={
                    'event_type': 'schema_save_error',
                    'collection': collection,
                    'error': str(e)
                }
            )
    
    @classmethod
    def _analyze_column(cls, series: pd.Series, include_constraints: bool = True) -> Dict[str, Any]:
        """Analyze a single column to determine its schema.
        
        Args:
            series: Pandas Series to analyze
            include_constraints: Whether to include min/max constraints
            
        Returns:
            Column schema dictionary
        """
        col_schema = {}
        
        # Determine data type
        dtype_str = str(series.dtype)
        if dtype_str.startswith('datetime'):
            col_schema['type'] = 'datetime'
        else:
            col_schema['type'] = cls.DTYPE_MAPPING.get(dtype_str, 'string')
        
        # Check for null values
        null_count = series.isnull().sum()
        total_count = len(series)
        col_schema['nullable'] = null_count > 0
        col_schema['null_percentage'] = round((null_count / total_count) * 100, 2) if total_count > 0 else 0
        
        # Check for object types that might be nested structures
        if dtype_str == 'object':
            # Sample a few values to determine if it's a dict/list
            sample_values = series.dropna().head(5)
            if len(sample_values) > 0:
                first_val = sample_values.iloc[0]
                if isinstance(first_val, dict):
                    col_schema['type'] = 'object'
                elif isinstance(first_val, list):
                    col_schema['type'] = 'array'
                elif isinstance(first_val, bytes):
                    col_schema['type'] = 'binary'
                # Otherwise keep as 'string'
        
        # Get non-null values for further analysis
        non_null_values = series.dropna()
        
        if len(non_null_values) == 0:
            return col_schema
        
        # Handle complex data types (lists, dicts)
        if len(non_null_values) > 0:
            first_value = non_null_values.iloc[0]
            if isinstance(first_value, list):
                col_schema['type'] = 'array'
                col_schema['is_array'] = True
                if len(first_value) > 0:
                    col_schema['array_item_type'] = type(first_value[0]).__name__
                col_schema['avg_array_length'] = float(non_null_values.apply(lambda x: len(x) if isinstance(x, list) else 0).mean())
                return col_schema
            elif isinstance(first_value, dict):
                col_schema['type'] = 'object'
                col_schema['is_object'] = True
                # Analyze common keys
                all_keys = set()
                for item in non_null_values:
                    if isinstance(item, dict):
                        all_keys.update(item.keys())
                col_schema['common_keys'] = sorted(list(all_keys))
                return col_schema
            
        # Type-specific analysis
        if col_schema['type'] in ['integer', 'float']:
            if include_constraints:
                col_schema['min_value'] = float(non_null_values.min())
                col_schema['max_value'] = float(non_null_values.max())
                col_schema['mean'] = float(non_null_values.mean())
                col_schema['std'] = float(non_null_values.std()) if len(non_null_values) > 1 else 0.0
                
        elif col_schema['type'] == 'string':
            if include_constraints:
                try:
                    # Safe string conversion to handle binary/unicode issues
                    def safe_str_convert(val):
                        try:
                            if isinstance(val, bytes):
                                return val.decode('utf-8', errors='replace')
                            return str(val)
                        except Exception:
                            return str(val)
                    
                    # Convert to string safely
                    str_values = non_null_values.apply(safe_str_convert)
                    str_lengths = str_values.str.len()
                    col_schema['min_length'] = int(str_lengths.min())
                    col_schema['max_length'] = int(str_lengths.max())
                    col_schema['avg_length'] = round(float(str_lengths.mean()), 2)
                except Exception as e:
                    logger.warning(f"Could not analyze string lengths: {e}")
                    # Set defaults
                    col_schema['min_length'] = 0
                    col_schema['max_length'] = 0
                    col_schema['avg_length'] = 0
                
            # Check for common patterns
            try:
                unique_values = non_null_values.nunique()
                if unique_values <= 10:  # Likely categorical
                    col_schema['suggested_values'] = sorted(non_null_values.unique().tolist())
                    col_schema['is_categorical'] = True
                else:
                    col_schema['is_categorical'] = False
            except (TypeError, ValueError):
                # Handle unhashable types
                col_schema['is_categorical'] = False
                
            # Check for common patterns (email, phone, etc.)
            try:
                # Safe string conversion for pattern detection
                def safe_str_convert(val):
                    try:
                        if isinstance(val, bytes):
                            return val.decode('utf-8', errors='replace')
                        return str(val)
                    except Exception:
                        return str(val)
                
                sample_values = non_null_values.head(100).apply(safe_str_convert)
                col_schema['patterns'] = cls._detect_patterns(sample_values)
            except (TypeError, ValueError, UnicodeDecodeError) as e:
                logger.warning(f"Could not detect patterns: {e}")
                col_schema['patterns'] = []
            
        elif col_schema['type'] == 'boolean':
            true_count = non_null_values.sum()
            false_count = len(non_null_values) - true_count
            col_schema['true_count'] = int(true_count)
            col_schema['false_count'] = int(false_count)
            
        elif col_schema['type'] == 'datetime':
            if include_constraints:
                col_schema['min_date'] = str(non_null_values.min())
                col_schema['max_date'] = str(non_null_values.max())
                
        # General statistics
        try:
            col_schema['unique_count'] = int(non_null_values.nunique())
            col_schema['duplicate_count'] = int(len(non_null_values) - non_null_values.nunique())
        except (TypeError, ValueError):
            # Handle unhashable types
            col_schema['unique_count'] = -1  # Unknown
            col_schema['duplicate_count'] = -1  # Unknown
        
        return col_schema
    
    @classmethod
    def _detect_patterns(cls, sample_values: pd.Series) -> List[str]:
        """Detect common patterns in string data.
        
        Args:
            sample_values: Sample of string values
            
        Returns:
            List of detected patterns
        """
        patterns = []
        
        # Email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if sample_values.str.match(email_pattern).any():
            patterns.append('email')
            
        # Phone pattern (simple)
        phone_pattern = r'^\+?[\d\s\-\(\)]{10,}$'
        if sample_values.str.match(phone_pattern).any():
            patterns.append('phone')
            
        # URL pattern
        url_pattern = r'^https?://'
        if sample_values.str.match(url_pattern).any():
            patterns.append('url')
            
        # UUID pattern
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if sample_values.str.match(uuid_pattern, case=False).any():
            patterns.append('uuid')
            
        # Numeric string pattern
        if sample_values.str.match(r'^\d+$').any():
            patterns.append('numeric_string')
            
        return patterns
    
    @classmethod
    def parse_avro_schema(cls, avro_content: Union[str, bytes, Dict[str, Any]]) -> Dict[str, Any]:
        """Parse Avro schema and convert to DataTransformer format.
        
        Args:
            avro_content: Avro schema as JSON string, bytes, or dict
            
        Returns:
            Schema dictionary compatible with DataTransformer
        """
        # Parse the avro schema
        if isinstance(avro_content, (str, bytes)):
            if isinstance(avro_content, bytes):
                avro_content = avro_content.decode('utf-8')
            avro_schema_dict = json.loads(avro_content)
        else:
            avro_schema_dict = avro_content
            
        # Validate it's a proper Avro schema
        try:
            avro.schema.parse(json.dumps(avro_schema_dict))
        except Exception as e:
            raise ValueError(f"Invalid Avro schema: {str(e)}")
            
        # Convert to our schema format
        if avro_schema_dict.get('type') != 'record':
            raise ValueError("Only Avro record schemas are supported")
            
        schema = {}
        fields = avro_schema_dict.get('fields', [])
        
        for field in fields:
            field_name = field['name']
            field_schema = cls._convert_avro_field(field)
            schema[field_name] = field_schema
            
        return schema
    
    @classmethod
    def _convert_avro_field(cls, avro_field: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single Avro field to our schema format.
        
        Args:
            avro_field: Avro field definition
            
        Returns:
            Field schema dictionary
        """
        field_schema = {}
        avro_type = avro_field['type']
        
        # Handle union types (nullable fields)
        if isinstance(avro_type, list):
            # Check if it's a nullable union (contains 'null')
            non_null_types = [t for t in avro_type if t != 'null']
            if 'null' in avro_type:
                field_schema['nullable'] = True
            else:
                field_schema['nullable'] = False
                
            if len(non_null_types) == 1:
                avro_type = non_null_types[0]
            else:
                # Multiple non-null types - use the first one as primary
                avro_type = non_null_types[0]
                field_schema['union_types'] = non_null_types
        else:
            field_schema['nullable'] = False
            
        # Convert Avro types to our types
        if isinstance(avro_type, str):
            type_mapping = {
                'null': 'string',
                'boolean': 'boolean',
                'int': 'integer',
                'long': 'integer',
                'float': 'float',
                'double': 'float',
                'bytes': 'string',
                'string': 'string',
            }
            field_schema['type'] = type_mapping.get(avro_type, 'string')
        elif isinstance(avro_type, dict):
            if avro_type.get('type') == 'array':
                field_schema['type'] = 'array'
                field_schema['items_type'] = cls._get_avro_type_name(avro_type.get('items', 'string'))
            elif avro_type.get('type') == 'map':
                field_schema['type'] = 'object'
                field_schema['values_type'] = cls._get_avro_type_name(avro_type.get('values', 'string'))
            elif avro_type.get('type') == 'record':
                field_schema['type'] = 'object'
                field_schema['nested_schema'] = {}
                for nested_field in avro_type.get('fields', []):
                    nested_name = nested_field['name']
                    field_schema['nested_schema'][nested_name] = cls._convert_avro_field(nested_field)
            elif avro_type.get('type') == 'enum':
                field_schema['type'] = 'string'
                field_schema['enum_values'] = avro_type.get('symbols', [])
            else:
                field_schema['type'] = 'string'
        else:
            field_schema['type'] = 'string'
            
        # Add field documentation if available
        if 'doc' in avro_field:
            field_schema['description'] = avro_field['doc']
            
        # Add default value if available
        if 'default' in avro_field:
            field_schema['default'] = avro_field['default']
            
        return field_schema
    
    @classmethod
    def _get_avro_type_name(cls, avro_type: Union[str, Dict[str, Any]]) -> str:
        """Get the type name from an Avro type definition.
        
        Args:
            avro_type: Avro type definition
            
        Returns:
            Type name string
        """
        if isinstance(avro_type, str):
            return avro_type
        elif isinstance(avro_type, dict):
            return avro_type.get('type', 'string')
        else:
            return 'string'
    
    @classmethod
    def infer_from_mongodb(
        cls,
        mongo_uri: str,
        database: str,
        collection: str,
        sample_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Infer schema from MongoDB collection sample.
        
        Enhanced detection:
        1. Sample random documents
        2. Detect nested objects (depth 5+)
        3. Identify array patterns:
           - Array of primitives → suggest aggregation
           - Array of objects → suggest flatten + explode
        4. Detect field name patterns:
           - CamelCase → suggest snake_case
           - Nested dots → suggest flatten path
        5. Suggest data types with confidence scores
        6. Flag quality issues (high null %, inconsistent types)
        
        Args:
            mongo_uri: MongoDB connection URI
            database: Database name
            collection: Collection name
            sample_size: Number of documents to sample
            
        Returns:
            Dictionary with schema, suggestions, and quality flags
        """
        try:
            client = pymongo.MongoClient(mongo_uri)
            db = client[database]
            coll = db[collection]
            
            # Sample random documents
            pipeline = [{"$sample": {"size": sample_size}}]
            sample_docs = list(coll.aggregate(pipeline))
            
            if not sample_docs:
                return {
                    "schema": {},
                    "suggestions": [],
                    "quality_flags": ["No documents found in collection"]
                }
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame(sample_docs)
            
            # Generate base schema (and save to metadata)
            schema = cls.generate_from_dataframe(
                df, 
                sample_size=len(df),
                collection=collection,
                save_to_metadata=True
            )
            
            # Analyze nested structures
            suggestions = cls.suggest_flattening_strategy(schema)
            
            # Detect quality issues
            quality_flags = cls._detect_quality_issues(df, schema)
            
            # Calculate complexity score
            complexity_score = cls._calculate_complexity_score(schema, suggestions)
            
            client.close()
            
            return {
                "schema": schema,
                "suggestions": suggestions,
                "quality_flags": quality_flags,
                "complexity_score": complexity_score
            }
            
        except Exception as e:
            logger.error(f"Error inferring schema from MongoDB: {e}")
            return {
                "schema": {},
                "suggestions": [],
                "quality_flags": [f"Error: {str(e)}"],
                "complexity_score": 0
            }
    
    @classmethod
    def suggest_flattening_strategy(cls, schema: Dict) -> List[Dict]:
        """
        Analyze nested structures and suggest flattening.
        
        Strategies:
        - Simple nested object: Flatten to dot notation
        - Array of objects: Explode to separate rows OR aggregate to JSON string
        - Deep nesting (5+): Suggest keeping as JSON column
        
        Args:
            schema: Inferred schema
            
        Returns:
            List of flattening suggestions
        """
        suggestions = []
        
        for field_name, field_schema in schema.items():
            if field_schema.get('is_object'):
                depth = cls._calculate_nesting_depth(field_schema)
                
                if depth >= 5:
                    suggestions.append({
                        "field": field_name,
                        "type": "deep_nested",
                        "suggestion": f"Keep '{field_name}' as JSON column (depth: {depth})",
                        "confidence": 0.9
                    })
                else:
                    suggestions.append({
                        "field": field_name,
                        "type": "nested_object",
                        "suggestion": f"Flatten '{field_name}' to dot notation (e.g., {field_name}_subfield)",
                        "confidence": 0.85
                    })
            
            elif field_schema.get('is_array'):
                item_type = field_schema.get('array_item_type', 'unknown')
                
                if item_type == 'dict':
                    suggestions.append({
                        "field": field_name,
                        "type": "nested_array",
                        "suggestion": f"Explode '{field_name}' to separate rows OR aggregate to JSON string",
                        "confidence": 0.8
                    })
                else:
                    suggestions.append({
                        "field": field_name,
                        "type": "array_primitive",
                        "suggestion": f"Aggregate '{field_name}' (array of {item_type})",
                        "confidence": 0.75
                    })
            
            # Check for CamelCase
            if any(c.isupper() for c in field_name[1:]):
                suggestions.append({
                    "field": field_name,
                    "type": "naming",
                    "suggestion": f"Convert '{field_name}' from CamelCase to snake_case",
                    "confidence": 0.7
                })
        
        return suggestions
    
    @classmethod
    def _calculate_nesting_depth(cls, schema: Dict, current_depth: int = 0) -> int:
        """Calculate maximum nesting depth of a schema."""
        if not isinstance(schema, dict):
            return current_depth
        
        if 'nested_schema' in schema:
            max_depth = current_depth
            for nested_field in schema['nested_schema'].values():
                depth = cls._calculate_nesting_depth(nested_field, current_depth + 1)
                max_depth = max(max_depth, depth)
            return max_depth
        
        return current_depth
    
    @classmethod
    def _detect_quality_issues(cls, df: pd.DataFrame, schema: Dict) -> List[str]:
        """Detect quality issues in the data."""
        flags = []
        
        for col_name, col_schema in schema.items():
            if col_name not in df.columns:
                continue
            
            # High null percentage
            null_pct = col_schema.get('null_percentage', 0)
            if null_pct > 50:
                flags.append(f"High null percentage in '{col_name}': {null_pct}%")
            
            # Inconsistent types (would need more analysis)
            # This is a simplified check
            
        return flags
    
    @classmethod
    def _calculate_complexity_score(cls, schema: Dict, suggestions: List[Dict]) -> float:
        """Calculate complexity score (0-100)."""
        score = 0.0
        
        # Base complexity from number of fields
        num_fields = len(schema)
        score += min(num_fields * 2, 40)  # Max 40 points
        
        # Nested structures add complexity
        nested_count = sum(1 for s in suggestions if s['type'] in ['nested_object', 'nested_array', 'deep_nested'])
        score += nested_count * 10  # 10 points per nested structure
        
        # Cap at 100
        return min(score, 100.0)
    
    @classmethod
    def diff_schemas(cls, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Compute differences between two schemas.
        
        Args:
            old_schema: Previous schema dictionary
            new_schema: New schema dictionary
            
        Returns:
            List of diff entries with action, field, old_type, new_type
        """
        diffs = []
        
        old_fields = set(old_schema.keys())
        new_fields = set(new_schema.keys())
        
        # Fields added
        for field in new_fields - old_fields:
            diffs.append({
                "action": "add",
                "field": field,
                "old_type": None,
                "new_type": new_schema[field].get('type', 'unknown')
            })
        
        # Fields removed
        for field in old_fields - new_fields:
            diffs.append({
                "action": "remove",
                "field": field,
                "old_type": old_schema[field].get('type', 'unknown'),
                "new_type": None
            })
        
        # Fields with type changes
        for field in old_fields & new_fields:
            old_field_schema = old_schema[field]
            new_field_schema = new_schema[field]
            
            old_type = old_field_schema.get('type', 'unknown')
            new_type = new_field_schema.get('type', 'unknown')
            
            if old_type != new_type:
                diffs.append({
                    "action": "type_change",
                    "field": field,
                    "old_type": old_type,
                    "new_type": new_type
                })
        
        return diffs
    
    @classmethod
    def detect_breaking_changes(
        cls,
        old_schema: Dict,
        new_schema: Dict
    ) -> Dict[str, Any]:
        """
        Compare schemas for breaking changes.
        
        Breaking:
        - Field removed
        - Type changed (string→int)
        - Non-nullable → nullable
        - Nested structure changed
        
        Non-breaking:
        - Field added
        - Type widened (int→float)
        - Nullable → non-nullable
        
        Args:
            old_schema: Previous schema
            new_schema: New schema
            
        Returns:
            Dictionary with breaking changes and compatibility info
        """
        breaking_changes = []
        non_breaking_changes = []
        
        old_fields = set(old_schema.keys())
        new_fields = set(new_schema.keys())
        
        # Fields removed (breaking)
        removed_fields = old_fields - new_fields
        for field in removed_fields:
            breaking_changes.append({
                "field": field,
                "type": "field_removed",
                "message": f"Field '{field}' was removed"
            })
        
        # Fields added (non-breaking)
        added_fields = new_fields - old_fields
        for field in added_fields:
            non_breaking_changes.append({
                "field": field,
                "type": "field_added",
                "message": f"Field '{field}' was added"
            })
        
        # Check existing fields
        common_fields = old_fields & new_fields
        for field in common_fields:
            old_field_schema = old_schema[field]
            new_field_schema = new_schema[field]
            
            # Type changes
            old_type = old_field_schema.get('type')
            new_type = new_field_schema.get('type')
            
            if old_type != new_type:
                # Check if it's a breaking change
                type_widening = {
                    ('integer', 'float'): False,  # Non-breaking
                    ('integer', 'string'): True,   # Breaking
                    ('float', 'string'): True,    # Breaking
                }
                
                is_breaking = type_widening.get((old_type, new_type), True)
                
                change = {
                    "field": field,
                    "type": "type_changed",
                    "message": f"Type changed from {old_type} to {new_type}",
                    "old_type": old_type,
                    "new_type": new_type
                }
                
                if is_breaking:
                    breaking_changes.append(change)
                else:
                    non_breaking_changes.append(change)
            
            # Nullability changes
            old_nullable = old_field_schema.get('nullable', False)
            new_nullable = new_field_schema.get('nullable', False)
            
            if old_nullable != new_nullable:
                if not old_nullable and new_nullable:
                    # Non-nullable → nullable (breaking)
                    breaking_changes.append({
                        "field": field,
                        "type": "nullability_changed",
                        "message": f"Field '{field}' became nullable"
                    })
                else:
                    # Nullable → non-nullable (non-breaking)
                    non_breaking_changes.append({
                        "field": field,
                        "type": "nullability_changed",
                        "message": f"Field '{field}' became non-nullable"
                    })
        
        return {
            "breaking_changes": breaking_changes,
            "non_breaking_changes": non_breaking_changes,
            "has_breaking_changes": len(breaking_changes) > 0,
            "compatible": len(breaking_changes) == 0
        }
    
    @classmethod
    def schema_to_avro(cls, schema: Dict[str, Any], record_name: str = "GeneratedRecord", 
                      namespace: str = "com.morphix.etl") -> Dict[str, Any]:
        """Convert our schema format to Avro schema format.
        
        Args:
            schema: Our schema dictionary
            record_name: Name for the Avro record
            namespace: Namespace for the Avro schema
            
        Returns:
            Avro schema dictionary
        """
        avro_schema = {
            "type": "record",
            "name": record_name,
            "namespace": namespace,
            "fields": []
        }
        
        for field_name, field_config in schema.items():
            avro_field = cls._convert_to_avro_field(field_name, field_config)
            avro_schema["fields"].append(avro_field)
            
        return avro_schema
    
    @classmethod
    def _convert_to_avro_field(cls, field_name: str, field_config: Dict[str, Any]) -> Dict[str, Any]:
        """Convert our field schema to Avro field format.
        
        Args:
            field_name: Field name
            field_config: Our field schema configuration
            
        Returns:
            Avro field dictionary
        """
        avro_field = {
            "name": field_name
        }
        
        # Convert type
        field_type = field_config.get('type', 'string')
        type_mapping = {
            'string': 'string',
            'integer': 'long',
            'float': 'double',
            'boolean': 'boolean',
            'datetime': 'string',  # Avro doesn't have native datetime
            'array': 'string',     # Simplified for now
            'object': 'string'     # Simplified for now
        }
        
        avro_type = type_mapping.get(field_type, 'string')
        
        # Handle nullable fields
        if field_config.get('nullable', False):
            avro_field['type'] = ['null', avro_type]
        else:
            avro_field['type'] = avro_type
            
        # Add description if available
        if 'description' in field_config:
            avro_field['doc'] = field_config['description']
            
        # Add default value if available
        if 'default' in field_config:
            avro_field['default'] = field_config['default']
            
        return avro_field