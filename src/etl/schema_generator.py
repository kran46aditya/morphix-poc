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


class SchemaGenerator:
    """Generates schemas from DataFrames or parses Avro schema files."""
    
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
                              include_constraints: bool = True) -> Dict[str, Any]:
        """Generate schema by analyzing a DataFrame sample.
        
        Args:
            df: Input DataFrame to analyze
            sample_size: Number of rows to sample for analysis
            include_constraints: Whether to include min/max constraints
            
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
            
        return schema
    
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
                str_lengths = non_null_values.astype(str).str.len()
                col_schema['min_length'] = int(str_lengths.min())
                col_schema['max_length'] = int(str_lengths.max())
                col_schema['avg_length'] = round(float(str_lengths.mean()), 2)
                
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
                sample_values = non_null_values.head(100).astype(str)
                col_schema['patterns'] = cls._detect_patterns(sample_values)
            except (TypeError, ValueError):
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