"""
ETL Module 2: Data Transformer

This module handles data transformation, flattening, and schema conformance.
It takes DataFrames from Module 1 and prepares them for downstream processing.
"""

from typing import Dict, Any, List, Optional, Union, Callable
import json
import pandas as pd
from datetime import datetime, date
import re
from .schema_generator import SchemaGenerator
from ..utils.logging import get_logger


class DataTransformer:
    """Second ETL module that transforms, flattens, and validates DataFrame data."""
    
    def __init__(self, schema: Optional[Dict[str, Any]] = None):
        """Initialize the data transformer.
        
        Args:
            schema: Optional schema definition for validation and transformation
        """
        self.schema = schema or {}
        self.transformations = []
        self.validation_errors = []
        self.schema_source = None  # Track where schema came from
        self.logger = get_logger(__name__)
        
    def add_transformation(self, func: Callable[[pd.DataFrame], pd.DataFrame], name: str = None):
        """Add a custom transformation function to the pipeline.
        
        Args:
            func: Function that takes a DataFrame and returns a transformed DataFrame
            name: Optional name for the transformation
        """
        self.transformations.append({
            'function': func,
            'name': name or f'transformation_{len(self.transformations)}'
        })
        
    def flatten_dataframe(self, df: pd.DataFrame, separator: str = '_') -> pd.DataFrame:
        """Flatten nested dictionaries and arrays in DataFrame columns.
        
        Args:
            df: Input DataFrame with potentially nested data
            separator: String to separate nested field names
            
        Returns:
            Flattened DataFrame
        """
        if df.empty:
            return df
            
        flattened_data = []
        
        for _, row in df.iterrows():
            flattened_row = {}
            for col, value in row.items():
                if isinstance(value, dict):
                    # Flatten dictionary
                    flat_dict = self._flatten_dict(value, parent_key=col, separator=separator)
                    flattened_row.update(flat_dict)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    # Flatten array of dictionaries (take first element for schema)
                    if len(value) > 0:
                        flat_dict = self._flatten_dict(value[0], parent_key=col, separator=separator)
                        flattened_row.update(flat_dict)
                        # Store array length
                        flattened_row[f"{col}_count"] = len(value)
                elif isinstance(value, list):
                    # Convert simple arrays to comma-separated strings
                    if value:
                        flattened_row[col] = ','.join(str(v) for v in value)
                        flattened_row[f"{col}_count"] = len(value)
                    else:
                        flattened_row[col] = None
                        flattened_row[f"{col}_count"] = 0
                else:
                    flattened_row[col] = value
                    
            flattened_data.append(flattened_row)
            
        return pd.DataFrame(flattened_data)
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', separator: str = '_') -> Dict[str, Any]:
        """Recursively flatten a nested dictionary.
        
        Args:
            d: Dictionary to flatten
            parent_key: Parent key for nested structure
            separator: String to separate nested keys
            
        Returns:
            Flattened dictionary
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{separator}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, separator).items())
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    # For arrays of objects, flatten the first object
                    items.extend(self._flatten_dict(v[0], new_key, separator).items())
                    items.append((f"{new_key}_count", len(v)))
                else:
                    # For simple arrays, convert to comma-separated string
                    items.append((new_key, ','.join(str(item) for item in v) if v else None))
                    items.append((f"{new_key}_count", len(v)))
            else:
                items.append((new_key, v))
        return dict(items)
    
    def apply_schema(self, df: pd.DataFrame, strict: bool = False) -> pd.DataFrame:
        """Apply schema transformations to DataFrame.
        
        Args:
            df: Input DataFrame
            strict: If True, raise exceptions for schema violations
            
        Returns:
            DataFrame transformed according to schema
        """
        if not self.schema:
            return df
            
        transformed_df = df.copy()
        self.validation_errors = []
        
        # Apply column transformations based on schema
        for col_name, col_config in self.schema.items():
            if col_name in transformed_df.columns:
                try:
                    transformed_df[col_name] = self._transform_column(
                        transformed_df[col_name], col_config
                    )
                except Exception as e:
                    error_msg = f"Error transforming column '{col_name}': {str(e)}"
                    self.validation_errors.append(error_msg)
                    if strict:
                        raise ValueError(error_msg)
            elif col_config.get('required', False):
                error_msg = f"Required column '{col_name}' is missing"
                self.validation_errors.append(error_msg)
                if strict:
                    raise ValueError(error_msg)
                # Add missing column with default value
                default_value = col_config.get('default')
                transformed_df[col_name] = default_value
        
        # Remove columns not in schema if specified
        if self.schema.get('_strict_columns', False):
            schema_columns = [col for col in self.schema.keys() if not col.startswith('_')]
            transformed_df = transformed_df[schema_columns]
            
        return transformed_df
    
    def _transform_column(self, series: pd.Series, col_config: Dict[str, Any]) -> pd.Series:
        """Transform a single column based on its configuration.
        
        Args:
            series: Input pandas Series
            col_config: Column configuration from schema
            
        Returns:
            Transformed Series
        """
        col_type = col_config.get('type', 'str')
        nullable = col_config.get('nullable', True)
        default_value = col_config.get('default')
        
        # Handle null values
        if not nullable and series.isnull().any():
            if default_value is not None:
                series = series.fillna(default_value)
            else:
                raise ValueError(f"Column contains null values but is not nullable")
        
        # Apply type conversions
        if col_type == 'int':
            series = pd.to_numeric(series, errors='coerce').astype('Int64')
        elif col_type == 'float':
            series = pd.to_numeric(series, errors='coerce')
        elif col_type == 'str':
            series = series.astype(str)
        elif col_type == 'datetime':
            series = pd.to_datetime(series, errors='coerce')
        elif col_type == 'bool':
            series = series.astype(bool)
        elif col_type == 'category':
            categories = col_config.get('categories', [])
            if categories:
                series = pd.Categorical(series, categories=categories)
            else:
                series = series.astype('category')
        
        # Apply validation rules
        if 'min_value' in col_config:
            mask = series >= col_config['min_value']
            if not mask.all():
                invalid_count = (~mask).sum()
                raise ValueError(f"{invalid_count} values below minimum {col_config['min_value']}")
                
        if 'max_value' in col_config:
            mask = series <= col_config['max_value']
            if not mask.all():
                invalid_count = (~mask).sum()
                raise ValueError(f"{invalid_count} values above maximum {col_config['max_value']}")
        
        if 'pattern' in col_config:
            pattern = re.compile(col_config['pattern'])
            mask = series.astype(str).str.match(pattern, na=False)
            if not mask.all():
                invalid_count = (~mask).sum()
                raise ValueError(f"{invalid_count} values don't match pattern {col_config['pattern']}")
        
        return series
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply standard data cleaning operations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Cleaned DataFrame
        """
        cleaned_df = df.copy()
        
        # Remove completely empty rows
        cleaned_df = cleaned_df.dropna(how='all')
        
        # Clean string columns
        for col in cleaned_df.select_dtypes(include=['object']).columns:
            # Strip whitespace
            cleaned_df[col] = cleaned_df[col].astype(str).str.strip()
            # Replace empty strings with NaN
            cleaned_df[col] = cleaned_df[col].replace('', pd.NA)
            # Replace 'null', 'NULL', 'None' strings with NaN
            cleaned_df[col] = cleaned_df[col].replace(['null', 'NULL', 'None', 'nan', 'NaN'], pd.NA)
        
        return cleaned_df
    
    def transform(self, df: pd.DataFrame, flatten: bool = True, clean: bool = True, apply_schema: bool = True) -> pd.DataFrame:
        """Apply complete transformation pipeline to DataFrame.
        
        Args:
            df: Input DataFrame from Module 1
            flatten: Whether to flatten nested structures
            clean: Whether to apply data cleaning
            apply_schema: Whether to apply schema transformations
            
        Returns:
            Fully transformed DataFrame
        """
        result_df = df.copy()
        
        # Step 1: Flatten nested structures
        if flatten:
            result_df = self.flatten_dataframe(result_df)
        
        # Step 2: Clean data
        if clean:
            result_df = self.clean_data(result_df)
        
        # Step 3: Apply custom transformations
        for transformation in self.transformations:
            try:
                result_df = transformation['function'](result_df)
            except Exception as e:
                raise ValueError(f"Error in transformation '{transformation['name']}': {str(e)}")
        
        # Step 4: Apply schema
        if apply_schema and self.schema:
            result_df = self.apply_schema(result_df)
        
        return result_df
    
    def transform_spark_dataframe(self, spark_df, spark_session):
        """Transform Spark DataFrame (placeholder for Spark-specific transformations).
        
        Args:
            spark_df: Input Spark DataFrame
            spark_session: Active Spark session
            
        Returns:
            Transformed Spark DataFrame
        """
        # Convert to pandas for transformation, then back to Spark
        # This is a simple approach - for large data, use native Spark operations
        pandas_df = spark_df.toPandas()
        transformed_pandas = self.transform(pandas_df)
        return spark_session.createDataFrame(transformed_pandas)
    
    def get_schema_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a schema summary from the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Schema summary with column types, null counts, etc.
        """
        summary = {}
        
        for col in df.columns:
            col_data = df[col]
            summary[col] = {
                'type': str(col_data.dtype),
                'null_count': col_data.isnull().sum(),
                'null_percentage': (col_data.isnull().sum() / len(df)) * 100,
                'unique_count': col_data.nunique(),
                'sample_values': col_data.dropna().head(3).tolist()
            }
            
            # Add numeric statistics for numeric columns
            if col_data.dtype in ['int64', 'float64', 'Int64']:
                summary[col].update({
                    'min_value': col_data.min(),
                    'max_value': col_data.max(),
                    'mean_value': col_data.mean()
                })
                
        return summary
    
    def validate_schema_compliance(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate DataFrame against the defined schema.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validation report
        """
        if not self.schema:
            return {'status': 'no_schema', 'errors': [], 'warnings': []}
        
        errors = []
        warnings = []
        
        # Check required columns
        for col_name, col_config in self.schema.items():
            if col_name.startswith('_'):
                continue
                
            if col_config.get('required', False) and col_name not in df.columns:
                errors.append(f"Required column '{col_name}' is missing")
        
        # Check column types and constraints
        for col in df.columns:
            if col in self.schema:
                col_config = self.schema[col]
                col_data = df[col]
                
                # Check nullability
                if not col_config.get('nullable', True) and col_data.isnull().any():
                    errors.append(f"Column '{col}' contains null values but is not nullable")
                
                # Check data type compatibility
                expected_type = col_config.get('type')
                if expected_type and not self._is_type_compatible(col_data.dtype, expected_type):
                    warnings.append(f"Column '{col}' has type {col_data.dtype} but expected {expected_type}")
        
        # Check for extra columns
        if self.schema.get('_strict_columns', False):
            schema_columns = [col for col in self.schema.keys() if not col.startswith('_')]
            extra_columns = set(df.columns) - set(schema_columns)
            if extra_columns:
                warnings.append(f"Extra columns found: {list(extra_columns)}")
        
        status = 'valid' if not errors else 'invalid'
        if warnings and not errors:
            status = 'valid_with_warnings'
            
        return {
            'status': status,
            'errors': errors,
            'warnings': warnings,
            'columns_validated': len([col for col in df.columns if col in self.schema]),
            'total_columns': len(df.columns)
        }
    
    def _is_type_compatible(self, actual_type, expected_type) -> bool:
        """Check if actual data type is compatible with expected type."""
        type_mapping = {
            'str': ['object', 'string'],
            'int': ['int64', 'Int64', 'int32', 'int16'],
            'float': ['float64', 'float32'],
            'datetime': ['datetime64[ns]', 'datetime64'],
            'bool': ['bool', 'boolean']
        }
        
        compatible_types = type_mapping.get(expected_type, [expected_type])
        return str(actual_type) in compatible_types
    
    def generate_schema_from_dataframe(self, df: pd.DataFrame, sample_size: int = 1000, 
                                     include_constraints: bool = True, replace_existing: bool = False) -> Dict[str, Any]:
        """Generate schema by analyzing a DataFrame sample.
        
        Args:
            df: Input DataFrame to analyze
            sample_size: Number of rows to sample for analysis
            include_constraints: Whether to include min/max constraints
            replace_existing: Whether to replace existing schema
            
        Returns:
            Generated schema dictionary
        """
        generated_schema = SchemaGenerator.generate_from_dataframe(
            df, sample_size=sample_size, include_constraints=include_constraints
        )
        
        if replace_existing or not self.schema:
            self.schema = generated_schema
            self.schema_source = "dataframe_generated"
        
        return generated_schema
    
    def load_schema_from_avro(self, avro_content: Union[str, bytes, Dict[str, Any]], 
                            replace_existing: bool = False) -> Dict[str, Any]:
        """Load schema from Avro format.
        
        Args:
            avro_content: Avro schema as JSON string, bytes, or dict
            replace_existing: Whether to replace existing schema
            
        Returns:
            Parsed schema dictionary
        """
        parsed_schema = SchemaGenerator.parse_avro_schema(avro_content)
        
        if replace_existing or not self.schema:
            self.schema = parsed_schema
            self.schema_source = "avro_loaded"
        
        return parsed_schema
    
    def export_schema_to_avro(self, record_name: str = "TransformedRecord", 
                            namespace: str = "com.morphix.etl") -> Dict[str, Any]:
        """Export current schema to Avro format.
        
        Args:
            record_name: Name for the Avro record
            namespace: Namespace for the Avro schema
            
        Returns:
            Avro schema dictionary
        """
        if not self.schema:
            raise ValueError("No schema available to export")
            
        return SchemaGenerator.schema_to_avro(self.schema, record_name, namespace)
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a summary of the current schema.
        
        Returns:
            Schema summary with statistics
        """
        if not self.schema:
            return {"status": "no_schema", "field_count": 0}
        
        summary = {
            "status": "active",
            "source": self.schema_source,
            "field_count": len(self.schema),
            "nullable_fields": sum(1 for field in self.schema.values() if field.get('nullable', False)),
            "field_types": {},
            "fields": list(self.schema.keys())
        }
        
        # Count field types
        for field_config in self.schema.values():
            field_type = field_config.get('type', 'unknown')
            summary["field_types"][field_type] = summary["field_types"].get(field_type, 0) + 1
        
        return summary
    
    def merge_schemas(self, other_schema: Dict[str, Any], strategy: str = "union") -> Dict[str, Any]:
        """Merge another schema with the current schema.
        
        Args:
            other_schema: Schema to merge
            strategy: Merge strategy ("union", "intersection", "override")
            
        Returns:
            Merged schema
        """
        if not self.schema:
            self.schema = other_schema.copy()
            return self.schema
        
        if strategy == "union":
            # Include all fields from both schemas
            merged = self.schema.copy()
            for field, config in other_schema.items():
                if field not in merged:
                    merged[field] = config
                else:
                    # Merge field configurations
                    merged_config = merged[field].copy()
                    merged_config.update(config)
                    # Handle nullable - true if either is nullable
                    if merged[field].get('nullable') or config.get('nullable'):
                        merged_config['nullable'] = True
                    merged[field] = merged_config
            self.schema = merged
            
        elif strategy == "intersection":
            # Only include fields present in both schemas
            merged = {}
            for field in self.schema:
                if field in other_schema:
                    merged_config = self.schema[field].copy()
                    merged_config.update(other_schema[field])
                    merged[field] = merged_config
            self.schema = merged
            
        elif strategy == "override":
            # Other schema takes precedence
            merged = self.schema.copy()
            merged.update(other_schema)
            self.schema = merged
            
        else:
            raise ValueError(f"Unknown merge strategy: {strategy}")
        
        self.schema_source = f"merged_{strategy}"
        return self.schema
    
    def apply_repair_plan(self, plan: Dict[str, Any], df: pd.DataFrame) -> pd.DataFrame:
        """Apply repair plan to DataFrame.
        
        Requires plan.approved = True to execute. Refuses execution otherwise.
        
        Args:
            plan: Repair plan dictionary with 'approved' flag and 'operations' list
            df: DataFrame to transform
            
        Returns:
            Transformed DataFrame
            
        Raises:
            ValueError: If plan is not approved
        """
        if not plan.get("approved", False):
            error_msg = "Repair plan not approved. Cannot apply unapproved repairs."
            self.logger.error(
                error_msg,
                extra={
                    'event_type': 'repair_plan_rejected',
                    'plan_id': plan.get("plan_id", "unknown"),
                    'approved': False
                }
            )
            raise ValueError(error_msg)
        
        self.logger.info(
            f"Applying approved repair plan: {plan.get('plan_id', 'unknown')}",
            extra={
                'event_type': 'repair_plan_applied',
                'plan_id': plan.get("plan_id", "unknown"),
                'operation_count': len(plan.get("operations", []))
            }
        )
        
        transformed_df = df.copy()
        operations = plan.get("operations", [])
        
        for operation in operations:
            op_type = operation.get("type")
            field = operation.get("field")
            
            try:
                if op_type == "type_conversion":
                    target_type = operation.get("target_type")
                    if target_type == "integer":
                        transformed_df[field] = pd.to_numeric(transformed_df[field], errors='coerce').astype('Int64')
                    elif target_type == "float":
                        transformed_df[field] = pd.to_numeric(transformed_df[field], errors='coerce')
                    elif target_type == "string":
                        transformed_df[field] = transformed_df[field].astype(str)
                    elif target_type == "boolean":
                        transformed_df[field] = transformed_df[field].astype(bool)
                
                elif op_type == "add_field":
                    default_value = operation.get("default_value")
                    transformed_df[field] = default_value
                
                elif op_type == "remove_field":
                    if field in transformed_df.columns:
                        transformed_df = transformed_df.drop(columns=[field])
                
                elif op_type == "fill_null":
                    fill_value = operation.get("fill_value")
                    transformed_df[field] = transformed_df[field].fillna(fill_value)
                
                self.logger.debug(
                    f"Applied repair operation: {op_type} on field {field}",
                    extra={
                        'event_type': 'repair_operation_applied',
                        'operation_type': op_type,
                        'field': field
                    }
                )
                
            except Exception as e:
                self.logger.error(
                    f"Failed to apply repair operation: {e}",
                    exc_info=True,
                    extra={
                        'event_type': 'repair_operation_error',
                        'operation_type': op_type,
                        'field': field,
                        'error': str(e)
                    }
                )
                # Continue with other operations
                continue
        
        return transformed_df