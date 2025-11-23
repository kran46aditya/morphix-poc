"""
ClickHouse Writer

Robust batch insert with type coercion safety checks and schema validation.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import os

try:
    import clickhouse_connect
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False

from ..utils.logging import get_logger

logger = get_logger(__name__)


class ClickHouseWriter:
    """Writer for ClickHouse database with type coercion and validation."""
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 database: Optional[str] = None, username: Optional[str] = None,
                 password: Optional[str] = None):
        """Initialize ClickHouse writer.
        
        Args:
            host: ClickHouse host (defaults to CLICKHOUSE_HOST env var or localhost)
            port: ClickHouse port (defaults to CLICKHOUSE_PORT env var or 8123)
            database: Database name (defaults to CLICKHOUSE_DATABASE env var)
            username: Username (defaults to CLICKHOUSE_USERNAME env var)
            password: Password (defaults to CLICKHOUSE_PASSWORD env var)
        """
        self.host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.database = database or os.getenv("CLICKHOUSE_DATABASE", "default")
        self.username = username or os.getenv("CLICKHOUSE_USERNAME", "default")
        self.password = password or os.getenv("CLICKHOUSE_PASSWORD", "")
        self.logger = get_logger(__name__)
        self._client = None
    
    def _get_client(self):
        """Get or create ClickHouse client."""
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError(
                "clickhouse-connect not available. Install with: pip install clickhouse-connect"
            )
        
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.username,
                password=self.password
            )
        
        return self._client
    
    def validate_target_schema(self, table_name: str, df: pd.DataFrame) -> Dict[str, Any]:
        """Validate DataFrame schema against target table schema.
        
        Args:
            table_name: Target table name
            df: DataFrame to validate
            
        Returns:
            Validation result dictionary
        """
        try:
            client = self._get_client()
            
            # Get table schema
            schema_query = f"DESCRIBE TABLE {table_name}"
            table_schema = client.query(schema_query).result_rows
            
            # Build schema dictionary
            target_schema = {}
            for row in table_schema:
                col_name = row[0]
                col_type = row[1]
                target_schema[col_name] = col_type
            
            # Check for missing columns
            missing_cols = set(target_schema.keys()) - set(df.columns)
            extra_cols = set(df.columns) - set(target_schema.keys())
            
            # Type compatibility check
            type_issues = []
            for col in df.columns:
                if col in target_schema:
                    target_type = target_schema[col]
                    df_type = str(df[col].dtype)
                    
                    # Basic type compatibility check
                    if not self._is_type_compatible(df_type, target_type):
                        type_issues.append({
                            "column": col,
                            "dataframe_type": df_type,
                            "target_type": target_type
                        })
            
            result = {
                "valid": len(missing_cols) == 0 and len(type_issues) == 0,
                "missing_columns": list(missing_cols),
                "extra_columns": list(extra_cols),
                "type_issues": type_issues,
                "target_schema": target_schema
            }
            
            if not result["valid"]:
                self.logger.warning(
                    f"Schema validation issues for table {table_name}",
                    extra={
                        'event_type': 'clickhouse_schema_validation_issues',
                        'table_name': table_name,
                        'missing_columns': list(missing_cols),
                        'type_issues': type_issues
                    }
                )
            
            return result
            
        except Exception as e:
            self.logger.error(
                f"Failed to validate target schema: {e}",
                exc_info=True,
                extra={
                    'event_type': 'clickhouse_schema_validation_error',
                    'table_name': table_name,
                    'error': str(e)
                }
            )
            return {
                "valid": False,
                "error": str(e)
            }
    
    def _is_type_compatible(self, df_type: str, target_type: str) -> bool:
        """Check if DataFrame type is compatible with ClickHouse type.
        
        Args:
            df_type: Pandas DataFrame dtype
            target_type: ClickHouse column type
            
        Returns:
            True if compatible, False otherwise
        """
        # Normalize types
        df_type_lower = df_type.lower()
        target_type_lower = target_type.lower()
        
        # Integer types
        if 'int' in df_type_lower and ('int' in target_type_lower or 'uint' in target_type_lower):
            return True
        
        # Float types
        if 'float' in df_type_lower and ('float' in target_type_lower or 'double' in target_type_lower):
            return True
        
        # String types
        if df_type_lower == 'object' and ('string' in target_type_lower or 'fixedstring' in target_type_lower):
            return True
        
        # Boolean types
        if 'bool' in df_type_lower and 'bool' in target_type_lower:
            return True
        
        # Date/DateTime types
        if 'datetime' in df_type_lower and ('date' in target_type_lower or 'datetime' in target_type_lower):
            return True
        
        return False
    
    def write_batch(self, table_name: str, df: pd.DataFrame, 
                   validate_schema: bool = True,
                   batch_size: int = 10000) -> Dict[str, Any]:
        """Write DataFrame to ClickHouse in batches with type coercion.
        
        Args:
            table_name: Target table name
            df: DataFrame to write
            validate_schema: Whether to validate schema before write
            batch_size: Batch size for inserts
            
        Returns:
            Write result dictionary with counts and statistics
        """
        if not CLICKHOUSE_AVAILABLE:
            raise ImportError(
                "clickhouse-connect not available. Install with: pip install clickhouse-connect"
            )
        
        if df.empty:
            self.logger.warning(
                f"Empty DataFrame provided for table {table_name}",
                extra={
                    'event_type': 'clickhouse_empty_dataframe',
                    'table_name': table_name
                }
            )
            return {
                "success": True,
                "rows_written": 0,
                "batches": 0
            }
        
        try:
            # Validate schema if requested
            if validate_schema:
                validation = self.validate_target_schema(table_name, df)
                if not validation.get("valid", False):
                    # Try to coerce types
                    df = self._coerce_types(df, validation.get("target_schema", {}))
            
            # Get client
            client = self._get_client()
            
            # Write in batches
            total_rows = len(df)
            batches = (total_rows + batch_size - 1) // batch_size
            rows_written = 0
            
            for i in range(0, total_rows, batch_size):
                batch_df = df.iloc[i:i + batch_size]
                
                # Coerce types for safety
                batch_df = self._coerce_types_safe(batch_df)
                
                # Insert batch
                client.insert_df(table_name, batch_df)
                rows_written += len(batch_df)
                
                self.logger.debug(
                    f"Inserted batch {i // batch_size + 1}/{batches} to {table_name}",
                    extra={
                        'event_type': 'clickhouse_batch_inserted',
                        'table_name': table_name,
                        'batch_number': i // batch_size + 1,
                        'batch_size': len(batch_df)
                    }
                )
            
            self.logger.info(
                f"Successfully wrote {rows_written} rows to {table_name}",
                extra={
                    'event_type': 'clickhouse_write_completed',
                    'table_name': table_name,
                    'rows_written': rows_written,
                    'batches': batches
                }
            )
            
            return {
                "success": True,
                "rows_written": rows_written,
                "batches": batches,
                "table_name": table_name
            }
            
        except Exception as e:
            self.logger.error(
                f"Failed to write to ClickHouse: {e}",
                exc_info=True,
                extra={
                    'event_type': 'clickhouse_write_error',
                    'table_name': table_name,
                    'error': str(e)
                }
            )
            raise
    
    def _coerce_types(self, df: pd.DataFrame, target_schema: Dict[str, str]) -> pd.DataFrame:
        """Coerce DataFrame types to match target schema.
        
        Args:
            df: DataFrame to coerce
            target_schema: Target schema dictionary
            
        Returns:
            Coerced DataFrame
        """
        coerced_df = df.copy()
        
        for col in coerced_df.columns:
            if col in target_schema:
                target_type = target_schema[col].lower()
                
                try:
                    if 'int' in target_type or 'uint' in target_type:
                        coerced_df[col] = pd.to_numeric(coerced_df[col], errors='coerce').astype('Int64')
                    elif 'float' in target_type or 'double' in target_type:
                        coerced_df[col] = pd.to_numeric(coerced_df[col], errors='coerce')
                    elif 'string' in target_type or 'fixedstring' in target_type:
                        coerced_df[col] = coerced_df[col].astype(str)
                    elif 'bool' in target_type:
                        coerced_df[col] = coerced_df[col].astype(bool)
                    elif 'date' in target_type:
                        coerced_df[col] = pd.to_datetime(coerced_df[col], errors='coerce')
                except Exception as e:
                    self.logger.warning(
                        f"Failed to coerce column {col} to {target_type}: {e}",
                        extra={
                            'event_type': 'clickhouse_type_coercion_warning',
                            'column': col,
                            'target_type': target_type,
                            'error': str(e)
                        }
                    )
        
        return coerced_df
    
    def _coerce_types_safe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply safe type coercion to DataFrame.
        
        Args:
            df: DataFrame to coerce
            
        Returns:
            Coerced DataFrame
        """
        coerced_df = df.copy()
        
        for col in coerced_df.columns:
            dtype = str(coerced_df[col].dtype)
            
            # Handle numeric types
            if 'int' in dtype:
                coerced_df[col] = pd.to_numeric(coerced_df[col], errors='coerce').astype('Int64')
            elif 'float' in dtype:
                coerced_df[col] = pd.to_numeric(coerced_df[col], errors='coerce')
            # Handle string types - ensure no None values become 'None' string
            elif dtype == 'object':
                coerced_df[col] = coerced_df[col].astype(str).replace('nan', None).replace('None', None)
            # Handle datetime
            elif 'datetime' in dtype:
                coerced_df[col] = pd.to_datetime(coerced_df[col], errors='coerce')
        
        return coerced_df

