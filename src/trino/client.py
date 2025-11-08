"""
Trino client for executing queries and managing connections.
"""
from typing import Optional, List, Dict, Any
import time
from datetime import datetime

try:
    import trino
    from trino.exceptions import TrinoQueryError, TrinoUserError, TrinoExternalError
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False
    trino = None
    TrinoQueryError = Exception
    TrinoUserError = Exception
    TrinoExternalError = Exception

from .models import TrinoConfig, TrinoQueryRequest, TrinoTableInfo, TrinoSchemaInfo, TrinoCatalogInfo, TrinoColumnInfo


class TrinoError(Exception):
    """Base exception for Trino operations."""
    pass


class TrinoQueryResult:
    """Result of a Trino query execution."""
    
    def __init__(
        self,
        query: str,
        columns: List[str],
        data: List[List[Any]],
        rows_returned: int,
        execution_time_ms: float,
        query_id: Optional[str] = None,
        error: Optional[str] = None
    ):
        self.query = query
        self.columns = columns
        self.data = data
        self.rows_returned = rows_returned
        self.execution_time_ms = execution_time_ms
        self.query_id = query_id
        self.error = error
        self.success = error is None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary format."""
        return {
            "query": self.query,
            "columns": self.columns,
            "data": self.data,
            "rows_returned": self.rows_returned,
            "execution_time_ms": self.execution_time_ms,
            "query_id": self.query_id,
            "success": self.success,
            "error": self.error
        }
    
    def to_records(self) -> List[Dict[str, Any]]:
        """Convert data to list of records (dict format)."""
        return [
            {col: row[i] for i, col in enumerate(self.columns)}
            for row in self.data
        ]


class TrinoClient:
    """Client for interacting with Trino server."""
    
    def __init__(self, config: Optional[TrinoConfig] = None):
        """Initialize Trino client.
        
        Args:
            config: Trino configuration. If None, uses default config.
        """
        if not TRINO_AVAILABLE:
            raise TrinoError("Trino library not available. Install with: pip install trino")
        
        self.config = config or TrinoConfig()
        self.connection = None
        self._ensure_connection()
    
    def _ensure_connection(self):
        """Ensure Trino connection is established."""
        if self.connection is None:
            try:
                self.connection = trino.dbapi.connect(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    catalog=self.config.catalog,
                    schema=self.config.schema,
                    http_scheme=self.config.http_scheme,
                    session_properties=self.config.session_properties,
                    request_timeout=self.config.request_timeout
                )
            except Exception as e:
                raise TrinoError(f"Failed to connect to Trino server: {str(e)}")
    
    def execute_query(
        self,
        query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        max_rows: Optional[int] = None,
        timeout: Optional[int] = None
    ) -> TrinoQueryResult:
        """Execute a SQL query.
        
        Args:
            query: SQL query to execute
            catalog: Catalog name (overrides default)
            schema: Schema name (overrides default)
            max_rows: Maximum rows to return
            timeout: Query timeout in seconds
            
        Returns:
            TrinoQueryResult with query results
            
        Raises:
            TrinoError: If query execution fails
        """
        start_time = time.time()
        query_id = None
        error = None
        
        try:
            # Use specified catalog/schema or defaults
            catalog = catalog or self.config.catalog
            schema = schema or self.config.schema
            
            # Set catalog/schema in connection if needed
            cursor = self.connection.cursor()
            
            # Execute query
            cursor.execute(query)
            
            # Get query ID if available
            try:
                query_id = getattr(cursor, 'query_id', None)
            except:
                pass
            
            # Fetch results
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchmany(max_rows or 1000)
            
            # Convert rows to list
            data = [list(row) for row in rows]
            
            execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
            
            return TrinoQueryResult(
                query=query,
                columns=columns,
                data=data,
                rows_returned=len(data),
                execution_time_ms=execution_time,
                query_id=query_id
            )
            
        except TrinoQueryError as e:
            error = str(e)
            execution_time = (time.time() - start_time) * 1000
            return TrinoQueryResult(
                query=query,
                columns=[],
                data=[],
                rows_returned=0,
                execution_time_ms=execution_time,
                query_id=query_id,
                error=error
            )
        except Exception as e:
            raise TrinoError(f"Query execution failed: {str(e)}")
    
    def list_catalogs(self) -> List[TrinoCatalogInfo]:
        """List all available catalogs.
        
        Returns:
            List of catalog information
            
        Raises:
            TrinoError: If operation fails
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW CATALOGS")
            rows = cursor.fetchall()
            
            catalogs = []
            for row in rows:
                catalogs.append(TrinoCatalogInfo(
                    catalog_name=row[0],
                    connector_name=row[1] if len(row) > 1 else None
                ))
            
            return catalogs
            
        except Exception as e:
            raise TrinoError(f"Failed to list catalogs: {str(e)}")
    
    def list_schemas(self, catalog: Optional[str] = None) -> List[TrinoSchemaInfo]:
        """List schemas in a catalog.
        
        Args:
            catalog: Catalog name (uses default if not provided)
            
        Returns:
            List of schema information
            
        Raises:
            TrinoError: If operation fails
        """
        try:
            catalog = catalog or self.config.catalog
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW SCHEMAS FROM {catalog}")
            rows = cursor.fetchall()
            
            schemas = []
            for row in rows:
                schemas.append(TrinoSchemaInfo(
                    catalog=catalog,
                    schema=row[0]
                ))
            
            return schemas
            
        except Exception as e:
            raise TrinoError(f"Failed to list schemas: {str(e)}")
    
    def list_tables(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[TrinoTableInfo]:
        """List tables in a schema.
        
        Args:
            catalog: Catalog name (uses default if not provided)
            schema: Schema name (uses default if not provided)
            
        Returns:
            List of table information
            
        Raises:
            TrinoError: If operation fails
        """
        try:
            catalog = catalog or self.config.catalog
            schema = schema or self.config.schema
            cursor = self.connection.cursor()
            cursor.execute(f"SHOW TABLES FROM {catalog}.{schema}")
            rows = cursor.fetchall()
            
            tables = []
            for row in rows:
                tables.append(TrinoTableInfo(
                    catalog=catalog,
                    schema=schema,
                    table=row[0],
                    table_type=row[1] if len(row) > 1 else None,
                    comment=row[2] if len(row) > 2 else None
                ))
            
            return tables
            
        except TrinoExternalError as e:
            error_msg = str(e)
            if "HIVE_METASTORE_ERROR" in error_msg or "Failed connecting to Hive metastore" in error_msg:
                raise TrinoError(
                    f"Failed to connect to Hive Metastore. "
                    f"This usually means the Hive Metastore service is not running or not accessible. "
                    f"Error: {error_msg}. "
                    f"To fix: Ensure hive-metastore container is running and healthy: "
                    f"docker compose ps hive-metastore && docker compose logs hive-metastore"
                )
            raise TrinoError(f"External error while listing tables: {error_msg}")
        except Exception as e:
            raise TrinoError(f"Failed to list tables: {str(e)}")
    
    def describe_table(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None
    ) -> List[TrinoColumnInfo]:
        """Describe a table (get column information).
        
        Args:
            table_name: Table name
            catalog: Catalog name (uses default if not provided)
            schema: Schema name (uses default if not provided)
            
        Returns:
            List of column information
            
        Raises:
            TrinoError: If operation fails
        """
        try:
            catalog = catalog or self.config.catalog
            schema = schema or self.config.schema
            cursor = self.connection.cursor()
            cursor.execute(f"DESCRIBE {catalog}.{schema}.{table_name}")
            rows = cursor.fetchall()
            
            columns = []
            for row in rows:
                columns.append(TrinoColumnInfo(
                    name=row[0],
                    type=row[1],
                    comment=row[2] if len(row) > 2 else None
                ))
            
            return columns
            
        except Exception as e:
            raise TrinoError(f"Failed to describe table: {str(e)}")
    
    def close(self):
        """Close Trino connection."""
        if self.connection:
            try:
                self.connection.close()
            except:
                pass
            self.connection = None
    
    def __enter__(self):
        """Context manager entry."""
        self._ensure_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

