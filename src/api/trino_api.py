"""
Trino API endpoints for querying Hudi tables.
"""
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

try:
    from ..trino import TrinoClient, TrinoConfig, TrinoQueryRequest, TrinoError
    from ..trino.client import TRINO_AVAILABLE
except ImportError:
    TrinoClient = None
    TrinoConfig = None
    TrinoQueryRequest = None
    TrinoError = Exception
    TRINO_AVAILABLE = False
from ..auth import get_current_active_user, UserResponse

trino_router = APIRouter(prefix="/trino", tags=["trino"])

# Import get_settings inside functions that need it to avoid import errors at module level


class TrinoQueryAPIRequest(BaseModel):
    """API request model for Trino queries."""
    sql: str = Field(..., description="SQL query to execute")
    catalog: Optional[str] = Field(None, description="Catalog name")
    schema: Optional[str] = Field(None, description="Schema name")
    max_rows: Optional[int] = Field(default=1000, description="Maximum rows to return")
    timeout: Optional[int] = Field(None, description="Query timeout in seconds")


def get_trino_client() -> TrinoClient:
    """Get Trino client using centralized configuration."""
    if not TRINO_AVAILABLE or TrinoClient is None:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "trino_client_not_available",
                "message": "Trino Python client library not installed. Install with: pip install trino",
                "help": "The trino Python package is a client library. You also need a Trino server running separately."
            }
        )
    
    try:
        from config.settings import get_settings
        settings = get_settings()
        trino_config = TrinoConfig(
            host=settings.trino.host,
            port=settings.trino.port,
            user=settings.trino.user,
            catalog=settings.trino.catalog,
            schema=settings.trino.schema,
            http_scheme=settings.trino.http_scheme,
            request_timeout=settings.trino.request_timeout
        )
        return TrinoClient(config=trino_config)
    except TrinoError as e:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "trino_server_unavailable",
                "message": f"Cannot connect to Trino server: {str(e)}",
                "help": "Ensure Trino server is running and accessible. See docs/trino_setup.md"
            }
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "trino_configuration_error",
                "message": str(e),
                "help": "Check Trino configuration in settings or environment variables."
            }
        )


@trino_router.post("/query")
def execute_query(request: TrinoQueryAPIRequest, current_user: UserResponse = Depends(get_current_active_user)):
    """Execute a SQL query via Trino.
    
    Note: Requires a Trino server to be running separately. This API uses
    the Trino Python client library to connect to an external Trino server.
    """
    from config.settings import get_settings
    settings = get_settings()
    trino_config = TrinoConfig(
        host=settings.trino.host,
        port=settings.trino.port,
        user=settings.trino.user,
        catalog=request.catalog or settings.trino.catalog,
        schema=request.schema or settings.trino.schema,
        http_scheme=settings.trino.http_scheme,
        request_timeout=request.timeout or settings.trino.request_timeout
    )
    
    client = TrinoClient(config=trino_config)
    try:
        result = client.execute_query(
            query=request.sql,
            catalog=request.catalog,
            schema=request.schema,
            max_rows=request.max_rows,
            timeout=request.timeout
        )
        return {
            "status": "ok" if result.success else "error",
            "query": result.query,
            "columns": result.columns,
            "data": result.data,
            "rows_returned": result.rows_returned,
            "execution_time_ms": result.execution_time_ms,
            "query_id": result.query_id,
            "error": result.error
        }
    finally:
        client.close()


@trino_router.get("/catalogs")
def list_catalogs(current_user: UserResponse = Depends(get_current_active_user)):
    """List all available Trino catalogs."""
    client = get_trino_client()
    try:
        catalogs = client.list_catalogs()
        return {
            "status": "ok",
            "catalogs": [
                {"catalog_name": cat.catalog_name, "connector_name": cat.connector_name}
                for cat in catalogs
            ]
        }
    finally:
        client.close()


@trino_router.get("/schemas")
def list_schemas(catalog: Optional[str] = None, current_user: UserResponse = Depends(get_current_active_user)):
    """List schemas in a catalog."""
    if not catalog:
        from config.settings import get_settings
        catalog = get_settings().trino.catalog
    
    client = get_trino_client()
    try:
        schemas = client.list_schemas(catalog=catalog)
        return {
            "status": "ok",
            "catalog": catalog,
            "schemas": [{"catalog": s.catalog, "schema": s.schema} for s in schemas]
        }
    finally:
        client.close()


@trino_router.get("/tables")
def list_tables(catalog: Optional[str] = None, schema: Optional[str] = None, current_user: UserResponse = Depends(get_current_active_user)):
    """List tables in a schema."""
    from config.settings import get_settings
    settings = get_settings()
    catalog = catalog or settings.trino.catalog
    schema = schema or settings.trino.schema
    
    client = get_trino_client()
    try:
        tables = client.list_tables(catalog=catalog, schema=schema)
        return {
            "status": "ok",
            "catalog": catalog,
            "schema": schema,
            "tables": [
                {
                    "catalog": t.catalog,
                    "schema": t.schema,
                    "table": t.table,
                    "table_type": t.table_type,
                    "comment": t.comment
                }
                for t in tables
            ]
        }
    finally:
        client.close()


@trino_router.get("/table/{table_name}/describe")
def describe_table(table_name: str, catalog: Optional[str] = None, schema: Optional[str] = None, current_user: UserResponse = Depends(get_current_active_user)):
    """Get table schema (column information)."""
    from config.settings import get_settings
    settings = get_settings()
    catalog = catalog or settings.trino.catalog
    schema = schema or settings.trino.schema
    
    client = get_trino_client()
    try:
        columns = client.describe_table(table_name=table_name, catalog=catalog, schema=schema)
        return {
            "status": "ok",
            "catalog": catalog,
            "schema": schema,
            "table": table_name,
            "columns": [{"name": c.name, "type": c.type, "comment": c.comment} for c in columns]
        }
    finally:
        client.close()


@trino_router.get("/health")
def trino_health_check(current_user: UserResponse = Depends(get_current_active_user)):
    """Check Trino server connectivity."""
    client = get_trino_client()
    try:
        catalogs = client.list_catalogs()
        return {"status": "ok", "message": "Trino server is reachable", "catalogs_count": len(catalogs)}
    finally:
        client.close()

