"""
Trino client models and configuration.
"""
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class TrinoConfig(BaseModel):
    """Trino server configuration."""
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="admin", description="Trino user")
    catalog: str = Field(default="hive", description="Default catalog name")
    schema: str = Field(default="default", description="Default schema name")
    http_scheme: str = Field(default="http", description="HTTP scheme (http/https)")
    auth: Optional[Dict[str, Any]] = Field(None, description="Authentication configuration")
    session_properties: Dict[str, str] = Field(default_factory=dict, description="Session properties")
    request_timeout: int = Field(default=300, description="Request timeout in seconds")
    
    @property
    def server_url(self) -> str:
        """Get Trino server URL."""
        return f"{self.http_scheme}://{self.host}:{self.port}"


class TrinoQueryRequest(BaseModel):
    """Request model for Trino queries."""
    sql: str = Field(..., description="SQL query to execute")
    catalog: Optional[str] = Field(None, description="Catalog name (overrides default)")
    schema: Optional[str] = Field(None, description="Schema name (overrides default)")
    max_rows: Optional[int] = Field(default=1000, description="Maximum rows to return")
    session_properties: Optional[Dict[str, str]] = Field(None, description="Session properties")
    timeout: Optional[int] = Field(None, description="Query timeout in seconds")


class TrinoTableInfo(BaseModel):
    """Information about a Trino table."""
    catalog: str = Field(..., description="Catalog name")
    schema: str = Field(..., description="Schema name")
    table: str = Field(..., description="Table name")
    table_type: Optional[str] = Field(None, description="Table type")
    comment: Optional[str] = Field(None, description="Table comment")


class TrinoSchemaInfo(BaseModel):
    """Information about a Trino schema."""
    catalog: str = Field(..., description="Catalog name")
    schema: str = Field(..., description="Schema name")


class TrinoCatalogInfo(BaseModel):
    """Information about a Trino catalog."""
    catalog_name: str = Field(..., description="Catalog name")
    connector_name: Optional[str] = Field(None, description="Connector name")


class TrinoColumnInfo(BaseModel):
    """Information about a table column."""
    name: str = Field(..., description="Column name")
    type: str = Field(..., description="Column type")
    comment: Optional[str] = Field(None, description="Column comment")

