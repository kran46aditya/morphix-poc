"""
Trino integration module for querying Hudi tables.

This module provides functionality to:
- Connect to Trino server
- Execute SQL queries
- List catalogs, schemas, and tables
- Query Hudi tables created by the platform
"""
from .client import TrinoClient, TrinoQueryResult, TrinoError
from .models import TrinoConfig, TrinoQueryRequest

__all__ = [
    "TrinoClient",
    "TrinoQueryResult",
    "TrinoError",
    "TrinoConfig",
    "TrinoQueryRequest"
]

