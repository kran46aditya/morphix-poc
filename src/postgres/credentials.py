"""Simple Postgres helper to store and retrieve MongoDB credentials per user.

This module uses the centralized configuration from config.settings.

It stores credentials in a table `mongo_credentials` with columns:
 - user_id (text primary key)
 - mongo_uri (text nullable)
 - username, password, host, port, database, collection (nullable)
 - query (jsonb)

This is intentionally small and synchronous.
"""
import os
import json
from typing import Optional, Dict, Any

import psycopg2
import psycopg2.extras

# Import centralized settings
try:
    import sys
    from pathlib import Path
    # Add project root to path if not already there
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from config.settings import get_settings
    _settings = get_settings()
except ImportError:
    # Fallback if settings not available (for backwards compatibility)
    _settings = None


def _get_conn():
    """Get PostgreSQL connection using centralized configuration."""
    if _settings:
        database_url = _settings.database.connection_url
    else:
        # Fallback to environment variable or default
        database_url = os.environ.get("DATABASE_URL", "postgresql://user:pass@localhost/morphix")
    return psycopg2.connect(database_url)


def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS mongo_credentials (
        user_id TEXT PRIMARY KEY,
        mongo_uri TEXT,
        username TEXT,
        password TEXT,
        host TEXT,
        port INTEGER,
        database TEXT,
        collection TEXT,
        query JSONB
    );
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    finally:
        conn.close()


def save_credentials(user_id: str, data: Dict[str, Any]):
    """Insert or update credentials for a user."""
    sql = """
    INSERT INTO mongo_credentials (user_id, mongo_uri, username, password, host, port, database, collection, query)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET
      mongo_uri = EXCLUDED.mongo_uri,
      username = EXCLUDED.username,
      password = EXCLUDED.password,
      host = EXCLUDED.host,
      port = EXCLUDED.port,
      database = EXCLUDED.database,
      collection = EXCLUDED.collection,
      query = EXCLUDED.query;
    """
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    user_id,
                    data.get("mongo_uri"),
                    data.get("username"),
                    data.get("password"),
                    data.get("host"),
                    data.get("port"),
                    data.get("database"),
                    data.get("collection"),
                    json.dumps(data.get("query") or {}),
                ))
    finally:
        conn.close()


def get_credentials(user_id: str) -> Optional[Dict[str, Any]]:
    sql = "SELECT user_id, mongo_uri, username, password, host, port, database, collection, query FROM mongo_credentials WHERE user_id = %s"
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (user_id,))
                row = cur.fetchone()
                if not row:
                    return None
                # Convert query JSON string to dict if needed
                if isinstance(row.get("query"), str):
                    try:
                        row["query"] = json.loads(row["query"]) if row["query"] else {}
                    except Exception:
                        row["query"] = {}
                return dict(row)
    finally:
        conn.close()
