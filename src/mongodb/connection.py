from typing import Any, Dict, List, Optional
import pymongo
from pymongo.collection import Collection
from bson import ObjectId, Binary, Decimal128
from bson.json_util import default as bson_default
from datetime import datetime as py_datetime
import base64


def _get_client(mongo_uri: str) -> pymongo.MongoClient:
    """Create a MongoClient from a URI. Caller is responsible for closing if needed.

    Importing pymongo.MongoClient at call time allows tests to monkeypatch
    `pymongo.MongoClient` (e.g., with mongomock) and have our code pick it up.
    """
    client = pymongo.MongoClient(mongo_uri)
    return client


def _serialize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively serialize a MongoDB document to JSON-serializable format.
    
    Handles:
    - ObjectId -> string
    - Binary -> base64 encoded string with metadata
    - datetime -> ISO format string
    - Decimal128 -> float or string
    - Other BSON types -> appropriate JSON types
    """
    if not isinstance(doc, dict):
        return doc
    
    serialized = {}
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            serialized[key] = str(value)
        elif isinstance(value, Binary):
            # Encode binary as base64 string with type info
            binary_data = value if isinstance(value, bytes) else value.data if hasattr(value, 'data') else bytes(value)
            serialized[key] = {
                "$binary": {
                    "base64": base64.b64encode(binary_data).decode('utf-8'),
                    "subType": "%02x" % (value.subtype if hasattr(value, 'subtype') else 0)
                }
            }
        elif isinstance(value, py_datetime):
            # Convert datetime to ISO format string
            serialized[key] = value.isoformat()
        elif isinstance(value, Decimal128):
            # Convert Decimal128 to string to preserve precision
            serialized[key] = str(value)
        elif isinstance(value, dict):
            # Recursively serialize nested dictionaries
            serialized[key] = _serialize_doc(value)
        elif isinstance(value, list):
            # Recursively serialize lists
            def _serialize_list_item(item):
                if isinstance(item, dict):
                    return _serialize_doc(item)
                elif isinstance(item, ObjectId):
                    return str(item)
                elif isinstance(item, Binary):
                    binary_data = item if isinstance(item, bytes) else item.data if hasattr(item, 'data') else bytes(item)
                    return {
                        "$binary": {
                            "base64": base64.b64encode(binary_data).decode('utf-8'),
                            "subType": "%02x" % (item.subtype if hasattr(item, 'subtype') else 0)
                        }
                    }
                elif isinstance(item, py_datetime):
                    return item.isoformat()
                else:
                    return item
            
            serialized[key] = [_serialize_list_item(item) for item in value]
        else:
            # Try using bson.json_util default handler for other BSON types
            try:
                # Use bson's default serializer
                serialized_value = bson_default(value)
                if isinstance(serialized_value, dict) and len(serialized_value) == 1:
                    # If it's a BSON type marker dict, use it as is
                    serialized[key] = serialized_value
                else:
                    serialized[key] = serialized_value
            except (TypeError, ValueError):
                # Fallback to string representation
                try:
                    serialized[key] = str(value)
                except Exception:
                    serialized[key] = None
    
    return serialized


def read_with_pymongo(mongo_uri: str, database: str, collection: str, query: Optional[Dict[str, Any]] = None, limit: int = 10) -> List[Dict[str, Any]]:
    """Read documents from MongoDB using pymongo and return a small preview list.

    This function serializes all BSON types (ObjectId, Binary, datetime, etc.) to
    JSON-compatible formats.
    """
    query = query or {}
    client = _get_client(mongo_uri)
    try:
        db = client[database]
        coll: Collection = db[collection]
        cursor = coll.find(query).limit(limit)
        docs = []
        for d in cursor:
            # Serialize the document to handle all BSON types
            serialized_doc = _serialize_doc(d)
            docs.append(serialized_doc)
        return docs
    except Exception as e:
        # Wrap and re-raise with more context
        raise RuntimeError(f"Error reading from MongoDB: {str(e)}") from e
    finally:
        client.close()
