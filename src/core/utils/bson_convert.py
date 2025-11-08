"""
BSON to JSON-serializable converter utility.

Converts MongoDB BSON types to JSON-serializable Python types.
"""

from bson import ObjectId, Decimal128
from datetime import datetime
import base64
from typing import Any


def bson_safe(value: Any) -> Any:
    """
    Recursively convert BSON types to JSON-serializable Python types.
    
    Handles:
    - ObjectId -> str
    - datetime -> ISO string
    - Decimal128 -> str
    - bytes -> base64 string
    - Nested dicts and lists
    
    Args:
        value: Value to convert (can be any BSON type)
        
    Returns:
        JSON-serializable Python value
        
    Example:
        >>> from bson import ObjectId
        >>> doc = {"_id": ObjectId(), "name": "test"}
        >>> safe = bson_safe(doc)
        >>> isinstance(safe["_id"], str)
        True
    """
    if value is None:
        return None
    
    if isinstance(value, ObjectId):
        return str(value)
    
    if isinstance(value, datetime):
        return value.isoformat()
    
    if isinstance(value, Decimal128):
        return str(value)  # or Decimal(value.to_decimal()) for numeric operations
    
    if isinstance(value, bytes):
        return base64.b64encode(value).decode('ascii')
    
    if isinstance(value, dict):
        return {k: bson_safe(v) for k, v in value.items()}
    
    if isinstance(value, list):
        return [bson_safe(v) for v in value]
    
    if isinstance(value, tuple):
        return tuple(bson_safe(v) for v in value)
    
    if isinstance(value, set):
        return [bson_safe(v) for v in value]
    
    return value


def redact_pii(doc: dict, pii_fields: list = None) -> dict:
    """
    Remove or mask PII fields from document.
    
    Args:
        doc: Document dictionary
        pii_fields: List of field names to redact (default: common PII fields)
        
    Returns:
        Document with PII fields redacted
        
    Example:
        >>> doc = {"name": "John", "ssn": "123-45-6789", "email": "john@example.com"}
        >>> safe = redact_pii(doc)
        >>> safe["ssn"]
        '[REDACTED]'
    """
    if pii_fields is None:
        pii_fields = ["ssn", "credit_card", "password", "email", "phone", "ssn_last4"]
    
    safe_doc = doc.copy()
    for field in pii_fields:
        if field in safe_doc:
            safe_doc[field] = "[REDACTED]"
    
    return safe_doc

