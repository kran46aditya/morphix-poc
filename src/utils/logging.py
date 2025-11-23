"""
Logging utility module for Morphix.

Provides JSON-structured logging with correlation ID propagation for distributed tracing.
"""

import json
import logging
import uuid
from typing import Optional, Dict, Any
from datetime import datetime
from contextvars import ContextVar

# Context variable for correlation ID propagation
_correlation_id: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)


def get_correlation_id() -> Optional[str]:
    """Get the current correlation ID from context.
    
    Returns:
        Current correlation ID or None if not set
    """
    return _correlation_id.get()


def set_correlation_id(correlation_id: Optional[str] = None) -> str:
    """Set correlation ID in context.
    
    Args:
        correlation_id: Optional correlation ID. If None, generates a new UUID.
        
    Returns:
        The correlation ID that was set
    """
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())
    _correlation_id.set(correlation_id)
    return correlation_id


def clear_correlation_id():
    """Clear the correlation ID from context."""
    _correlation_id.set(None)


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log string
        """
        log_data: Dict[str, Any] = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add correlation ID if available
        correlation_id = get_correlation_id()
        if correlation_id:
            log_data['correlation_id'] = correlation_id
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields from record (check both extra_fields attribute and extra dict)
        if hasattr(record, 'extra_fields'):
            log_data.update(record.extra_fields)
        # Also check if extra dict was passed via logging call
        if hasattr(record, 'extra') and isinstance(record.extra, dict):
            log_data.update(record.extra)
        
        return json.dumps(log_data, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Get a JSON logger with correlation ID propagation.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (default: INFO)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    # Create console handler with JSON formatter
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(JSONFormatter())
    
    logger.addHandler(handler)
    logger.propagate = False  # Prevent duplicate logs from parent loggers
    
    return logger


class CorrelationContext:
    """Context manager for correlation ID propagation."""
    
    def __init__(self, correlation_id: Optional[str] = None):
        """Initialize correlation context.
        
        Args:
            correlation_id: Optional correlation ID. If None, generates a new UUID.
        """
        self.correlation_id = correlation_id
        self._previous_id: Optional[str] = None
    
    def __enter__(self) -> str:
        """Enter context and set correlation ID.
        
        Returns:
            The correlation ID
        """
        self._previous_id = get_correlation_id()
        return set_correlation_id(self.correlation_id)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and restore previous correlation ID."""
        if self._previous_id is not None:
            set_correlation_id(self._previous_id)
        else:
            clear_correlation_id()

