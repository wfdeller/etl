"""
Correlation ID management for distributed tracing

This module provides correlation ID generation and management for tracking
requests across the ETL pipeline (Kafka → Spark → Iceberg).

Correlation IDs enable:
- End-to-end request tracking
- Distributed tracing in monitoring tools (Dynatrace, CloudWatch)
- Log correlation across services
- Troubleshooting multi-stage pipelines
"""

import uuid
from contextvars import ContextVar
from typing import Optional

# Thread-safe context variable for correlation ID
correlation_id: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)


def generate_correlation_id() -> str:
    """
    Generate a unique correlation ID for request tracing.

    Returns:
        str: UUID-based correlation ID

    Example:
        >>> cid = generate_correlation_id()
        >>> print(cid)
        'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
    """
    return str(uuid.uuid4())


def get_correlation_id() -> str:
    """
    Get the current correlation ID or generate a new one if not set.

    This is the primary method for accessing the correlation ID in your code.
    If no correlation ID exists in the current context, a new one is
    automatically generated and stored.

    Returns:
        str: Current or new correlation ID

    Example:
        >>> # First call generates and stores ID
        >>> cid1 = get_correlation_id()
        >>> # Subsequent calls return the same ID
        >>> cid2 = get_correlation_id()
        >>> assert cid1 == cid2
    """
    cid = correlation_id.get()
    if cid is None:
        cid = generate_correlation_id()
        correlation_id.set(cid)
    return cid


def set_correlation_id(cid: str) -> None:
    """
    Set the correlation ID for the current context.

    Use this when propagating correlation IDs from:
    - Kafka message headers
    - HTTP request headers
    - Parent process/job

    Args:
        cid: Correlation ID to set

    Example:
        >>> # Propagate from Kafka message header
        >>> kafka_cid = kafka_record.headers.get('correlation_id')
        >>> if kafka_cid:
        >>>     set_correlation_id(kafka_cid)
    """
    correlation_id.set(cid)


def clear_correlation_id() -> None:
    """
    Clear the correlation ID from the current context.

    Useful for cleanup between processing batches or requests.

    Example:
        >>> set_correlation_id('test-id')
        >>> assert get_correlation_id() == 'test-id'
        >>> clear_correlation_id()
        >>> # Next call to get_correlation_id() will generate new ID
    """
    correlation_id.set(None)


def with_correlation_id(cid: Optional[str] = None):
    """
    Context manager for scoped correlation ID.

    Args:
        cid: Optional correlation ID. If None, generates new ID.

    Example:
        >>> with with_correlation_id('my-custom-id'):
        >>>     print(get_correlation_id())  # Prints: my-custom-id
        >>>     # All operations here use the same correlation ID
        >>> # Correlation ID is cleared when exiting context
    """
    class CorrelationContext:
        def __init__(self, correlation_id_value: Optional[str]):
            self.cid = correlation_id_value or generate_correlation_id()
            self.token = None

        def __enter__(self):
            self.token = correlation_id.set(self.cid)
            return self.cid

        def __exit__(self, exc_type, exc_val, exc_tb):
            correlation_id.reset(self.token)
            return False

    return CorrelationContext(cid)
