"""
Monitoring and metrics collection for ETL pipelines
Supports CloudWatch (AWS), structured logging, and performance tracking
"""

import logging
import time
import json
from datetime import datetime
from typing import Dict, Optional, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Collects and emits metrics for ETL operations

    Supports:
    - CloudWatch metrics (when boto3 available and AWS credentials configured)
    - Structured JSON logging for downstream analysis
    - In-memory metrics for reporting
    """

    def __init__(self, job_name: str, source_name: str, enable_cloudwatch: bool = True):
        """
        Initialize metrics collector

        Args:
            job_name: Name of the job (e.g., 'bulk_load', 'cdc_consumer')
            source_name: Source identifier (e.g., 'dev_source')
            enable_cloudwatch: Attempt to use CloudWatch if available
        """
        self.job_name = job_name
        self.source_name = source_name
        self.enable_cloudwatch = enable_cloudwatch
        self.cloudwatch_client = None
        self.metrics_buffer = []

        # Try to initialize CloudWatch client
        if enable_cloudwatch:
            try:
                import boto3
                self.cloudwatch_client = boto3.client('cloudwatch')
                logger.info("CloudWatch metrics enabled")
            except ImportError:
                logger.warning("boto3 not available - CloudWatch metrics disabled (pip install boto3)")
            except Exception as e:
                logger.warning(f"CloudWatch client initialization failed: {e} - metrics will be logged only")

    def emit_metric(self, metric_name: str, value: float, unit: str = 'Count',
                   dimensions: Optional[Dict[str, str]] = None):
        """
        Emit a metric to CloudWatch and structured logs

        Args:
            metric_name: Metric name (e.g., 'RecordsProcessed', 'TableLoadDuration')
            value: Metric value
            unit: CloudWatch unit (Count, Seconds, Bytes, etc.)
            dimensions: Additional dimensions for the metric
        """
        timestamp = datetime.utcnow()

        # Build dimensions
        metric_dimensions = {
            'JobName': self.job_name,
            'SourceName': self.source_name
        }
        if dimensions:
            metric_dimensions.update(dimensions)

        # Store in buffer
        metric_data = {
            'timestamp': timestamp.isoformat(),
            'metric_name': metric_name,
            'value': value,
            'unit': unit,
            'dimensions': metric_dimensions
        }
        self.metrics_buffer.append(metric_data)

        # Log structured metric
        logger.info(f"METRIC: {json.dumps(metric_data)}")

        # Send to CloudWatch if available
        if self.cloudwatch_client:
            try:
                self.cloudwatch_client.put_metric_data(
                    Namespace='ETL/Pipeline',
                    MetricData=[{
                        'MetricName': metric_name,
                        'Value': value,
                        'Unit': unit,
                        'Timestamp': timestamp,
                        'Dimensions': [
                            {'Name': k, 'Value': v}
                            for k, v in metric_dimensions.items()
                        ]
                    }]
                )
            except Exception as e:
                logger.warning(f"Failed to send metric to CloudWatch: {e}")

    def emit_counter(self, metric_name: str, count: int = 1,
                    dimensions: Optional[Dict[str, str]] = None):
        """
        Emit a counter metric

        Args:
            metric_name: Counter name
            count: Count value (default: 1)
            dimensions: Additional dimensions
        """
        self.emit_metric(metric_name, float(count), unit='Count', dimensions=dimensions)

    def emit_duration(self, metric_name: str, duration_seconds: float,
                     dimensions: Optional[Dict[str, str]] = None):
        """
        Emit a duration metric

        Args:
            metric_name: Duration metric name
            duration_seconds: Duration in seconds
            dimensions: Additional dimensions
        """
        self.emit_metric(metric_name, duration_seconds, unit='Seconds', dimensions=dimensions)

    def emit_bytes(self, metric_name: str, bytes_count: int,
                  dimensions: Optional[Dict[str, str]] = None):
        """
        Emit a bytes metric

        Args:
            metric_name: Bytes metric name
            bytes_count: Number of bytes
            dimensions: Additional dimensions
        """
        self.emit_metric(metric_name, float(bytes_count), unit='Bytes', dimensions=dimensions)

    @contextmanager
    def track_duration(self, operation_name: str, dimensions: Optional[Dict[str, str]] = None):
        """
        Context manager to track operation duration

        Usage:
            with metrics.track_duration('table_load', {'table': 'S_CONTACT'}):
                # ... perform operation ...
                pass

        Args:
            operation_name: Operation name for the metric
            dimensions: Additional dimensions
        """
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.emit_duration(f"{operation_name}_duration", duration, dimensions=dimensions)

    def emit_success(self, operation_name: str, dimensions: Optional[Dict[str, str]] = None):
        """
        Emit a success metric

        Args:
            operation_name: Operation name
            dimensions: Additional dimensions
        """
        self.emit_counter(f"{operation_name}_success", count=1, dimensions=dimensions)

    def emit_failure(self, operation_name: str, error_type: str = 'Unknown',
                    dimensions: Optional[Dict[str, str]] = None):
        """
        Emit a failure metric

        Args:
            operation_name: Operation name
            error_type: Type of error
            dimensions: Additional dimensions
        """
        failure_dimensions = dimensions or {}
        failure_dimensions['ErrorType'] = error_type
        self.emit_counter(f"{operation_name}_failure", count=1, dimensions=failure_dimensions)

    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Get summary of collected metrics

        Returns:
            dict: Metrics summary
        """
        summary = {
            'total_metrics': len(self.metrics_buffer),
            'job_name': self.job_name,
            'source_name': self.source_name,
            'cloudwatch_enabled': self.cloudwatch_client is not None
        }

        # Aggregate by metric name
        by_metric = {}
        for metric in self.metrics_buffer:
            name = metric['metric_name']
            if name not in by_metric:
                by_metric[name] = {
                    'count': 0,
                    'sum': 0.0,
                    'min': float('inf'),
                    'max': float('-inf')
                }

            value = metric['value']
            by_metric[name]['count'] += 1
            by_metric[name]['sum'] += value
            by_metric[name]['min'] = min(by_metric[name]['min'], value)
            by_metric[name]['max'] = max(by_metric[name]['max'], value)

        summary['metrics'] = by_metric
        return summary


class StructuredLogger:
    """
    Structured logging for ETL events
    Emits JSON-formatted log lines for downstream analysis
    """

    def __init__(self, job_name: str, source_name: str):
        """
        Initialize structured logger

        Args:
            job_name: Job name
            source_name: Source identifier
        """
        self.job_name = job_name
        self.source_name = source_name
        self.logger = logging.getLogger(__name__)

    def log_event(self, event_type: str, details: Dict[str, Any],
                  level: str = 'INFO', table_name: Optional[str] = None):
        """
        Log a structured event

        Args:
            event_type: Event type (e.g., 'table_load_start', 'schema_change')
            details: Event details
            level: Log level (INFO, WARNING, ERROR)
            table_name: Optional table name
        """
        event = {
            'timestamp': datetime.utcnow().isoformat(),
            'job_name': self.job_name,
            'source_name': self.source_name,
            'event_type': event_type,
            'details': details
        }

        if table_name:
            event['table_name'] = table_name

        log_line = f"EVENT: {json.dumps(event)}"

        if level == 'INFO':
            self.logger.info(log_line)
        elif level == 'WARNING':
            self.logger.warning(log_line)
        elif level == 'ERROR':
            self.logger.error(log_line)
        else:
            self.logger.info(log_line)

    def log_table_start(self, table_name: str, details: Optional[Dict] = None):
        """Log table processing start"""
        self.log_event('table_processing_start', details or {}, table_name=table_name)

    def log_table_complete(self, table_name: str, record_count: int,
                          duration_seconds: float, details: Optional[Dict] = None):
        """Log table processing completion"""
        event_details = {
            'record_count': record_count,
            'duration_seconds': duration_seconds,
            **(details or {})
        }
        self.log_event('table_processing_complete', event_details, table_name=table_name)

    def log_table_failure(self, table_name: str, error: str, details: Optional[Dict] = None):
        """Log table processing failure"""
        event_details = {
            'error': error,
            **(details or {})
        }
        self.log_event('table_processing_failure', event_details,
                      level='ERROR', table_name=table_name)

    def log_job_start(self, details: Optional[Dict] = None):
        """Log job start"""
        self.log_event('job_start', details or {})

    def log_job_complete(self, tables_processed: int, total_records: int,
                        duration_seconds: float, details: Optional[Dict] = None):
        """Log job completion"""
        event_details = {
            'tables_processed': tables_processed,
            'total_records': total_records,
            'duration_seconds': duration_seconds,
            **(details or {})
        }
        self.log_event('job_complete', event_details)

    def log_job_failure(self, error: str, details: Optional[Dict] = None):
        """Log job failure"""
        event_details = {
            'error': error,
            **(details or {})
        }
        self.log_event('job_failure', event_details, level='ERROR')


class PerformanceTracker:
    """
    Track performance metrics for operations
    Useful for identifying bottlenecks
    """

    def __init__(self):
        """Initialize performance tracker"""
        self.operations = {}

    @contextmanager
    def track(self, operation_name: str, metadata: Optional[Dict] = None):
        """
        Track an operation's performance

        Args:
            operation_name: Name of the operation
            metadata: Additional metadata
        """
        start_time = time.time()
        start_memory = self._get_memory_usage()

        try:
            yield
            success = True
        except Exception:
            success = False
            raise
        finally:
            duration = time.time() - start_time
            end_memory = self._get_memory_usage()
            memory_delta = end_memory - start_memory if end_memory and start_memory else None

            if operation_name not in self.operations:
                self.operations[operation_name] = []

            self.operations[operation_name].append({
                'duration': duration,
                'memory_delta': memory_delta,
                'success': success,
                'metadata': metadata or {},
                'timestamp': datetime.utcnow().isoformat()
            })

    @staticmethod
    def _get_memory_usage() -> Optional[int]:
        """Get current memory usage in bytes"""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            return process.memory_info().rss
        except ImportError:
            return None
        except Exception:
            return None

    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary"""
        summary = {}

        for operation, records in self.operations.items():
            durations = [r['duration'] for r in records]
            successes = sum(1 for r in records if r.get('success', True))

            summary[operation] = {
                'count': len(records),
                'success_count': successes,
                'failure_count': len(records) - successes,
                'avg_duration': sum(durations) / len(durations) if durations else 0,
                'min_duration': min(durations) if durations else 0,
                'max_duration': max(durations) if durations else 0,
                'total_duration': sum(durations)
            }

        return summary
