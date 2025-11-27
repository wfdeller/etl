#!/usr/bin/env python3
"""
CDC Kafka to Iceberg Consumer
Consumes Debezium CDC events from Kafka and applies them to Iceberg tables
Starts from SCN/LSN recorded in _cdc_status table after bulk load
"""

import sys
import os
import argparse
import logging
import json
import base64
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional

# Auto-detect project root
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Add lib to path
sys.path.insert(0, str(PROJECT_ROOT / 'lib'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DecimalType

from config_loader import get_source_config, get_kafka_config
from status_tracker import CDCStatusTracker
from schema_tracker import SchemaTracker
from spark_utils import SparkSessionFactory
from iceberg_utils import IcebergTableManager
from monitoring import MetricsCollector, StructuredLogger
from correlation import set_correlation_id, generate_correlation_id

# Configure logging
# For Databricks: use StreamHandler only (logs captured by driver)
# For local dev: add FileHandler if LOG_DIR is set
handlers = [logging.StreamHandler()]

log_dir_env = os.environ.get('LOG_DIR')
if log_dir_env:
    log_dir = Path(log_dir_env)
    log_dir.mkdir(exist_ok=True, parents=True)
    handlers.append(logging.FileHandler(log_dir / 'cdc_consumer.log'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(name)s - %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger(__name__)

# CDC position update interval (number of events between position updates)
# Default: 10 events (reduces risk of loss on crash vs previous 100)
# Set CDC_POSITION_UPDATE_INTERVAL environment variable to override
CDC_POSITION_UPDATE_INTERVAL = int(os.environ.get('CDC_POSITION_UPDATE_INTERVAL', '10'))


# Spark session creation moved to SparkSessionFactory.create()


def convert_timestamps_for_schema(data: Dict, schema: StructType) -> Dict:
    """
    Convert types for schema compatibility.

    Debezium sends:
    - Timestamps as integers (millis since epoch), but Spark TimestampType requires datetime objects
    - Numeric values as integers/floats, but Spark DecimalType requires Decimal objects

    Args:
        data: Dictionary containing CDC event data
        schema: Spark StructType schema with expected column types

    Returns:
        New dictionary with types converted to match schema
    """
    if not data or not schema:
        return data

    # Create a copy to avoid modifying original
    converted_data = data.copy()

    # Convert types based on schema
    for field in schema.fields:
        field_name = field.name
        if field_name not in converted_data:
            continue

        value = converted_data[field_name]
        if value is None:
            continue

        # Convert TimestampType
        if isinstance(field.dataType, TimestampType) and isinstance(value, int):
            try:
                # Convert milliseconds to seconds and create datetime
                converted_data[field_name] = datetime.fromtimestamp(value / 1000.0)
            except (ValueError, OSError) as e:
                logger.warning(f"Could not convert timestamp for {field_name}: {value} - {e}")

        # Convert DecimalType
        elif isinstance(field.dataType, DecimalType):
            if isinstance(value, (int, float)):
                try:
                    converted_data[field_name] = Decimal(str(value))
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not convert decimal for {field_name}: {value} - {e}")
            elif isinstance(value, str):
                # Handle base64-encoded values from Debezium (for Oracle RAW/BINARY types)
                # Also handle numeric strings
                try:
                    # Try to decode as base64 first
                    decoded = base64.b64decode(value)
                    # Convert bytes to numeric value
                    converted_data[field_name] = Decimal(str(int.from_bytes(decoded, byteorder='big', signed=True)))
                except Exception:
                    # If base64 decode fails, try to parse as string number
                    try:
                        converted_data[field_name] = Decimal(value)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Could not convert decimal for {field_name}: {value} - {e}, setting to None")
                        converted_data[field_name] = None

    return converted_data


def get_starting_position(spark: SparkSession, tracker: CDCStatusTracker, db_type: str) -> Dict[str, Optional[int]]:
    """
    Get the starting SCN/LSN from the status table
    Uses the minimum SCN/LSN from completed tables to ensure no data is missed
    
    Returns:
        dict: {'oracle_scn': int, 'postgres_lsn': str}
    """
    
    try:
        status_df = tracker.get_status()
        
        # Filter to completed tables only
        completed_df = status_df.filter(status_df.load_status == 'completed')
        
        if completed_df.count() == 0:
            logger.warning("No completed tables found in status table - starting from beginning")
            return {'oracle_scn': None, 'postgres_lsn': None}
        
        if db_type == 'oracle':
            # Get the minimum SCN from completed tables
            # This ensures we don't miss any changes
            min_scn_row = completed_df.agg({'oracle_scn': 'min'}).first()
            min_scn = min_scn_row[0]
            
            if min_scn is None:
                logger.warning("No SCN found in status table")
                return {'oracle_scn': None, 'postgres_lsn': None}
            
            logger.info(f"Starting CDC from Oracle SCN: {min_scn}")
            return {'oracle_scn': min_scn, 'postgres_lsn': None}
        
        elif db_type == 'postgres':
            # Get the minimum LSN from completed tables
            min_lsn_row = completed_df.agg({'postgres_lsn': 'min'}).first()
            min_lsn = min_lsn_row[0]
            
            if min_lsn is None:
                logger.warning("No LSN found in status table")
                return {'oracle_scn': None, 'postgres_lsn': None}
            
            logger.info(f"Starting CDC from Postgres LSN: {min_lsn}")
            return {'oracle_scn': None, 'postgres_lsn': min_lsn}
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    except Exception as e:
        logger.error(f"Error getting starting position: {e}")
        return {'oracle_scn': None, 'postgres_lsn': None}


def get_table_scn_map(tracker: CDCStatusTracker) -> Dict[str, int]:
    """
    Get a mapping of table_name -> SCN for all completed tables
    This allows per-table SCN tracking for filtering duplicates

    Returns:
        dict: {table_name: scn}
    """

    try:
        status_df = tracker.get_status()
        completed_df = status_df.filter(status_df.load_status == 'completed')

        scn_map = {}
        for row in completed_df.collect():
            if row['oracle_scn'] is not None:
                scn_map[row['table_name']] = row['oracle_scn']
            elif row['postgres_lsn'] is not None:
                scn_map[row['table_name']] = row['postgres_lsn']

        logger.info(f"Loaded SCN/LSN map for {len(scn_map)} tables")
        return scn_map

    except Exception as e:
        logger.error(f"Error loading SCN map: {e}")
        return {}


def get_primary_keys_map(tracker: CDCStatusTracker) -> Dict[str, list]:
    """
    Get a mapping of table_name -> primary_keys for all completed tables
    This allows dynamic PK-based MERGE/DELETE operations

    Returns:
        dict: {table_name: [pk_column1, pk_column2, ...]}
    """

    try:
        status_df = tracker.get_status()
        completed_df = status_df.filter(status_df.load_status == 'completed')

        pk_map = {}
        for row in completed_df.collect():
            table_name = row['table_name']
            # Row objects don't have .get() method, check if field exists
            primary_keys = row['primary_keys'] if 'primary_keys' in row.asDict() else []

            # Only store if we have valid primary keys
            if primary_keys and len(primary_keys) > 0:
                pk_map[table_name] = primary_keys

        logger.info(f"Loaded primary keys for {len(pk_map)} tables")
        return pk_map

    except Exception as e:
        logger.error(f"Error loading primary keys map: {e}")
        return {}


# Iceberg table operations moved to IcebergTableManager
        
def apply_cdc_event(spark: SparkSession, iceberg_mgr: IcebergTableManager, namespace: str, table_name: str,
                    operation: str, before: dict, after: dict,
                    tracker: CDCStatusTracker = None, db_type: str = None,
                    event_scn: int = None, primary_keys_map: Dict[str, list] = None,
                    schema_tracker: SchemaTracker = None, source_db: str = None):
    """
    Apply a CDC event to an Iceberg table

    Args:
        spark: SparkSession
        iceberg_mgr: IcebergTableManager instance
        namespace: Iceberg namespace
        table_name: Target table name
        operation: 'c' (create/insert), 'u' (update), 'd' (delete), 'r' (read/snapshot)
        before: Before image (for updates and deletes)
        after: After image (for inserts and updates)
        tracker: Optional status tracker for recording new tables
        db_type: Database type ('oracle' or 'postgres')
        event_scn: SCN/LSN of this event
        primary_keys_map: Map of table_name -> [pk_columns] for dynamic PK handling
        schema_tracker: Optional schema tracker for detecting schema changes
        source_db: Source database identifier for schema change tracking
    """

    iceberg_table = iceberg_mgr.get_full_table_name(namespace, table_name)

    # Determine sample data for schema inference
    sample_data = after or before
    if not sample_data:
        logger.warning(f"No data in CDC event for {table_name}, skipping")
        return

    # Ensure table exists (auto-create for new tables)
    if not iceberg_mgr.ensure_table_exists(namespace, table_name, sample_data):
        logger.error(f"Could not ensure table {table_name} exists, skipping event")
        return

    # Try to get existing table schema (more robust than inference)
    try:
        table_schema = spark.table(iceberg_table).schema
        logger.debug(f"{table_name}: using existing table schema with {len(table_schema.fields)} columns")
    except Exception:
        # Table doesn't exist or can't be read, will use schema inference
        table_schema = None
        logger.debug(f"{table_name}: will use schema inference")

    # Detect schema changes before applying CDC event
    if schema_tracker and source_db:
        try:
            # Get current schema from the event data
            if table_schema:
                # Convert timestamps for schema compatibility
                converted_sample = convert_timestamps_for_schema(sample_data, table_schema)
                # Use existing schema to create DataFrame
                current_df = spark.createDataFrame([converted_sample], schema=table_schema)
            else:
                # Fallback to inference
                current_df = spark.createDataFrame([sample_data])
            changes = schema_tracker.detect_and_record_changes(table_name, current_df.schema, source_db)

            if changes:
                for change in changes:
                    logger.warning(f"Schema change detected in {table_name}: {change['change_type']} - "
                                 f"column {change['column_name']} "
                                 f"({change['old_data_type']} -> {change['new_data_type']})")
        except Exception as e:
            logger.error(f"Error detecting schema changes for {table_name}: {e}")

    # Determine primary keys for MERGE/DELETE operations
    pk_cols = None
    if primary_keys_map and table_name in primary_keys_map:
        # Use discovered primary keys from status tracker
        pk_cols = primary_keys_map[table_name]
        logger.debug(f"{table_name}: using primary keys from metadata: {pk_cols}")
    else:
        # Fallback to ROWID (Oracle) or first column
        sample_data = after or before
        if sample_data:
            if 'ROWID' in sample_data:
                pk_cols = ['ROWID']
                logger.debug(f"{table_name}: using fallback primary key: ROWID")
            else:
                pk_cols = [list(sample_data.keys())[0]]
                logger.warning(f"{table_name}: no primary key metadata found, using first column: {pk_cols[0]}")

    try:
        if operation in ('c', 'r'):
            # Insert
            if after:
                # Convert timestamps if using explicit schema
                after_data = convert_timestamps_for_schema(after, table_schema) if table_schema else after
                df = spark.createDataFrame([after_data], schema=table_schema) if table_schema else spark.createDataFrame([after_data])
                df.writeTo(iceberg_table).option("mergeSchema", "true").append()
                logger.debug(f"INSERT into {table_name}")

        elif operation == 'u':
            # Update - use MERGE with dynamic PK handling (supports composite keys)
            if after and before and pk_cols:
                # Convert timestamps if using explicit schema
                after_data = convert_timestamps_for_schema(after, table_schema) if table_schema else after
                df = spark.createDataFrame([after_data], schema=table_schema) if table_schema else spark.createDataFrame([after_data])
                df.createOrReplaceTempView("cdc_update")

                # Build ON clause for composite keys: "target.pk1 = source.pk1 AND target.pk2 = source.pk2"
                on_conditions = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

                # Build debug message showing PK values
                pk_values = ", ".join([f"{pk}={after.get(pk)}" for pk in pk_cols])

                spark.sql(f"""
                    MERGE INTO {iceberg_table} target
                    USING cdc_update source
                    ON {on_conditions}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
                logger.debug(f"UPDATE {table_name} WHERE {pk_values}")

        elif operation == 'd':
            # Delete - supports composite keys
            if before and pk_cols:
                # Convert timestamps if using explicit schema
                before_data = convert_timestamps_for_schema(before, table_schema) if table_schema else before
                # Create temp view for delete matching
                delete_df = spark.createDataFrame([before_data], schema=table_schema) if table_schema else spark.createDataFrame([before_data])
                delete_df.createOrReplaceTempView("cdc_delete")

                # Build WHERE clause for composite keys
                where_conditions = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

                # Build debug message showing PK values
                pk_values = ", ".join([f"{pk}={before.get(pk)}" for pk in pk_cols])

                spark.sql(f"""
                    DELETE FROM {iceberg_table} target
                    WHERE EXISTS (
                        SELECT 1 FROM cdc_delete source
                        WHERE {where_conditions}
                    )
                """)
                logger.debug(f"DELETE from {table_name} WHERE {pk_values}")

        else:
            logger.warning(f"Unknown operation: {operation}")
        
        # Record that we've processed this new table (if tracker provided)
        if tracker and table_name not in get_known_tables_cache():
            add_to_known_tables_cache(table_name)
            if db_type == 'oracle':
                tracker.record_initial_load_complete(table_name, 1, oracle_scn=event_scn)
            elif db_type == 'postgres':
                tracker.record_initial_load_complete(table_name, 1, postgres_lsn=str(event_scn))
            logger.info(f"Recorded new table {table_name} in status tracker")
    
    except Exception as e:
        logger.error(f"Error applying CDC event to {table_name}: {e}")
        raise


# Simple cache to track known tables (avoid repeated status table updates)
_known_tables_cache = set()

def get_known_tables_cache():
    return _known_tables_cache

def add_to_known_tables_cache(table_name):
    _known_tables_cache.add(table_name)

def init_known_tables_cache(tracker: CDCStatusTracker):
    """Initialize cache with tables from status tracker"""
    global _known_tables_cache
    try:
        status_df = tracker.get_status()
        tables = [row.table_name for row in status_df.select("table_name").collect()]
        _known_tables_cache = set(tables)
        logger.info(f"Initialized known tables cache with {len(_known_tables_cache)} tables")
    except Exception as e:
        logger.warning(f"Could not initialize known tables cache: {e}")
        _known_tables_cache = set()        


def process_kafka_batch(spark: SparkSession, iceberg_mgr: IcebergTableManager, kafka_config: dict, source_config: dict,
                        tracker: CDCStatusTracker, table_scn_map: Dict[str, int],
                        primary_keys_map: Dict[str, list], schema_tracker: SchemaTracker = None, batch_size: int = 1000):
    """
    Process a batch of Kafka messages

    Args:
        spark: SparkSession
        kafka_config: Kafka connection config
        source_config: Source database config
        tracker: CDC status tracker
        table_scn_map: Map of table -> starting SCN
        primary_keys_map: Map of table -> [pk_columns] for dynamic PK handling
        schema_tracker: Optional schema tracker for detecting schema changes
        batch_size: Number of messages to process per batch
    """

    namespace = source_config['iceberg_namespace']
    topic_prefix = source_config['kafka_topic_prefix']
    db_type = source_config['database_type']

    # Initialize metrics collector for CDC monitoring
    metrics = MetricsCollector(job_name='cdc_consumer', source_name=namespace)
    structured_logger = StructuredLogger(job_name='cdc_consumer', source_name=namespace)

    # Extract source_db identifier for schema tracking (extract host from JDBC URL)
    jdbc_url = source_config['database_connection'].get('url', '')
    source_db = jdbc_url.split('@')[-1] if '@' in jdbc_url else jdbc_url

    # Build topic pattern (e.g., dev.mydb.SCHEMA.*)
    schema = source_config['database_connection']['schema']
    topic_pattern = f"{topic_prefix}.{schema}.*"
    
    logger.info(f"Subscribing to Kafka topics: {topic_pattern}")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
        .option("subscribePattern", topic_pattern) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", batch_size) \
        .load()
    
    def process_batch(batch_df, batch_id):
        """Process each micro-batch"""

        # Generate correlation ID for this batch
        set_correlation_id(generate_correlation_id())

        if batch_df.isEmpty():
            return

        # Track batch processing time and metrics
        import time as time_module
        batch_start_time = time_module.time()
        processing_timestamp_ms = int(batch_start_time * 1000)

        logger.info(f"Processing batch {batch_id} with {batch_df.count()} messages")

        # Process each message
        messages = batch_df.select(
            col("topic"),
            col("key").cast("string"),
            col("value").cast("string"),
            col("timestamp")
        ).collect()

        events_processed = 0
        events_skipped = 0
        new_tables_created = 0
        max_lag_seconds = 0.0

        for msg in messages:
            try:
                topic = msg['topic']
                value = msg['value']

                if not value:
                    continue

                # Parse the CDC event
                event = json.loads(value)
                payload = event.get('payload', event)

                # Extract table name from topic
                table_name = topic.split('.')[-1]

                # Extract operation and data
                operation = payload.get('op')
                before = payload.get('before')
                after = payload.get('after')

                # Extract source SCN/LSN and timestamp
                source_info = payload.get('source', {})
                event_scn = source_info.get('scn') or source_info.get('lsn')
                # Convert to int if it's a string (Debezium may send large numbers as strings)
                if event_scn and isinstance(event_scn, str):
                    event_scn = int(event_scn)

                # Calculate CDC lag from Debezium timestamp
                event_timestamp_ms = source_info.get('ts_ms')
                if event_timestamp_ms:
                    lag_ms = processing_timestamp_ms - event_timestamp_ms
                    lag_seconds = max(0, lag_ms / 1000.0)  # Ensure non-negative
                    max_lag_seconds = max(max_lag_seconds, lag_seconds)

                # Check if this is a known table
                is_new_table = table_name not in get_known_tables_cache()

                # Skip events before starting point ONLY for known tables
                if not is_new_table:
                    table_start_scn = table_scn_map.get(table_name)
                    if table_start_scn and event_scn and event_scn < table_start_scn:
                        events_skipped += 1
                        continue
                else:
                    logger.info(f"New table detected: {table_name}")
                    new_tables_created += 1

                # Apply the CDC event
                apply_cdc_event(
                    spark, iceberg_mgr, namespace, table_name, operation, before, after,
                    tracker=tracker, db_type=db_type, event_scn=event_scn,
                    primary_keys_map=primary_keys_map,
                    schema_tracker=schema_tracker, source_db=source_db
                )
                events_processed += 1

                # Update CDC position periodically (configurable via CDC_POSITION_UPDATE_INTERVAL)
                if events_processed % CDC_POSITION_UPDATE_INTERVAL == 0:
                    if db_type == 'oracle':
                        tracker.update_cdc_position(table_name, oracle_scn=event_scn)
                    else:
                        tracker.update_cdc_position(table_name, postgres_lsn=str(event_scn))

            except Exception as e:
                # Row objects don't have .get() method, use dict access
                topic = msg['topic'] if 'topic' in msg.asDict() else 'unknown'
                logger.error(f"Error processing message from {topic}: {e}")
                continue

        # Calculate batch metrics
        batch_duration = time_module.time() - batch_start_time
        throughput = events_processed / batch_duration if batch_duration > 0 else 0

        # Emit CDC metrics
        if max_lag_seconds > 0:
            metrics.emit_gauge('cdc_lag_seconds', max_lag_seconds,
                             unit='Seconds',
                             dimensions={'namespace': namespace})

        metrics.emit_gauge('cdc_throughput_records_per_sec', throughput,
                         unit='Count/Second',
                         dimensions={'namespace': namespace})

        metrics.emit_gauge('cdc_batch_processing_time', batch_duration,
                         unit='Seconds',
                         dimensions={'namespace': namespace})

        metrics.emit_counter('cdc_events_processed', count=events_processed,
                           dimensions={'namespace': namespace})

        metrics.emit_counter('cdc_events_skipped', count=events_skipped,
                           dimensions={'namespace': namespace})

        # Log structured batch completion event
        structured_logger.log_event('cdc_batch_complete', {
            'batch_id': batch_id,
            'events_processed': events_processed,
            'events_skipped': events_skipped,
            'new_tables_created': new_tables_created,
            'batch_duration_seconds': batch_duration,
            'throughput_records_per_sec': throughput,
            'max_lag_seconds': max_lag_seconds
        })

        logger.info(f"Batch {batch_id}: processed {events_processed}, skipped {events_skipped}, "
                   f"new tables {new_tables_created}, throughput {throughput:.1f} rec/s, "
                   f"max lag {max_lag_seconds:.1f}s")
    


    # Start streaming query
    # Use CHECKPOINT_PATH env var (supports s3://, dbfs://, file://)
    # Falls back to local path for development
    checkpoint_base = os.environ.get('CHECKPOINT_PATH', str(PROJECT_ROOT / 'checkpoints'))
    checkpoint_dir = f"{checkpoint_base}/{namespace}"

    logger.info(f"Using checkpoint location: {checkpoint_dir}")

    query = kafka_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query


def run_cdc_consumer(source_name: str, batch_size: int = 1000):
    """
    Main CDC consumer function
    
    Args:
        source_name: Source name from sources.yaml
        batch_size: Messages per batch
    """
    
    logger.info("="*80)
    logger.info(f"Starting CDC Consumer for source: {source_name}")
    logger.info("="*80)
    
    # Load configuration
    source_config = get_source_config(source_name)
    kafka_config = get_kafka_config()
    
    db_type = source_config['database_type']
    namespace = source_config['iceberg_namespace']
    
    logger.info(f"Database type: {db_type}")
    logger.info(f"Target namespace: {namespace}")
    logger.info(f"Kafka bootstrap: {kafka_config['bootstrap_servers']}")
    
    # Create Spark session with Kafka support
    spark = SparkSessionFactory.create(f"CDC-Consumer-{source_name}", db_type=None, enable_kafka=True)

    # Initialize utilities
    iceberg_mgr = IcebergTableManager(spark)
    tracker = CDCStatusTracker(spark, namespace)
    schema_tracker = SchemaTracker(spark, namespace)

    # Initialize known tables cache
    init_known_tables_cache(tracker)

    # Get starting position from bulk load
    start_position = get_starting_position(spark, tracker, db_type)

    if db_type == 'oracle' and start_position['oracle_scn']:
        logger.info(f"Will filter events before SCN: {start_position['oracle_scn']}")
    elif db_type == 'postgres' and start_position['postgres_lsn']:
        logger.info(f"Will filter events before LSN: {start_position['postgres_lsn']}")
    else:
        logger.warning("No starting position found - processing all events from Kafka")

    # Get per-table SCN map for filtering
    table_scn_map = get_table_scn_map(tracker)

    # Get per-table primary keys map for dynamic PK handling
    primary_keys_map = get_primary_keys_map(tracker)

    # Start processing
    try:
        query = process_kafka_batch(
            spark, iceberg_mgr, kafka_config, source_config, tracker, table_scn_map,
            primary_keys_map, schema_tracker, batch_size
        )
        
        logger.info("CDC Consumer started. Press Ctrl+C to stop.")
        query.awaitTermination()
    
    except KeyboardInterrupt:
        logger.info("Shutting down CDC Consumer...")
    
    except Exception as e:
        logger.error(f"CDC Consumer error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
        logger.info("CDC Consumer stopped.")


def main():
    parser = argparse.ArgumentParser(
        description='CDC Kafka to Iceberg Consumer'
    )
    parser.add_argument(
        '--source',
        required=True,
        help='Source name from sources.yaml (e.g., dev_source)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of messages to process per batch (default: 1000)'
    )
    
    args = parser.parse_args()
    
    try:
        run_cdc_consumer(args.source, args.batch_size)
    except Exception as e:
        logger.error(f"CDC Consumer failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
