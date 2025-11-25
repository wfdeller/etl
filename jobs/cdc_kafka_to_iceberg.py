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
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

# Auto-detect project root
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Add lib to path
sys.path.insert(0, str(PROJECT_ROOT / 'lib'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from config_loader import get_source_config, get_kafka_config
from status_tracker import CDCStatusTracker

# Configure logging
log_dir = PROJECT_ROOT / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'cdc_consumer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session with Kafka and Iceberg support"""

    # Required environment variables
    catalog_name = os.environ.get('CATALOG_NAME')
    if not catalog_name:
        raise ValueError("CATALOG_NAME environment variable must be set")

    warehouse_path = os.environ.get('WAREHOUSE_PATH')
    if not warehouse_path:
        raise ValueError("WAREHOUSE_PATH environment variable must be set")

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


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


def ensure_table_exists(spark: SparkSession, namespace: str, table_name: str, sample_data: dict) -> bool:
    """
    Ensure Iceberg table exists, create if not

    Args:
        spark: SparkSession
        namespace: Iceberg namespace
        table_name: Table name
        sample_data: Sample record to infer schema

    Returns:
        bool: True if table exists or was created
    """

    catalog_name = os.environ.get('CATALOG_NAME')
    if not catalog_name:
        raise ValueError("CATALOG_NAME environment variable must be set")

    iceberg_table = f"{catalog_name}.{namespace}.{table_name}"

    try:
        # Check if table exists
        spark.table(iceberg_table)
        return True
    except:
        # Table doesn't exist - create it
        logger.info(f"Creating new table {iceberg_table} from CDC event")

        try:
            # Create DataFrame from sample data to infer schema
            df = spark.createDataFrame([sample_data])
            df.writeTo(iceberg_table).using("iceberg").create()

            logger.info(f"Created new table {iceberg_table} with {len(sample_data)} columns")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {iceberg_table}: {e}")
            return False
        
def apply_cdc_event(spark: SparkSession, namespace: str, table_name: str,
                    operation: str, before: dict, after: dict,
                    tracker: CDCStatusTracker = None, db_type: str = None,
                    event_scn: int = None):
    """
    Apply a CDC event to an Iceberg table

    Args:
        spark: SparkSession
        namespace: Iceberg namespace
        table_name: Target table name
        operation: 'c' (create/insert), 'u' (update), 'd' (delete), 'r' (read/snapshot)
        before: Before image (for updates and deletes)
        after: After image (for inserts and updates)
        tracker: Optional status tracker for recording new tables
        db_type: Database type ('oracle' or 'postgres')
        event_scn: SCN/LSN of this event
    """

    catalog_name = os.environ.get('CATALOG_NAME')
    if not catalog_name:
        raise ValueError("CATALOG_NAME environment variable must be set")

    iceberg_table = f"{catalog_name}.{namespace}.{table_name}"
    
    # Determine sample data for schema inference
    sample_data = after or before
    if not sample_data:
        logger.warning(f"No data in CDC event for {table_name}, skipping")
        return
    
    # Ensure table exists (auto-create for new tables)
    if not ensure_table_exists(spark, namespace, table_name, sample_data):
        logger.error(f"Could not ensure table {table_name} exists, skipping event")
        return
    
    try:
        if operation in ('c', 'r'):
            # Insert
            if after:
                df = spark.createDataFrame([after])
                df.writeTo(iceberg_table).option("mergeSchema", "true").append()
                logger.debug(f"INSERT into {table_name}")
        
        elif operation == 'u':
            # Update - use MERGE
            if after and before:
                df = spark.createDataFrame([after])
                df.createOrReplaceTempView("cdc_update")
                
                # Determine primary key (usually ROW_ID for Siebel)
                pk_col = 'ROW_ID' if 'ROW_ID' in after else list(after.keys())[0]
                
                spark.sql(f"""
                    MERGE INTO {iceberg_table} target
                    USING cdc_update source
                    ON target.{pk_col} = source.{pk_col}
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
                logger.debug(f"UPDATE {table_name} WHERE {pk_col} = {after.get(pk_col)}")
        
        elif operation == 'd':
            # Delete using DataFrame API to avoid SQL injection
            if before:
                pk_col = 'ROW_ID' if 'ROW_ID' in before else list(before.keys())[0]
                pk_value = before[pk_col]

                # Create temp view for delete matching
                delete_df = spark.createDataFrame([before])
                delete_df.createOrReplaceTempView("cdc_delete")

                spark.sql(f"""
                    DELETE FROM {iceberg_table} target
                    WHERE EXISTS (
                        SELECT 1 FROM cdc_delete source
                        WHERE target.{pk_col} = source.{pk_col}
                    )
                """)
                logger.debug(f"DELETE from {table_name} WHERE {pk_col} = {pk_value}")
        
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
    except:
        _known_tables_cache = set()        


def process_kafka_batch(spark: SparkSession, kafka_config: dict, source_config: dict,
                        tracker: CDCStatusTracker, table_scn_map: Dict[str, int],
                        batch_size: int = 1000):
    """
    Process a batch of Kafka messages
    
    Args:
        spark: SparkSession
        kafka_config: Kafka connection config
        source_config: Source database config
        tracker: CDC status tracker
        table_scn_map: Map of table -> starting SCN
        batch_size: Number of messages to process per batch
    """
    
    namespace = source_config['iceberg_namespace']
    topic_prefix = source_config['kafka_topic_prefix']
    db_type = source_config['database_type']
    
    # Build topic pattern (e.g., dev.siebel.SIEBEL.*)
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

        if batch_df.isEmpty():
            return

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

                # Extract source SCN/LSN
                source_info = payload.get('source', {})
                event_scn = source_info.get('scn') or source_info.get('lsn')

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
                    spark, namespace, table_name, operation, before, after,
                    tracker=tracker, db_type=db_type, event_scn=event_scn
                )
                events_processed += 1

                # Update CDC position periodically
                if events_processed % 100 == 0:
                    if db_type == 'oracle':
                        tracker.update_cdc_position(table_name, oracle_scn=event_scn)
                    else:
                        tracker.update_cdc_position(table_name, postgres_lsn=str(event_scn))

            except Exception as e:
                logger.error(f"Error processing message from {msg.get('topic')}: {e}")
                continue

        logger.info(f"Batch {batch_id}: processed {events_processed}, skipped {events_skipped}, new tables {new_tables_created}")
    


    # Start streaming query
    checkpoint_dir = PROJECT_ROOT / 'checkpoints' / namespace
    query = kafka_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", str(checkpoint_dir)) \
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
    
    # Create Spark session
    spark = create_spark_session(f"CDC-Consumer-{source_name}")

    # Initialize status tracker
    tracker = CDCStatusTracker(spark, namespace)

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
    
    # Start processing
    try:
        query = process_kafka_batch(
            spark, kafka_config, source_config, tracker, table_scn_map, batch_size
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
        help='Source name from sources.yaml (e.g., dev_siebel)'
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
        logger.error(f"CDC Consumer failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
