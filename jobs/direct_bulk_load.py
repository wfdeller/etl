#!/usr/bin/env python3
"""
Unified direct bulk load from Oracle/Postgres to Iceberg
- Supports both Oracle and Postgres databases
- Checkpoint/resume capability
- Parallel table processing
- Configurable retry logic
- Proper error handling and logging
"""

import sys
import os
import argparse
import logging
import json
import time
import shutil
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Auto-detect project root
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Add lib to path
sys.path.insert(0, str(PROJECT_ROOT / 'lib'))

from config_loader import get_source_config, get_kafka_config
from status_tracker import CDCStatusTracker
from schema_tracker import SchemaTracker
from extractors import get_extractor
from monitoring import MetricsCollector, StructuredLogger
from data_quality import DataQualityChecker
from pyspark.sql import SparkSession
from spark_utils import SparkSessionFactory
from jdbc_utils import DatabaseConnectionBuilder
from iceberg_utils import IcebergTableManager

# Configure logging
# For Databricks: use StreamHandler only (logs captured by driver)
# For local dev: add FileHandler if LOG_DIR is set
handlers = [logging.StreamHandler()]

log_dir_env = os.environ.get('LOG_DIR')
if log_dir_env:
    log_dir = Path(log_dir_env)
    log_dir.mkdir(exist_ok=True, parents=True)
    handlers.append(logging.FileHandler(log_dir / 'direct_load.log'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(name)s - %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger(__name__)

# Global lock for thread-safe operations
write_lock = Lock()


# Validation moved to DatabaseConnectionBuilder.validate_config()


# Spark session creation moved to SparkSessionFactory.create()


# JDBC connection building moved to DatabaseConnectionBuilder.build_jdbc_config()


def get_scn_or_lsn(spark: SparkSession, jdbc_config: dict, db_type: str) -> dict:
    """Get current SCN (Oracle) or LSN (Postgres) before starting load"""

    if db_type == 'oracle':
        scn_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("query", "SELECT current_scn FROM v$database") \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .load()

        scn = int(scn_df.first()[0])
        logger.info(f"Oracle SCN captured: {scn}")
        return {'oracle_scn': scn, 'postgres_lsn': None}

    elif db_type == 'postgres':
        lsn_df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_config['url']) \
            .option("query", "SELECT pg_current_wal_lsn()::text") \
            .option("user", jdbc_config['user']) \
            .option("password", jdbc_config['password']) \
            .load()

        lsn = lsn_df.first()[0]
        logger.info(f"Postgres LSN captured: {lsn}")
        return {'oracle_scn': None, 'postgres_lsn': lsn}


def get_completed_tables(tracker: CDCStatusTracker) -> set:
    """Get set of tables that have already been completed"""
    
    try:
        status_df = tracker.get_status()
        completed = status_df.filter(
            status_df.load_status == 'completed'
        ).select('table_name').collect()
        
        completed_set = {row.table_name for row in completed}
        logger.info(f"Found {len(completed_set)} already completed tables")
        return completed_set
    except Exception as e:
        logger.warning(f"Could not get completed tables: {e}")
        return set()


# Iceberg table writing moved to IcebergTableManager.write_table()

def process_single_table(table_name: str, extractor, tracker, schema_tracker, iceberg_mgr, namespace: str,
                        jdbc_config: dict, scn_lsn: dict, parallel_tables: int,
                        skip_empty: bool, max_retries: int, retry_backoff: int,
                        tables_with_long: dict):
    """
    Process a single table with retry logic

    Returns:
        tuple: (success: bool, table_name: str, record_count: int, error: str)
    """

    import threading
    thread_id = threading.current_thread().name

    logger.info(f"{table_name}: Processing table: {table_name}")

    # Idempotency check: skip if this load already completed
    # Check if there's a completed load with the same SCN/LSN
    try:
        from pyspark.sql.functions import col, lit

        # Build filter conditions based on which SCN/LSN values are present
        scn_filter = None
        lsn_filter = None

        if scn_lsn.get('oracle_scn') is not None:
            scn_filter = (col("oracle_scn") == lit(scn_lsn['oracle_scn']))

        if scn_lsn.get('postgres_lsn') is not None:
            lsn_filter = (col("postgres_lsn") == lit(scn_lsn['postgres_lsn']))

        # Combine filters
        base_filter = (col("table_name") == lit(table_name)) & (col("load_status") == lit("completed"))

        if scn_filter is not None and lsn_filter is not None:
            scn_lsn_filter = scn_filter | lsn_filter
        elif scn_filter is not None:
            scn_lsn_filter = scn_filter
        elif lsn_filter is not None:
            scn_lsn_filter = lsn_filter
        else:
            # No SCN/LSN to check, skip idempotency check
            scn_lsn_filter = None

        if scn_lsn_filter is not None:
            existing_load = tracker.spark.table(tracker.status_table) \
                .filter(base_filter & scn_lsn_filter) \
                .first()
        else:
            existing_load = None

        if existing_load:
            logger.info(f"{table_name}: SKIP: {table_name} already loaded at SCN/LSN "
                       f"(SCN: {scn_lsn['oracle_scn']}, LSN: {scn_lsn['postgres_lsn']}) - "
                       f"{existing_load['record_count']} records - skipping duplicate load")
            return (True, table_name, existing_load['record_count'], None)
    except Exception as e:
        logger.warning(f"{table_name}: Could not check for duplicate load: {e} - proceeding with load")

    for attempt in range(max_retries + 1):
        try:
            # Extract table
            df, record_count, metadata = extractor.extract_table(
                table_name, scn_lsn, parallel_tables
            )

            # Extract primary keys from metadata
            primary_keys = metadata.get('primary_keys', [])

            # Track tables with LONG columns (Oracle only)
            if metadata.get('long_columns'):
                with write_lock:
                    tables_with_long[table_name] = metadata['long_columns']

            # Handle skipped tables (LONG columns with skip_table policy)
            if df is None:
                logger.info(f"{table_name}: SKIP: {table_name} (excluded by policy)")
                return (True, table_name, 0, None)

            # Skip empty tables if configured
            if record_count == 0:
                if skip_empty:
                    logger.info(f"{table_name}: SKIP: {table_name} is empty - will be created by CDC when data arrives")
                    return (True, table_name, 0, None)
                else:
                    # Create empty table with schema
                    logger.info(f"{table_name}: Creating empty table {table_name} with schema only")
                    # df already has the schema, just write it
                    with write_lock:
                        iceberg_mgr.write_table(df, namespace, table_name)

                        # Record baseline schema
                        source_db = jdbc_config['url'].split('@')[-1] if '@' in jdbc_config['url'] else jdbc_config['url']
                        schema_tracker.record_baseline_schema(table_name, df.schema, source_db)

                    # Record completion with 0 records
                    with write_lock:
                        tracker.record_initial_load_complete(
                            table_name=table_name,
                            record_count=0,
                            primary_keys=primary_keys,
                            oracle_scn=scn_lsn['oracle_scn'] + 1 if scn_lsn['oracle_scn'] else None,
                            postgres_lsn=scn_lsn['postgres_lsn']
                        )

                    logger.info(f"{table_name}: SUCCESS: {table_name} complete - 0 records (schema only)")
                    return (True, table_name, 0, None)

            # Record start in status table
            with write_lock:
                tracker.record_initial_load_start(
                    table_name=table_name,
                    source_db=jdbc_config['url'].split('@')[-1] if '@' in jdbc_config['url'] else jdbc_config['url'],
                    primary_keys=primary_keys,
                    oracle_scn=scn_lsn['oracle_scn'],
                    postgres_lsn=scn_lsn['postgres_lsn']
                )

            # Write to Iceberg
            with write_lock:
                iceberg_mgr.write_table(df, namespace, table_name)

                # Record baseline schema
                source_db = jdbc_config['url'].split('@')[-1] if '@' in jdbc_config['url'] else jdbc_config['url']
                schema_tracker.record_baseline_schema(table_name, df.schema, source_db)

            # Unpersist if it was persisted
            if metadata.get('persisted'):
                df.unpersist()

            # Record completion
            with write_lock:
                tracker.record_initial_load_complete(
                    table_name=table_name,
                    record_count=record_count,
                    primary_keys=primary_keys,
                    oracle_scn=scn_lsn['oracle_scn'] + 1 if scn_lsn['oracle_scn'] else None,
                    postgres_lsn=scn_lsn['postgres_lsn']
                )

            logger.info(f"{table_name}: SUCCESS: {table_name} complete - {record_count:,} records")
            return (True, table_name, record_count, None)

        except Exception as e:
            error_msg = str(e)

            if attempt < max_retries:
                wait_time = retry_backoff ** attempt
                logger.warning(f"{table_name}: Attempt {attempt + 1}/{max_retries + 1} failed for {table_name}: {error_msg}")
                logger.warning(f"{table_name}: -- Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"{table_name}: FAILED: {table_name} after {max_retries + 1} attempts - {error_msg}")
                import traceback
                logger.error(traceback.format_exc())

                # Record failure
                with write_lock:
                    tracker.record_load_failure(table_name, error_msg[:500])  # Truncate long errors

                return (False, table_name, 0, error_msg)

    return (False, table_name, 0, "Max retries exceeded")



def generate_debezium_config(source_config: dict, scn_lsn: dict, output_path: str):
    """Generate Debezium connector configuration with correct starting point"""
    
    db_type = source_config['database_type']
    db_config = source_config['database_connection']
    
    if db_type == 'oracle':
        config = {
            "name": f"{source_config['kafka_topic_prefix'].replace('.', '-')}-cdc",
            "config": {
                "connector.class": "io.debezium.connector.oracle.OracleConnector",
                "database.hostname": db_config['host'],
                "database.port": db_config['port'],
                "database.user": db_config['username'],
                "database.password": db_config['password'],
                "database.dbname": db_config['service_name'],
                "database.server.name": source_config['kafka_topic_prefix'].split('.')[0],
                "table.include.list": rf"{db_config['schema']}\..*",
                "snapshot.mode": "schema_only",
                "log.mining.start.scn": str(scn_lsn['oracle_scn'] + 1),
                "log.mining.strategy": "online_catalog",
                "log.mining.batch.size.default": "10000",
                "tombstones.on.delete": "false",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter"
            }
        }
    
    elif db_type == 'postgres':
        config = {
            "name": f"{source_config['kafka_topic_prefix'].replace('.', '-')}-cdc",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": db_config['host'],
                "database.port": db_config['port'],
                "database.user": db_config['username'],
                "database.password": db_config['password'],
                "database.dbname": db_config['database'],
                "database.server.name": source_config['kafka_topic_prefix'].split('.')[0],
                "table.include.list": rf"{db_config['schema']}\..*",
                "snapshot.mode": "never",
                "plugin.name": "pgoutput",
                "publication.autocreate.mode": "filtered",
                "slot.name": f"debezium_{source_config['kafka_topic_prefix'].replace('.', '_')}",
                "tombstones.on.delete": "false",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter"
            }
        }
    
    with open(output_path, 'w') as f:
        json.dump(config, f, indent=2)
    
    logger.info(f"Debezium config written to {output_path}")


def direct_bulk_load(
    source_name: str,
    parallel_tables: int = None,
    parallel_workers: int = None,
    table_filter: str = None,
    resume: bool = True
):
    """
    Main direct load function with parallel processing and checkpoint/resume
    
    Args:
        source_name: Name from sources.yaml
        parallel_tables: Number of partitions per table (overrides config)
        parallel_workers: Number of tables to process simultaneously (overrides config)
        table_filter: SQL LIKE pattern to filter tables (e.g., 'S_%')
        resume: Skip already completed tables
    """
    
    start_time = datetime.now()
    logger.info("="*80)
    logger.info(f"Starting direct bulk load for source: {source_name}")
    logger.info("="*80)
    
    # Load configuration
    source_config = get_source_config(source_name)

    # Validate configuration
    DatabaseConnectionBuilder.validate_config(source_config, source_name)
    logger.info(f"Configuration validated successfully for source '{source_name}'")
    
    db_type = source_config['database_type']
    namespace = source_config['iceberg_namespace']
    
    # Get bulk load config with defaults
    bulk_config = source_config.get('bulk_load', {})
    
    # Override with command line args if provided
    if parallel_tables is None:
        parallel_tables = bulk_config.get('parallel_tables', 8)
    
    if parallel_workers is None:
        parallel_workers = bulk_config.get('parallel_workers', 1)
    
    skip_empty = bulk_config.get('skip_empty_tables', True)
    max_retries = bulk_config.get('max_retries', 3)
    retry_backoff = bulk_config.get('retry_backoff', 2)
    checkpoint_enabled = bulk_config.get('checkpoint_enabled', True) and resume
    
    logger.info(f"Database type: {db_type}")
    logger.info(f"Target namespace: {namespace}")
    logger.info(f"Parallel partitions per table: {parallel_tables}")
    logger.info(f"Parallel table workers: {parallel_workers}")
    logger.info(f"Max retries per table: {max_retries}")
    logger.info(f"Checkpoint/Resume: {checkpoint_enabled}")
    
    # Create Spark session
    spark = SparkSessionFactory.create(f"BulkLoad-{source_name}", db_type)

    # Get catalog name from environment
    catalog = os.environ.get('CATALOG_NAME')
    if not catalog:
        raise ValueError("CATALOG_NAME environment variable must be set")

    # Initialize utilities
    iceberg_mgr = IcebergTableManager(spark, catalog)
    tracker = CDCStatusTracker(spark, namespace, catalog)
    schema_tracker = SchemaTracker(spark, namespace, catalog)

    # Get JDBC connection details
    jdbc_config = DatabaseConnectionBuilder.build_jdbc_config(source_config)
    
    # STEP 1: Capture SCN/LSN BEFORE starting load
    scn_lsn = get_scn_or_lsn(spark, jdbc_config, db_type)
    
    # STEP 2: Get extractor
    extractor = get_extractor(spark, jdbc_config, db_type, bulk_config)
    
    # STEP 3: Get table list
    all_tables = extractor.get_table_list(table_filter)
    
    if not all_tables:
        logger.error("No tables found to process!")
        return
    
    # STEP 4: Apply checkpoint/resume
    tables_to_process = all_tables
    if checkpoint_enabled:
        completed_tables = get_completed_tables(tracker)
        tables_to_process = [t for t in all_tables if t not in completed_tables]
        
        if len(tables_to_process) < len(all_tables):
            logger.info(f"Resuming: {len(completed_tables)} tables already completed")
            logger.info(f"Processing remaining {len(tables_to_process)} tables")
    
    if not tables_to_process:
        logger.info("All tables already completed!")
        return
    
    # STEP 5: Process tables in parallel
    total_records = 0
    success_count = 0
    failed_tables = []
    tables_with_long = {}  # Track tables with LONG columns
    
    logger.info(f"{'='*80}")
    logger.info(f"Processing {len(tables_to_process)} tables with {parallel_workers} parallel workers")
    logger.info(f"{'='*80}\n")
    
    if parallel_workers == 1:
        # Sequential processing
        for i, table_name in enumerate(tables_to_process, 1):
            logger.info(f"Table {i}/{len(tables_to_process)}: {table_name}")
            
            success, table, records, error = process_single_table(
                table_name, extractor, tracker, schema_tracker, iceberg_mgr, namespace, jdbc_config,
                scn_lsn, parallel_tables, skip_empty, max_retries,
                retry_backoff, tables_with_long
            )
            
            if success:
                success_count += 1
                total_records += records
            else:
                failed_tables.append(table_name)
    
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = {
                executor.submit(
                    process_single_table,
                    table_name, extractor, tracker, schema_tracker, iceberg_mgr, namespace, jdbc_config,
                    scn_lsn, parallel_tables, skip_empty, max_retries,
                    retry_backoff, tables_with_long
                ): table_name
                for table_name in tables_to_process
            }
            
            completed = 0
            for future in as_completed(futures):
                table_name = futures[future]
                completed += 1
                
                try:
                    success, table, records, error = future.result()
                    
                    logger.info(f"Progress: {completed}/{len(tables_to_process)} tables completed")
                    
                    if success:
                        success_count += 1
                        total_records += records
                    else:
                        failed_tables.append(table_name)
                        
                except Exception as e:
                    logger.error(f"Unexpected error processing {table_name}: {e}")
                    failed_tables.append(table_name)
    
    # STEP 6: Generate Debezium config
    config_dir = os.environ.get('CONFIG_DIR')
    if not config_dir:
        raise ValueError("CONFIG_DIR environment variable must be set")
    config_output = f"{config_dir}/debezium_{source_name}_cdc.json"
    generate_debezium_config(source_config, scn_lsn, config_output)
    
    # STEP 7: Summary
    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"{'='*80}")
    logger.info(f"DIRECT LOAD COMPLETE")
    logger.info(f"{'='*80}")
    logger.info(f"Duration: {duration:.1f} seconds ({duration/3600:.2f} hours)")
    logger.info(f"Tables processed: {success_count}/{len(tables_to_process)}")
    logger.info(f"Total records: {total_records:,}")
    if duration > 0:
        logger.info(f"Average speed: {total_records/duration:.0f} records/sec")
    
    if db_type == 'oracle':
        logger.info(f"Starting SCN: {scn_lsn['oracle_scn']}")
        logger.info(f"CDC should start from SCN: {scn_lsn['oracle_scn'] + 1}")
    else:
        logger.info(f"Starting LSN: {scn_lsn['postgres_lsn']}")
        logger.info(f"CDC should start from LSN: {scn_lsn['postgres_lsn']}")
    
    if failed_tables:
        logger.warning(f"\nFailed tables ({len(failed_tables)}):")
        for table in failed_tables:
            logger.warning(f"  - {table}")
    
    # Report on LONG columns (Oracle only)
    if db_type == 'oracle' and tables_with_long:
        logger.warning(f"{'='*80}")
        logger.warning(f"TABLES WITH EXCLUDED LONG COLUMNS ({len(tables_with_long)} tables)")
        logger.warning(f"{'='*80}")
        for table, cols in sorted(tables_with_long.items()):
            logger.warning(f"  {table}: {', '.join(cols)}")
    
    logger.info(f"{'='*80}")


def drop_namespace_tables(spark: SparkSession, namespace: str, catalog: str, auto_confirm: bool = False):
    """
    Drop all tables in the Iceberg namespace (fresh start)

    Args:
        spark: SparkSession
        namespace: Iceberg namespace (e.g., 'bronze.dev_source')
        catalog: Iceberg catalog name (required)
        auto_confirm: Skip confirmation prompt

    Returns:
        bool: True if tables were dropped, False if cancelled
    """

    full_namespace = f"{catalog}.{namespace}"

    try:
        # Get list of tables
        tables_df = spark.sql(f"SHOW TABLES IN {full_namespace}")
        tables = [row.tableName for row in tables_df.collect()]
    except Exception as e:
        # If namespace doesn't exist, that's fine - nothing to drop
        if "NoSuchNamespaceException" in str(type(e)) or "Namespace does not exist" in str(e):
            logger.info(f"Namespace {full_namespace} does not exist yet - nothing to drop")
            return True
        else:
            # Re-raise if it's a different error
            raise

    if not tables:
        logger.info(f"No tables found in {full_namespace} - nothing to drop")
        return True

    # Show warning
    logger.warning("="*80)
    logger.warning("FRESH START MODE - ALL DATA WILL BE DELETED")
    logger.warning("="*80)
    logger.warning(f"Namespace: {full_namespace}")
    logger.warning(f"Tables to drop: {len(tables)}")
    logger.warning("")
    logger.warning("Tables:")
    for table in sorted(tables):
        logger.warning(f"  - {table}")
    logger.warning("")
    logger.warning("This action CANNOT be undone!")
    logger.warning("="*80)

    # Confirmation prompt
    if not auto_confirm:
        import sys
        if sys.stdin.isatty():
            response = input("\nType 'YES' to confirm deletion: ")
            if response != 'YES':
                logger.info("Fresh start cancelled by user")
                return False
        else:
            logger.error("Cannot prompt for confirmation in non-interactive mode. Use --yes flag.")
            return False
    else:
        logger.warning("Auto-confirm enabled - proceeding with deletion")

    # Drop all tables
    logger.info(f"Dropping {len(tables)} tables from {full_namespace}")
    dropped_count = 0
    failed_tables = []

    for table in tables:
        try:
            full_table_name = f"{full_namespace}.{table}"
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info(f"Dropped: {table}")
            dropped_count += 1
        except Exception as e:
            logger.error(f"Failed to drop {table}: {e}")
            failed_tables.append(table)

    logger.info(f"Successfully dropped {dropped_count}/{len(tables)} tables")

    if failed_tables:
        logger.warning(f"Failed to drop {len(failed_tables)} tables: {', '.join(failed_tables)}")

    return True


def clear_checkpoint_directory(namespace: str):
    """
    Clear checkpoint directory for the namespace
    Note: Only works for local file:// paths, not cloud storage (s3://, dbfs://)
    For cloud storage, manually delete checkpoints via cloud console or dbfs commands
    """
    # Use CHECKPOINT_PATH env var (supports s3://, dbfs://, file://)
    checkpoint_base = os.environ.get('CHECKPOINT_PATH', str(PROJECT_ROOT / 'checkpoints'))
    checkpoint_dir_str = f"{checkpoint_base}/{namespace}"

    # Only attempt deletion for local file paths
    if checkpoint_base.startswith('file://') or not ('://' in checkpoint_base):
        # Convert to Path for local filesystem operations
        if checkpoint_base.startswith('file://'):
            checkpoint_base = checkpoint_base[7:]  # Remove file:// prefix

        checkpoint_dir = Path(checkpoint_base) / namespace

        if checkpoint_dir.exists():
            try:
                shutil.rmtree(checkpoint_dir)
                logger.info(f"Cleared checkpoint directory: {checkpoint_dir}")
                return True
            except Exception as e:
                logger.warning(f"Failed to clear checkpoint directory: {e}")
                return False
        else:
            logger.info(f"No checkpoint directory to clear: {checkpoint_dir}")
            return True
    else:
        logger.warning(f"Cannot auto-clear cloud checkpoint location: {checkpoint_dir_str}")
        logger.warning(f"Please manually delete checkpoints using cloud tools (aws s3 rm, dbfs rm, etc.)")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Unified direct bulk load from Oracle/Postgres to Iceberg'
    )
    parser.add_argument(
        '--source',
        required=True,
        help='Source name from sources.yaml (e.g., dev_source, prod_aurora_db1)'
    )
    parser.add_argument(
        '--parallel-tables',
        type=int,
        help='Number of partitions per table for parallel extraction (overrides config)'
    )
    parser.add_argument(
        '--parallel-workers',
        type=int,
        help='Number of tables to process simultaneously (overrides config)'
    )
    parser.add_argument(
        '--table-filter',
        help='SQL LIKE pattern to filter tables (e.g., "S_%%")'
    )
    parser.add_argument(
        '--no-resume',
        dest='resume',
        action='store_false',
        default=True,
        help='Do not resume from checkpoint - process all tables'
    )
    parser.add_argument(
        '--fresh-start',
        action='store_true',
        default=False,
        help='DROP ALL TABLES in namespace and start fresh (DESTRUCTIVE - requires confirmation)'
    )
    parser.add_argument(
        '--yes',
        action='store_true',
        default=False,
        help='Auto-confirm destructive operations (for automation/scripts)'
    )

    args = parser.parse_args()

    try:
        # Fresh start mode - drop all tables first
        if args.fresh_start:
            logger.warning("Fresh start mode requested - will drop all tables")

            # Load config to get namespace
            source_config = get_source_config(args.source)
            namespace = source_config['iceberg_namespace']
            db_type = source_config['database_type']

            # Get catalog name from environment
            catalog = os.environ.get('CATALOG_NAME')
            if not catalog:
                raise ValueError("CATALOG_NAME environment variable must be set")

            # Create minimal Spark session for dropping tables
            spark = SparkSessionFactory.create(f"FreshStart-{args.source}", db_type)

            # Drop all tables
            if not drop_namespace_tables(spark, namespace, catalog, args.yes):
                logger.error("Fresh start cancelled - exiting")
                sys.exit(0)

            # Clear checkpoint directory
            clear_checkpoint_directory(namespace)

            logger.info("Fresh start complete - proceeding with bulk load")
            logger.info("="*80)
            spark.stop()

        # Run normal bulk load
        direct_bulk_load(
            source_name=args.source,
            parallel_tables=args.parallel_tables,
            parallel_workers=args.parallel_workers,
            table_filter=args.table_filter,
            resume=args.resume
        )
    except Exception as e:
        logger.error(f"Direct load failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
