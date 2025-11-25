#!/usr/bin/env python3
"""
Schema Changes Query Tool
Query and export schema change history from the _schema_changes audit table
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Auto-detect project root
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Add lib to path
sys.path.insert(0, str(PROJECT_ROOT / 'lib'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

from config_loader import get_source_config
from schema_tracker import SchemaTracker
from spark_utils import SparkSessionFactory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def query_schema_changes(source_name: str, table_name: str = None, change_type: str = None,
                         since_date: str = None, output_format: str = 'table', output_file: str = None):
    """
    Query schema changes with optional filters

    Args:
        source_name: Source name from sources.yaml
        table_name: Optional table name filter
        change_type: Optional change type filter (baseline, column_added, column_removed, column_modified)
        since_date: Optional date filter (format: YYYY-MM-DD)
        output_format: Output format ('table' or 'csv')
        output_file: Optional output file path (for CSV export)
    """

    logger.info("="*80)
    logger.info(f"Querying schema changes for source: {source_name}")
    logger.info("="*80)

    # Load configuration
    source_config = get_source_config(source_name)
    namespace = source_config['iceberg_namespace']

    logger.info(f"Target namespace: {namespace}")

    # Create Spark session
    spark = SparkSessionFactory.create(f"Schema-Changes-Query-{source_name}", db_type=None)

    try:
        # Initialize schema tracker
        schema_tracker = SchemaTracker(spark, namespace)

        # Get changes with optional filters
        if since_date:
            try:
                since_timestamp = datetime.strptime(since_date, '%Y-%m-%d')
                df = schema_tracker.get_all_changes(since_timestamp=since_timestamp)
            except ValueError:
                logger.error(f"Invalid date format: {since_date}. Use YYYY-MM-DD")
                sys.exit(1)
        else:
            df = schema_tracker.get_all_changes()

        # Apply table name filter
        if table_name:
            df = df.filter(col("table_name") == table_name)
            logger.info(f"Filter: table_name = {table_name}")

        # Apply change type filter
        if change_type:
            valid_types = ['baseline', 'column_added', 'column_removed', 'column_modified']
            if change_type not in valid_types:
                logger.error(f"Invalid change_type: {change_type}. Must be one of: {', '.join(valid_types)}")
                sys.exit(1)
            df = df.filter(col("change_type") == change_type)
            logger.info(f"Filter: change_type = {change_type}")

        # Apply date filter
        if since_date:
            logger.info(f"Filter: detected_timestamp >= {since_date}")

        # Check if results exist
        count = df.count()
        if count == 0:
            logger.info("No schema changes found matching the specified filters")
            return

        logger.info(f"Found {count} schema change(s)")
        logger.info("")

        # Output results
        if output_format == 'csv':
            if output_file:
                # Export to CSV file
                df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(output_file)
                logger.info(f"Schema changes exported to: {output_file}")
            else:
                # Print CSV to stdout
                df.show(count, truncate=False)
        else:
            # Print as table
            df.show(count, truncate=False)

    except Exception as e:
        logger.error(f"Error querying schema changes: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


def list_tables_with_changes(source_name: str):
    """
    List all tables that have schema changes recorded

    Args:
        source_name: Source name from sources.yaml
    """

    logger.info("="*80)
    logger.info(f"Listing tables with schema changes for source: {source_name}")
    logger.info("="*80)

    # Load configuration
    source_config = get_source_config(source_name)
    namespace = source_config['iceberg_namespace']

    # Create Spark session
    spark = SparkSessionFactory.create(f"Schema-Changes-List-{source_name}", db_type=None)

    try:
        # Initialize schema tracker
        schema_tracker = SchemaTracker(spark, namespace)

        # Get all changes and group by table
        df = schema_tracker.get_all_changes()

        # Group by table and count changes
        summary = df.groupBy("table_name").count().orderBy(desc("count"))

        count = summary.count()
        if count == 0:
            logger.info("No schema changes found")
            return

        logger.info(f"Found {count} table(s) with schema changes:")
        logger.info("")
        summary.show(count, truncate=False)

    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description='Query schema change history from Iceberg audit table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all tables with schema changes
  %(prog)s --source dev_siebel --list-tables

  # Query all schema changes for a specific table
  %(prog)s --source dev_siebel --table S_CONTACT

  # Query all schema changes since a specific date
  %(prog)s --source dev_siebel --since 2025-01-01

  # Query specific change type
  %(prog)s --source dev_siebel --table S_CONTACT --change-type column_added

  # Export to CSV
  %(prog)s --source dev_siebel --table S_CONTACT --output csv --file /tmp/changes.csv
        """
    )

    parser.add_argument(
        '--source',
        required=True,
        help='Source name from sources.yaml (e.g., dev_siebel)'
    )

    parser.add_argument(
        '--list-tables',
        action='store_true',
        help='List all tables with schema changes'
    )

    parser.add_argument(
        '--table',
        help='Filter by table name'
    )

    parser.add_argument(
        '--change-type',
        choices=['baseline', 'column_added', 'column_removed', 'column_modified'],
        help='Filter by change type'
    )

    parser.add_argument(
        '--since',
        help='Filter changes since date (format: YYYY-MM-DD)'
    )

    parser.add_argument(
        '--output',
        choices=['table', 'csv'],
        default='table',
        help='Output format (default: table)'
    )

    parser.add_argument(
        '--file',
        help='Output file path (for CSV export)'
    )

    args = parser.parse_args()

    try:
        if args.list_tables:
            list_tables_with_changes(args.source)
        else:
            query_schema_changes(
                args.source,
                table_name=args.table,
                change_type=args.change_type,
                since_date=args.since,
                output_format=args.output,
                output_file=args.file
            )
    except Exception as e:
        logger.error(f"Query failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
