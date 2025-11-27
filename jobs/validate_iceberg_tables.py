#!/usr/bin/env python3
"""
Validate Iceberg tables after bulk load
Lists all tables with row counts and identifies issues
"""

import sys
import os
import argparse
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession

# Auto-detect project root
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Add lib to path
sys.path.insert(0, str(PROJECT_ROOT / 'lib'))

from config_loader import get_source_config
from spark_utils import SparkSessionFactory


# Spark session creation moved to SparkSessionFactory.create()


def validate_tables(source_name: str, show_empty: bool = True, show_missing: bool = True):
    """
    Validate all Iceberg tables for a source
    
    Args:
        source_name: Source name from sources.yaml
        show_empty: Show tables with 0 rows
        show_missing: Check source database for missing tables
    """
    
    print("="*100)
    print(f"ICEBERG TABLE VALIDATION: {source_name}")
    print("="*100)
    print(f"Started at: {datetime.now()}")
    print()
    
    # Load config
    source_config = get_source_config(source_name)
    namespace = source_config['iceberg_namespace']
    db_type = source_config['database_type']
    
    # Create Spark session
    spark = SparkSessionFactory.create("Validate-Iceberg-Tables")
    
    # Get all tables in namespace
    print(f"Scanning namespace: {namespace}")
    print("-"*100)

    # Get catalog name
    catalog_name = os.environ.get('CATALOG_NAME')
    if not catalog_name:
        raise ValueError("CATALOG_NAME environment variable must be set")

    try:
        # List all tables in the namespace
        tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{namespace}")
        iceberg_tables = [(row.namespace, row.tableName) for row in tables_df.collect()]

        # Filter out status table
        iceberg_tables = [(ns, tbl) for ns, tbl in iceberg_tables if not tbl.startswith('_')]

        print(f"Found {len(iceberg_tables)} tables in Iceberg")
        print()

    except Exception as e:
        print(f"ERROR: Could not list tables in {namespace}: {e}")
        return

    # Count rows in each table
    print(f"{'Table Name':<50} {'Row Count':>15} {'Status':<20}")
    print("-"*100)

    total_rows = 0
    empty_tables = []
    error_tables = []
    non_empty_tables = []

    for ns, table_name in sorted(iceberg_tables, key=lambda x: x[1]):
        full_table = f"{catalog_name}.{ns}.{table_name}"
        
        try:
            count = spark.table(full_table).count()
            total_rows += count
            
            if count == 0:
                empty_tables.append(table_name)
                status = "EMPTY"
                if show_empty:
                    print(f"{table_name:<50} {count:>15,} {status:<20}")
            else:
                non_empty_tables.append((table_name, count))
                status = "OK"
                print(f"{table_name:<50} {count:>15,} {status:<20}")
                
        except Exception as e:
            error_tables.append((table_name, str(e)))
            print(f"{table_name:<50} {'ERROR':>15} {str(e)[:50]:<20}")
    
    # Summary
    print("-"*100)
    print(f"{'TOTAL':<50} {total_rows:>15,}")
    print("="*100)
    print()
    
    # Statistics
    print("SUMMARY:")
    print(f"  Total tables in Iceberg: {len(iceberg_tables)}")
    print(f"  Tables with data: {len(non_empty_tables)}")
    print(f"  Empty tables: {len(empty_tables)}")
    print(f"  Error tables: {len(error_tables)}")
    print(f"  Total rows: {total_rows:,}")
    print()
    
    # Top 10 largest tables
    if non_empty_tables:
        print("TOP 10 LARGEST TABLES:")
        print(f"{'Table Name':<50} {'Row Count':>15}")
        print("-"*70)
        for table, count in sorted(non_empty_tables, key=lambda x: x[1], reverse=True)[:10]:
            print(f"{table:<50} {count:>15,}")
        print()
    
    # Empty tables list
    if empty_tables and show_empty:
        print(f"EMPTY TABLES ({len(empty_tables)}):")
        for i, table in enumerate(empty_tables, 1):
            print(f"  {i}. {table}")
            if i >= 20:
                print(f"  ... and {len(empty_tables) - 20} more")
                break
        print()
    
    # Error tables
    if error_tables:
        print(f"ERROR TABLES ({len(error_tables)}):")
        for table, error in error_tables:
            print(f"  {table}: {error}")
        print()
    
    # Check for missing tables (if requested)
    if show_missing and db_type in ['oracle', 'postgres']:
        print("CHECKING FOR MISSING TABLES IN SOURCE DATABASE...")
        print("-"*100)
        
        try:
            # Set up JDBC
            db_config = source_config['database_connection']
            
            if db_type == 'oracle':
                jdbc_url = f"jdbc:oracle:thin:@{db_config['host']}:{db_config['port']}/{db_config['service_name']}"
                jdbc_jar = "/opt/spark/jars/ojdbc8.jar"
                schema_query = f"""
                    SELECT table_name
                    FROM all_tables 
                    WHERE owner = '{db_config['schema'].upper()}'
                    AND table_name NOT LIKE 'BIN$%%'
                    ORDER BY table_name
                """
            elif db_type == 'postgres':
                jdbc_url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
                jdbc_jar = "/opt/spark/jars/postgresql-42.6.0.jar"
                schema_query = f"""
                    SELECT table_name
                    FROM information_schema.tables 
                    WHERE table_schema = '{db_config['schema'].lower()}'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """
            
            # Get source tables
            source_tables_df = spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("query", schema_query) \
                .option("user", db_config['username']) \
                .option("password", db_config['password']) \
                .load()
            
            source_tables = {row[0] for row in source_tables_df.collect()}
            iceberg_table_names = {tbl for ns, tbl in iceberg_tables}
            
            missing_in_iceberg = source_tables - iceberg_table_names
            extra_in_iceberg = iceberg_table_names - source_tables
            
            print(f"Source database tables: {len(source_tables)}")
            print(f"Iceberg tables: {len(iceberg_table_names)}")
            print()
            
            if missing_in_iceberg:
                print(f"MISSING IN ICEBERG ({len(missing_in_iceberg)} tables):")
                for i, table in enumerate(sorted(missing_in_iceberg), 1):
                    print(f"  {i}. {table}")
                    if i >= 20:
                        print(f"  ... and {len(missing_in_iceberg) - 20} more")
                        break
                print()
            else:
                print("All source tables are present in Iceberg")
                print()
            
            if extra_in_iceberg:
                print(f"EXTRA IN ICEBERG (not in source) ({len(extra_in_iceberg)} tables):")
                for i, table in enumerate(sorted(extra_in_iceberg), 1):
                    print(f"  {i}. {table}")
                    if i >= 20:
                        print(f"  ... and {len(extra_in_iceberg) - 20} more")
                        break
                print()
            
        except Exception as e:
            print(f"Could not check source database: {e}")
            print()
    
    print("="*100)
    print(f"Completed at: {datetime.now()}")
    print("="*100)
    
    spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description='Validate Iceberg tables after bulk load'
    )
    parser.add_argument(
        '--source',
        required=True,
        help='Source name from sources.yaml (e.g., dev_source)'
    )
    parser.add_argument(
        '--hide-empty',
        action='store_true',
        help='Hide empty tables from output'
    )
    parser.add_argument(
        '--no-check-missing',
        action='store_true',
        help='Skip checking for missing tables in source database'
    )
    
    args = parser.parse_args()
    
    try:
        validate_tables(
            source_name=args.source,
            show_empty=not args.hide_empty,
            show_missing=not args.no_check_missing
        )
    except Exception as e:
        print(f"Validation failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
