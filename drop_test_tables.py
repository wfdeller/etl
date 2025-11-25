#!/usr/bin/env python3
"""Drop specific tables for testing convert_to_clob functionality"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Set up paths
PROJECT_ROOT = Path(__file__).parent.absolute()
sys.path.insert(0, str(PROJECT_ROOT / 'lib'))

# Tables to drop for testing
TEST_TABLES = [
    'EIM_ACT_DTL',
    'EIM_ACCNT_DTL',
    'EIM_BASELN_DTL'
]

# Create Spark session
spark = SparkSession.builder \
    .appName("Drop Test Tables") \
    .config("spark.jars", str(PROJECT_ROOT / 'jars' / 'iceberg-spark-runtime.jar')) \
    .config("spark.driver.extraClassPath", str(PROJECT_ROOT / 'jars' / 'iceberg-spark-runtime.jar')) \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "localhost") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", f"file://{PROJECT_ROOT}/warehouse") \
    .getOrCreate()

print("\n" + "="*100)
print("DROPPING TEST TABLES FOR CONVERT_TO_CLOB TEST")
print("="*100)

for table_name in TEST_TABLES:
    full_table = f"local.bronze.siebel.{table_name}"
    try:
        # Check if table exists
        spark.sql(f"DESCRIBE TABLE {full_table}")

        # Drop the table
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        print(f"✓ Dropped: {table_name}")

        # Also delete from _cdc_status
        spark.sql(f"""
            DELETE FROM local.bronze.siebel._cdc_status
            WHERE table_name = '{table_name}'
        """)
        print(f"  ✓ Removed from _cdc_status")

    except Exception as e:
        if "Table not found" in str(e) or "cannot be found" in str(e):
            print(f"⊘ Table does not exist: {table_name}")
        else:
            print(f"✗ Error dropping {table_name}: {e}")

print("\n" + "="*100)
print("VERIFICATION - Remaining test table status entries:")
print("="*100)

remaining = spark.sql(f"""
    SELECT table_name, load_status, record_count
    FROM local.bronze.siebel._cdc_status
    WHERE table_name IN ('EIM_ACT_DTL', 'EIM_ACCNT_DTL', 'EIM_BASELN_DTL')
""")
remaining.show(truncate=False)

spark.stop()
