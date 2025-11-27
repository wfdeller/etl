"""
Iceberg utilities for ETL pipeline
Provides centralized Iceberg table operations
"""

import os
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


class IcebergTableManager:
    """Manager for Iceberg table operations"""

    def __init__(self, spark: SparkSession, catalog_name: Optional[str] = None):
        """
        Initialize IcebergTableManager

        Args:
            spark: SparkSession instance
            catalog_name: Iceberg catalog name (defaults to CATALOG_NAME env var)

        Raises:
            ValueError: If catalog_name is not provided and CATALOG_NAME env var is not set
        """
        self.spark = spark

        if catalog_name is None:
            catalog_name = os.environ.get('CATALOG_NAME')
            if not catalog_name:
                raise ValueError("CATALOG_NAME environment variable must be set")

        self.catalog_name = catalog_name

    def get_full_table_name(self, namespace: str, table_name: str) -> str:
        """
        Build fully qualified Iceberg table name

        Args:
            namespace: Iceberg namespace (e.g., 'bronze.dev_source')
            table_name: Table name

        Returns:
            Fully qualified table name (e.g., 'local.bronze.dev_source.customers')
        """
        return f"{self.catalog_name}.{namespace}.{table_name}"

    def table_exists(self, namespace: str, table_name: str) -> bool:
        """
        Check if Iceberg table exists

        Args:
            namespace: Iceberg namespace
            table_name: Table name

        Returns:
            True if table exists, False otherwise
        """
        full_table = self.get_full_table_name(namespace, table_name)

        try:
            self.spark.table(full_table)
            return True
        except Exception:
            return False

    def create_table(self, df: DataFrame, namespace: str, table_name: str) -> bool:
        """
        Create a new Iceberg table from DataFrame

        Args:
            df: DataFrame with data and schema
            namespace: Iceberg namespace
            table_name: Table name

        Returns:
            True if table was created successfully, False otherwise
        """
        full_table = self.get_full_table_name(namespace, table_name)

        try:
            logger.info(f"Creating table {full_table}")
            df.writeTo(full_table).using("iceberg").create()
            logger.info(f"Created table {full_table}")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {full_table}: {e}")
            return False

    def write_table(self, df: DataFrame, namespace: str, table_name: str,
                    merge_schema: bool = True) -> bool:
        """
        Write DataFrame to Iceberg table (create or append)

        Args:
            df: DataFrame to write
            namespace: Iceberg namespace
            table_name: Table name
            merge_schema: Whether to merge schema on append (default: True)

        Returns:
            True if write was successful, False otherwise
        """
        full_table = self.get_full_table_name(namespace, table_name)

        try:
            if self.table_exists(namespace, table_name):
                logger.info(f"Table {full_table} exists, appending...")
                if merge_schema:
                    df.writeTo(full_table).option("mergeSchema", "true").append()
                else:
                    df.writeTo(full_table).append()
            else:
                logger.info(f"Creating new table {full_table}")
                df.writeTo(full_table).using("iceberg").create()

            logger.info(f"Written to {full_table}")
            return True

        except Exception as e:
            logger.error(f"Error writing to {full_table}: {e}")
            return False

    def ensure_table_exists(self, namespace: str, table_name: str,
                           sample_data: Optional[dict] = None) -> bool:
        """
        Ensure Iceberg table exists, create from sample data if not

        Args:
            namespace: Iceberg namespace
            table_name: Table name
            sample_data: Sample record dict to infer schema (required if table doesn't exist)

        Returns:
            True if table exists or was created, False otherwise
        """
        full_table = self.get_full_table_name(namespace, table_name)

        # Check if table already exists
        if self.table_exists(namespace, table_name):
            return True

        # Table doesn't exist - create it
        if not sample_data:
            logger.error(f"Cannot create table {full_table} - no sample data provided")
            return False

        logger.info(f"Creating new table {full_table} from CDC event")

        try:
            # Create DataFrame from sample data to infer schema
            df = self.spark.createDataFrame([sample_data])
            df.writeTo(full_table).using("iceberg").create()

            logger.info(f"Created new table {full_table} with {len(sample_data)} columns")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {full_table}: {e}")
            return False

    def drop_table(self, namespace: str, table_name: str) -> bool:
        """
        Drop an Iceberg table

        Args:
            namespace: Iceberg namespace
            table_name: Table name

        Returns:
            True if table was dropped, False otherwise
        """
        full_table = self.get_full_table_name(namespace, table_name)

        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {full_table}")
            logger.info(f"Dropped table {full_table}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop table {full_table}: {e}")
            return False

    def list_tables(self, namespace: str, exclude_system: bool = True) -> list:
        """
        List all tables in a namespace

        Args:
            namespace: Iceberg namespace
            exclude_system: Exclude system tables starting with '_' (default: True)

        Returns:
            List of table names
        """
        full_namespace = f"{self.catalog_name}.{namespace}"

        try:
            tables_df = self.spark.sql(f"SHOW TABLES IN {full_namespace}")
            tables = [(row.namespace, row.tableName) for row in tables_df.collect()]

            if exclude_system:
                tables = [(ns, tbl) for ns, tbl in tables if not tbl.startswith('_')]

            return [tbl for ns, tbl in tables]
        except Exception as e:
            logger.error(f"Failed to list tables in {full_namespace}: {e}")
            return []

    def get_table_count(self, namespace: str, table_name: str) -> Optional[int]:
        """
        Get row count for a table

        Args:
            namespace: Iceberg namespace
            table_name: Table name

        Returns:
            Row count or None if table doesn't exist or error occurs
        """
        full_table = self.get_full_table_name(namespace, table_name)

        try:
            return self.spark.table(full_table).count()
        except Exception as e:
            logger.error(f"Failed to count rows in {full_table}: {e}")
            return None
