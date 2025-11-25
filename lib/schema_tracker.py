"""
Schema change tracking for CDC pipelines
Tracks schema changes in Iceberg table for transformation developer awareness
"""

import logging
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class SchemaTracker:
    """
    Tracks schema changes in Iceberg audit table

    Detects:
    - Baseline schema establishment (initial load)
    - Column additions
    - Column removals
    - Column type changes

    Table schema:
    - change_id: string (timestamp-based unique ID)
    - table_name: string
    - change_type: string (baseline, column_added, column_removed, column_modified)
    - column_name: string (null for baseline)
    - old_data_type: string (null for baseline and additions)
    - new_data_type: string (null for removals)
    - detected_timestamp: timestamp
    - source_db: string
    - change_details: string (JSON with additional metadata)
    """

    def __init__(self, spark: SparkSession, namespace: str, catalog: str = 'local'):
        """
        Initialize schema tracker

        Args:
            spark: SparkSession
            namespace: Iceberg namespace (e.g., 'bronze.siebel')
            catalog: Iceberg catalog name (default: 'local')
        """
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.changes_table = f"{catalog}.{namespace}._schema_changes"

        # Create schema changes table if it doesn't exist
        self._create_changes_table_if_not_exists()

    @staticmethod
    def _get_changes_schema() -> StructType:
        """
        Get the schema changes table schema

        Returns:
            StructType: Schema changes table schema
        """
        return StructType([
            StructField('change_id', StringType(), False),
            StructField('table_name', StringType(), False),
            StructField('change_type', StringType(), False),
            StructField('column_name', StringType(), True),
            StructField('old_data_type', StringType(), True),
            StructField('new_data_type', StringType(), True),
            StructField('detected_timestamp', TimestampType(), False),
            StructField('source_db', StringType(), True),
            StructField('change_details', StringType(), True)
        ])

    def _create_changes_table_if_not_exists(self):
        """Create the schema changes tracking table if it doesn't exist"""
        try:
            # Try to read the table to see if it exists
            self.spark.table(self.changes_table)
            logger.info(f"Schema changes table {self.changes_table} already exists")
        except AnalysisException:
            # Table doesn't exist, create it
            logger.info(f"Creating schema changes table {self.changes_table}")

            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], self._get_changes_schema())

            # Create Iceberg table
            empty_df.writeTo(self.changes_table).using("iceberg").create()

            logger.info(f"Created schema changes table {self.changes_table}")

    def record_baseline_schema(self, table_name: str, schema: StructType, source_db: str = None):
        """
        Record baseline schema for a table (called during initial load)

        Args:
            table_name: Table name
            schema: PySpark StructType schema
            source_db: Source database identifier
        """
        timestamp = datetime.now()
        change_id = f"{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}_baseline"

        # Serialize schema to JSON
        schema_json = json.dumps({
            'fields': [
                {
                    'name': field.name,
                    'type': str(field.dataType),
                    'nullable': field.nullable
                }
                for field in schema.fields
            ]
        })

        change_data = [{
            'change_id': change_id,
            'table_name': table_name,
            'change_type': 'baseline',
            'column_name': None,
            'old_data_type': None,
            'new_data_type': None,
            'detected_timestamp': timestamp,
            'source_db': source_db,
            'change_details': schema_json
        }]

        df = self.spark.createDataFrame(change_data, schema=self._get_changes_schema())
        df.writeTo(self.changes_table).using("iceberg").append()

        logger.info(f"Recorded baseline schema for {table_name} ({len(schema.fields)} columns)")

    def get_latest_schema(self, table_name: str) -> dict:
        """
        Get the latest known schema for a table

        Args:
            table_name: Table name

        Returns:
            dict: Schema information with fields:
                - exists: bool (True if baseline schema found)
                - schema: dict (field name -> data type mapping)
                - timestamp: datetime (when schema was recorded)
        """
        try:
            from pyspark.sql.functions import col

            # Get most recent baseline or schema-affecting change
            latest = self.spark.table(self.changes_table) \
                .filter(col("table_name") == table_name) \
                .orderBy(col("detected_timestamp").desc()) \
                .first()

            if not latest:
                return {'exists': False, 'schema': {}, 'timestamp': None}

            # If latest record is baseline, parse the full schema
            if latest['change_type'] == 'baseline':
                schema_dict = json.loads(latest['change_details'])
                schema_map = {
                    field['name']: field['type']
                    for field in schema_dict['fields']
                }
                return {
                    'exists': True,
                    'schema': schema_map,
                    'timestamp': latest['detected_timestamp']
                }
            else:
                # Reconstruct schema from all changes (more complex, simplified for now)
                # For MVP, we'll require baseline to exist
                logger.warning(f"No baseline schema found for {table_name}, cannot determine latest schema")
                return {'exists': False, 'schema': {}, 'timestamp': None}

        except Exception as e:
            logger.error(f"Error retrieving latest schema for {table_name}: {e}")
            return {'exists': False, 'schema': {}, 'timestamp': None}

    def detect_and_record_changes(self, table_name: str, current_schema: StructType, source_db: str = None):
        """
        Detect and record schema changes by comparing current schema with latest known schema

        Args:
            table_name: Table name
            current_schema: Current PySpark StructType schema
            source_db: Source database identifier

        Returns:
            list: List of detected changes (empty if no changes)
        """
        # Get latest known schema
        latest = self.get_latest_schema(table_name)

        if not latest['exists']:
            # No baseline exists - record this as baseline
            self.record_baseline_schema(table_name, current_schema, source_db)
            return []

        # Compare schemas
        known_schema = latest['schema']
        current_schema_map = {field.name: str(field.dataType) for field in current_schema.fields}

        changes = []
        timestamp = datetime.now()

        # Detect column additions
        for col_name, col_type in current_schema_map.items():
            if col_name not in known_schema:
                changes.append({
                    'change_id': f"{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}_add_{col_name}",
                    'table_name': table_name,
                    'change_type': 'column_added',
                    'column_name': col_name,
                    'old_data_type': None,
                    'new_data_type': col_type,
                    'detected_timestamp': timestamp,
                    'source_db': source_db,
                    'change_details': json.dumps({'action': 'added', 'column': col_name, 'type': col_type})
                })

        # Detect column removals
        for col_name, col_type in known_schema.items():
            if col_name not in current_schema_map:
                changes.append({
                    'change_id': f"{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}_remove_{col_name}",
                    'table_name': table_name,
                    'change_type': 'column_removed',
                    'column_name': col_name,
                    'old_data_type': col_type,
                    'new_data_type': None,
                    'detected_timestamp': timestamp,
                    'source_db': source_db,
                    'change_details': json.dumps({'action': 'removed', 'column': col_name, 'type': col_type})
                })

        # Detect column type modifications
        for col_name in set(known_schema.keys()) & set(current_schema_map.keys()):
            old_type = known_schema[col_name]
            new_type = current_schema_map[col_name]
            if old_type != new_type:
                changes.append({
                    'change_id': f"{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}_modify_{col_name}",
                    'table_name': table_name,
                    'change_type': 'column_modified',
                    'column_name': col_name,
                    'old_data_type': old_type,
                    'new_data_type': new_type,
                    'detected_timestamp': timestamp,
                    'source_db': source_db,
                    'change_details': json.dumps({'action': 'modified', 'column': col_name, 'old_type': old_type, 'new_type': new_type})
                })

        # Record changes if any detected
        if changes:
            df = self.spark.createDataFrame(changes, schema=self._get_changes_schema())
            df.writeTo(self.changes_table).using("iceberg").append()

            logger.warning(f"Detected {len(changes)} schema change(s) for {table_name}:")
            for change in changes:
                logger.warning(f"  - {change['change_type']}: {change['column_name']} "
                             f"({change['old_data_type']} -> {change['new_data_type']})")

        return changes

    def get_changes_for_table(self, table_name: str):
        """
        Get all schema changes for a specific table

        Args:
            table_name: Table name

        Returns:
            DataFrame: Schema changes for the table
        """
        from pyspark.sql.functions import col
        return self.spark.table(self.changes_table) \
            .filter(col("table_name") == table_name) \
            .orderBy(col("detected_timestamp").desc())

    def get_all_changes(self, since_timestamp: datetime = None):
        """
        Get all schema changes, optionally filtered by timestamp

        Args:
            since_timestamp: Optional datetime to filter changes after

        Returns:
            DataFrame: Schema changes
        """
        df = self.spark.table(self.changes_table)

        if since_timestamp:
            from pyspark.sql.functions import col
            df = df.filter(col("detected_timestamp") >= since_timestamp)

        return df.orderBy("detected_timestamp", ascending=False)
