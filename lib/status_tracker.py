"""
Status tracking for CDC pipelines
Manages Iceberg table for tracking load progress and CDC position
"""

import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, ArrayType

logger = logging.getLogger(__name__)


class CDCStatusTracker:
    """
    Tracks CDC status in Iceberg table

    Table schema:
    - table_name: string (primary key)
    - load_status: string (in_progress, completed, failed)
    - initial_load_start: timestamp
    - initial_load_end: timestamp
    - record_count: int
    - source_db: string
    - primary_keys: array<string> (discovered primary key columns)
    - oracle_scn: int (for Oracle sources)
    - postgres_lsn: string (for Postgres sources)
    - last_processed_timestamp: timestamp
    - error_message: string
    """
    
    def __init__(self, spark: SparkSession, namespace: str, catalog: str = 'local'):
        """
        Initialize status tracker
        
        Args:
            spark: SparkSession
            namespace: Iceberg namespace (e.g., 'bronze.siebel')
            catalog: Iceberg catalog name (default: 'local')
        """
        self.spark = spark
        self.catalog = catalog
        self.namespace = namespace
        self.status_table = f"{catalog}.{namespace}._cdc_status"
        
        # Create status table if it doesn't exist
        self._create_status_table_if_not_exists()
    
    def _create_status_table_if_not_exists(self):
        """Create the status tracking table if it doesn't exist"""
        
        try:
            # Try to read the table to see if it exists
            self.spark.table(self.status_table)
            logger.info(f"Status table {self.status_table} already exists")
        except:
            # Table doesn't exist, create it
            logger.info(f"Creating status table {self.status_table}")
            
            schema = StructType([
                StructField('table_name', StringType(), False),
                StructField('load_status', StringType(), True),
                StructField('initial_load_start', TimestampType(), True),
                StructField('initial_load_end', TimestampType(), True),
                StructField('record_count', IntegerType(), True),
                StructField('source_db', StringType(), True),
                StructField('primary_keys', ArrayType(StringType()), True),
                StructField('oracle_scn', LongType(), True),  # Changed to LongType for large SCN values
                StructField('postgres_lsn', StringType(), True),
                StructField('last_processed_timestamp', TimestampType(), True),
                StructField('error_message', StringType(), True)
            ])
            
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Create Iceberg table
            empty_df.writeTo(self.status_table).using("iceberg").create()
            
            logger.info(f"Created status table {self.status_table}")
    
    def record_initial_load_start(self, table_name: str, source_db: str, primary_keys: list = None, oracle_scn: int = None, postgres_lsn: str = None):
        """Record the start of initial load for a table"""

        status_data = [{
            'table_name': table_name,
            'load_status': 'in_progress',
            'initial_load_start': datetime.now(),
            'initial_load_end': None,
            'record_count': None,
            'source_db': source_db,
            'primary_keys': primary_keys if primary_keys else [],
            'oracle_scn': oracle_scn,
            'postgres_lsn': postgres_lsn,
            'last_processed_timestamp': None,
            'error_message': None
        }]

        # Define explicit schema to avoid type inference issues
        schema = StructType([
            StructField('table_name', StringType(), False),
            StructField('load_status', StringType(), True),
            StructField('initial_load_start', TimestampType(), True),
            StructField('initial_load_end', TimestampType(), True),
            StructField('record_count', IntegerType(), True),
            StructField('source_db', StringType(), True),
            StructField('primary_keys', ArrayType(StringType()), True),
            StructField('oracle_scn', LongType(), True),  # Changed to LongType for large SCN values
            StructField('postgres_lsn', StringType(), True),
            StructField('last_processed_timestamp', TimestampType(), True),
            StructField('error_message', StringType(), True)
        ])

        df = self.spark.createDataFrame(status_data, schema=schema)

        # Use merge instead of append
        df.createOrReplaceTempView("status_updates")

        self.spark.sql(f"""
            MERGE INTO {self.status_table} target
            USING status_updates source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        logger.info(f"Recorded initial load start for {table_name} (SCN: {oracle_scn}, LSN: {postgres_lsn})")


    def record_initial_load_complete(self, table_name: str, record_count: int, primary_keys: list = None, oracle_scn: int = None, postgres_lsn: str = None):
        """Record completion of initial load for a table"""

        # First get existing record to preserve initial_load_start and primary_keys
        try:
            from pyspark.sql.functions import col
            existing = self.spark.table(self.status_table) \
                .filter(col("table_name") == table_name) \
                .orderBy(col("initial_load_start").desc()) \
                .limit(1) \
                .first()

            if existing:
                initial_start = existing['initial_load_start']
                source_db = existing['source_db']
                existing_pks = existing.get('primary_keys', [])
            else:
                initial_start = None
                source_db = None
                existing_pks = []
        except:
            initial_start = None
            source_db = None
            existing_pks = []

        # Use provided primary_keys if available, otherwise preserve existing
        final_pks = primary_keys if primary_keys is not None else existing_pks

        status_data = [{
            'table_name': table_name,
            'load_status': 'completed',
            'initial_load_start': initial_start,
            'initial_load_end': datetime.now(),
            'record_count': record_count,
            'source_db': source_db,
            'primary_keys': final_pks if final_pks else [],
            'oracle_scn': oracle_scn,
            'postgres_lsn': postgres_lsn,
            'last_processed_timestamp': None,
            'error_message': None
        }]

        # Define explicit schema to avoid type inference issues
        schema = StructType([
            StructField('table_name', StringType(), False),
            StructField('load_status', StringType(), True),
            StructField('initial_load_start', TimestampType(), True),
            StructField('initial_load_end', TimestampType(), True),
            StructField('record_count', IntegerType(), True),
            StructField('source_db', StringType(), True),
            StructField('primary_keys', ArrayType(StringType()), True),
            StructField('oracle_scn', LongType(), True),  # Changed to LongType for large SCN values
            StructField('postgres_lsn', StringType(), True),
            StructField('last_processed_timestamp', TimestampType(), True),
            StructField('error_message', StringType(), True)
        ])

        df = self.spark.createDataFrame(status_data, schema=schema)

        # Use merge instead of append
        df.createOrReplaceTempView("status_updates")

        self.spark.sql(f"""
            MERGE INTO {self.status_table} target
            USING status_updates source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        logger.info(f"Recorded initial load completion for {table_name}: {record_count} records")


    def record_load_failure(self, table_name: str, error_message: str):
        """Record failure of initial load for a table"""

        # Get existing record to preserve initial_load_start
        try:
            from pyspark.sql.functions import col
            existing = self.spark.table(self.status_table) \
                .filter(col("table_name") == table_name) \
                .orderBy(col("initial_load_start").desc()) \
                .limit(1) \
                .first()

            if existing:
                initial_start = existing['initial_load_start']
                source_db = existing['source_db']
            else:
                initial_start = None
                source_db = None
        except:
            initial_start = None
            source_db = None

        status_data = [{
            'table_name': table_name,
            'load_status': 'failed',
            'initial_load_start': initial_start,
            'initial_load_end': datetime.now(),
            'record_count': None,
            'source_db': source_db,
            'oracle_scn': None,
            'postgres_lsn': None,
            'last_processed_timestamp': None,
            'error_message': error_message
        }]

        # Define explicit schema to avoid type inference issues
        schema = StructType([
            StructField('table_name', StringType(), False),
            StructField('load_status', StringType(), True),
            StructField('initial_load_start', TimestampType(), True),
            StructField('initial_load_end', TimestampType(), True),
            StructField('record_count', IntegerType(), True),
            StructField('source_db', StringType(), True),
            StructField('oracle_scn', LongType(), True),  # Changed to LongType for large SCN values
            StructField('postgres_lsn', StringType(), True),
            StructField('last_processed_timestamp', TimestampType(), True),
            StructField('error_message', StringType(), True)
        ])

        df = self.spark.createDataFrame(status_data, schema=schema)

        # Use merge instead of append
        df.createOrReplaceTempView("status_updates")

        self.spark.sql(f"""
            MERGE INTO {self.status_table} target
            USING status_updates source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        logger.error(f"Recorded initial load failure for {table_name}: {error_message}")


    def update_cdc_position(self, table_name: str, oracle_scn: int = None, postgres_lsn: str = None, last_timestamp: datetime = None):
        """Update CDC position for a table"""

        status_data = [{
            'table_name': table_name,
            'load_status': None,
            'initial_load_start': None,
            'initial_load_end': None,
            'record_count': None,
            'source_db': None,
            'oracle_scn': oracle_scn,
            'postgres_lsn': postgres_lsn,
            'last_processed_timestamp': last_timestamp or datetime.now(),
            'error_message': None
        }]

        # Define explicit schema to avoid type inference issues
        schema = StructType([
            StructField('table_name', StringType(), False),
            StructField('load_status', StringType(), True),
            StructField('initial_load_start', TimestampType(), True),
            StructField('initial_load_end', TimestampType(), True),
            StructField('record_count', IntegerType(), True),
            StructField('source_db', StringType(), True),
            StructField('oracle_scn', LongType(), True),  # Changed to LongType for large SCN values
            StructField('postgres_lsn', StringType(), True),
            StructField('last_processed_timestamp', TimestampType(), True),
            StructField('error_message', StringType(), True)
        ])

        df = self.spark.createDataFrame(status_data, schema=schema)

        # Use merge to update only CDC position fields
        df.createOrReplaceTempView("status_updates")

        self.spark.sql(f"""
            MERGE INTO {self.status_table} target
            USING status_updates source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET
                oracle_scn = source.oracle_scn,
                postgres_lsn = source.postgres_lsn,
                last_processed_timestamp = source.last_processed_timestamp
            WHEN NOT MATCHED THEN INSERT *
        """)

        logger.debug(f"Updated CDC position for {table_name} (SCN: {oracle_scn}, LSN: {postgres_lsn})")
    
    def get_status(self, table_name: str = None):
        """
        Get status for table(s)
        
        Args:
            table_name: Optional table name filter
            
        Returns:
            DataFrame with status information
        """
        df = self.spark.table(self.status_table)
        
        if table_name:
            df = df.filter(df.table_name == table_name)
        
        return df
    
    def get_last_scn_lsn(self, table_name: str):
        """
        Get the last processed SCN/LSN for a table
        
        Returns:
            dict: {'oracle_scn': int, 'postgres_lsn': str}
        """
        df = self.get_status(table_name)
        
        if df.count() == 0:
            return {'oracle_scn': None, 'postgres_lsn': None}
        
        # Get most recent record
        latest = df.orderBy(df.last_processed_timestamp.desc()).first()
        
        return {
            'oracle_scn': latest['oracle_scn'],
            'postgres_lsn': latest['postgres_lsn']
        }
