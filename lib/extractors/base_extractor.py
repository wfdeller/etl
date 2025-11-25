"""
Base table extractor class
"""

import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BaseTableExtractor(ABC):
    """Base class for database-specific table extractors"""
    
    def __init__(self, spark, jdbc_config, db_type, config):
        self.spark = spark
        self.jdbc_config = jdbc_config
        self.db_type = db_type
        self.config = config
        self.schema = jdbc_config['schema']
        self.large_threshold = config.get('large_table_threshold', 100000)
        self.batch_size = config.get('batch_size', 50000)
    
    @abstractmethod
    def get_table_list(self, table_filter=None):
        """Get list of tables to process"""
        pass
    
    @abstractmethod
    def extract_table(self, table_name, scn_lsn, num_partitions=8):
        """
        Extract a single table
        
        Returns:
            tuple: (DataFrame, record_count, metadata_dict)
                   or (None, 0, metadata_dict) if table should be skipped
        """
        pass
    
    @abstractmethod
    def get_row_count(self, table_name, **kwargs):
        """Get row count for table"""
        pass
    
    def _read_jdbc(self, query, num_partitions=None):
        """Read from JDBC with common options"""

        # Get driver class based on database type
        if self.db_type == 'oracle':
            driver_class = "oracle.jdbc.OracleDriver"
        elif self.db_type == 'postgres':
            driver_class = "org.postgresql.Driver"
        else:
            driver_class = None

        reader = self.spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_config['url']) \
            .option("user", self.jdbc_config['user']) \
            .option("password", self.jdbc_config['password']) \
            .option("fetchsize", str(self.batch_size))

        if driver_class:
            reader = reader.option("driver", driver_class)

        if num_partitions:
            reader = reader.option("numPartitions", str(num_partitions))

        # Handle both dbtable and query
        if query.strip().startswith('(') and query.strip().endswith(')'):
            reader = reader.option("dbtable", query)
        else:
            reader = reader.option("query", query)

        return reader.load()
    
    def _persist_if_large(self, df, record_count, metadata):
        """Persist large tables to disk to avoid OOM"""
        
        if record_count > self.large_threshold:
            from pyspark import StorageLevel
            df = df.persist(StorageLevel.DISK_ONLY)
            metadata['persisted'] = True
            logger.info(f"  Persisted large table to disk")
        
        return df
