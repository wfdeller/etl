"""
Postgres-specific table extractor
"""

import logging
from .base_extractor import BaseTableExtractor
from schema_fixers import PostgresSchemFixer

logger = logging.getLogger(__name__)


class PostgresTableExtractor(BaseTableExtractor):
    """Postgres-specific table extraction with LSN support"""
    
    def __init__(self, spark, jdbc_config, config):
        super().__init__(spark, jdbc_config, 'postgres', config)
    
    def get_table_list(self, table_filter=None):
        """Get list of Postgres tables"""

        # Sanitize table_filter to prevent SQL injection (allow alphanumeric, underscore, percent)
        if table_filter:
            import re
            if not re.match(r'^[A-Za-z0-9_%]+$', table_filter):
                raise ValueError(f"Invalid table_filter pattern: {table_filter}. Only alphanumeric, underscore, and % allowed.")

        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{self.schema.lower()}'
            AND table_type = 'BASE TABLE'
        """

        if table_filter:
            query += f" AND table_name LIKE '{table_filter}'"

        query += " ORDER BY table_name"

        df = self._read_jdbc(query)
        tables = [row[0] for row in df.collect()]

        logger.info(f"Found {len(tables)} tables to process")
        return tables
    
    def build_query(self, table_name):
        """Build Postgres query (no flashback needed - we use Debezium snapshot mode)"""
        return f"SELECT * FROM {self.schema}.{table_name}"
    
    def get_row_count(self, table_name):
        """Get row count for table"""

        query = f"SELECT COUNT(*) as cnt FROM {self.schema}.{table_name}"

        try:
            df = self._read_jdbc(f"({query})")
            return int(df.first()[0])
        except Exception as e:
            logger.warning(f"Could not count {table_name}: {e}")
            return None

    def get_primary_key_columns(self, table_name):
        """
        Get primary key columns for a Postgres table

        Queries information_schema to discover PK columns in order

        Returns:
            list: Primary key column names in order, or None if no PK found
        """

        query = f"""
            (SELECT kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
               AND tc.table_schema = kcu.table_schema
             WHERE tc.table_schema = '{self.schema.lower()}'
               AND tc.table_name = '{table_name.lower()}'
               AND tc.constraint_type = 'PRIMARY KEY'
             ORDER BY kcu.ordinal_position)
        """

        try:
            df = self._read_jdbc(query)
            pk_cols = [row[0] for row in df.collect()]

            if pk_cols:
                logger.info(f"{table_name}: found primary key columns: {', '.join(pk_cols)}")
                return pk_cols
            else:
                logger.warning(f"{table_name}: no primary key found")
                return None

        except Exception as e:
            logger.error(f"{table_name}: could not determine primary key: {e}")
            return None

    def extract_table(self, table_name, scn_lsn, num_partitions=8):
        """
        Extract Postgres table
        
        Returns:
            tuple: (DataFrame, record_count, metadata)
        """
        
        logger.info(f"{table_name}: starting extracting of {table_name}...")

        metadata = {
            'empty': False,
            'persisted': False,
            'primary_keys': []
        }

        # Discover primary keys
        pk_cols = self.get_primary_key_columns(table_name)
        if pk_cols:
            metadata['primary_keys'] = pk_cols

        # Build query
        base_query = self.build_query(table_name)

        # Check if empty
        record_count = self.get_row_count(table_name)

        if record_count == 0:
            logger.info(f"{table_name}: table is empty")
            metadata['empty'] = True
            
            # Get schema only
            schema_query = f"({base_query} LIMIT 0)"
            df = self._read_jdbc(schema_query)

            # Fix Postgres-specific schema issues (if any)
            df = PostgresSchemFixer.fix_schema(df)
            
            return df, 0, metadata

        logger.info(f"{table_name}: Table has {record_count:,} records")
        
        # Read data
        try:
            df = self._read_jdbc(f"({base_query})", num_partitions=num_partitions)

            # Fix Postgres-specific schema issues (if any)
            df = PostgresSchemFixer.fix_schema(df)
            
            # Persist large tables
            df = self._persist_if_large(df, record_count, metadata)

            logger.info(f"{table_name}: extracted {record_count:,} records from {table_name}")

            return df, record_count, metadata

        except Exception as e:
            logger.warning(f"{table_name}: initial read failed, retrying without partitioning: {e}")

            df = self._read_jdbc(f"({base_query})", num_partitions=None)

            df = PostgresSchemFixer.fix_schema(df)
            
            if record_count is None:
                record_count = df.count()
            
            df = self._persist_if_large(df, record_count, metadata)

            logger.info(f"{table_name}: extracted {record_count:,} records from {table_name}")

            return df, record_count, metadata
