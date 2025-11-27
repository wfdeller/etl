"""
Oracle-specific table extractor
"""

import logging
from .base_extractor import BaseTableExtractor
from schema_fixers import OracleSchemFixer

logger = logging.getLogger(__name__)


class OracleTableExtractor(BaseTableExtractor):
    """Oracle-specific table extraction with LONG column handling and SCN support"""
    
    def __init__(self, spark, jdbc_config, config):
        super().__init__(spark, jdbc_config, 'oracle', config)
        self.handle_long = config.get('handle_long_columns', 'exclude')
    
    def get_table_list(self, table_filter=None):
        """Get list of Oracle tables"""

        # Sanitize table_filter to prevent SQL injection (allow alphanumeric, underscore, percent)
        if table_filter:
            import re
            if not re.match(r'^[A-Za-z0-9_%]+$', table_filter):
                raise ValueError(f"Invalid table_filter pattern: {table_filter}. Only alphanumeric, underscore, and % allowed.")

        query = f"""
            SELECT table_name
            FROM all_tables
            WHERE owner = '{self.schema.upper()}'
            AND table_name NOT LIKE 'BIN$%%'
        """

        if table_filter:
            query += f" AND table_name LIKE '{table_filter}'"

        query += " ORDER BY table_name"

        df = self._read_jdbc(query)
        tables = [row[0] for row in df.collect()]

        logger.info(f"Found {len(tables)} tables to process")
        return tables
    
    def has_long_columns(self, table_name):
        """Check if table has LONG columns"""
        
        query = f"""
            (SELECT column_name 
             FROM all_tab_columns 
             WHERE owner = '{self.schema.upper()}' 
             AND table_name = '{table_name}' 
             AND data_type = 'LONG'
             ORDER BY column_id)
        """
        
        try:
            df = self._read_jdbc(query)
            long_columns = [row[0] for row in df.collect()]
            return long_columns
        except Exception as e:
            logger.warning(f"{table_name}: could not check for LONG columns in {table_name}: {e}")
            return []
    
    def get_non_long_columns(self, table_name):
        """Get list of non-LONG columns"""

        query = f"""
            (SELECT column_name
             FROM all_tab_columns
             WHERE owner = '{self.schema.upper()}'
             AND table_name = '{table_name}'
             AND data_type != 'LONG'
             ORDER BY column_id)
        """

        df = self._read_jdbc(query)
        return [row[0] for row in df.collect()]

    def get_primary_key_columns(self, table_name):
        """Get primary key columns for a table"""

        query = f"""
            (SELECT acc.column_name
             FROM all_constraints ac
             JOIN all_cons_columns acc
               ON ac.constraint_name = acc.constraint_name
               AND ac.owner = acc.owner
             WHERE ac.owner = '{self.schema.upper()}'
             AND ac.table_name = '{table_name}'
             AND ac.constraint_type = 'P'
             ORDER BY acc.position)
        """

        try:
            df = self._read_jdbc(query)
            pk_cols = [row[0] for row in df.collect()]
            if pk_cols:
                logger.info(f"{table_name}: found primary key columns: {', '.join(pk_cols)}")
                return pk_cols
            else:
                # Fallback to ROW_ID if no explicit primary key
                logger.info(f"{table_name}: no primary key found, will use ROW_ID")
                return ['ROW_ID']
        except Exception as e:
            logger.warning(f"{table_name}: could not determine primary key: {e}, using ROW_ID")
            return ['ROW_ID']

    def build_query(self, table_name, scn=None, exclude_long_columns=None):
        """Build optimized Oracle query with SCN flashback"""
        
        if exclude_long_columns:
            columns = self.get_non_long_columns(table_name)
            column_list = ", ".join(columns)
            base = f"SELECT {column_list} FROM {self.schema}.{table_name}"
        else:
            base = f"SELECT * FROM {self.schema}.{table_name}"
        
        if scn:
            base += f" AS OF SCN {scn}"
        
        return base
    
    def get_row_count(self, table_name, scn=None):
        """Get row count for table at specific SCN"""
        
        query = f"SELECT COUNT(*) as cnt FROM {self.schema}.{table_name}"
        if scn:
            query += f" AS OF SCN {scn}"
        
        try:
            df = self._read_jdbc(f"({query})")
            return int(df.first()[0])
        except Exception as e:
            logger.warning(f"{table_name}: could not count {table_name}: {e}")
            return None
    
    def extract_table(self, table_name, scn_lsn, num_partitions=8):
        """
        Extract Oracle table with LONG column handling and SCN flashback
        
        Returns:
            tuple: (DataFrame, record_count, metadata)
        """
        
        logger.info(f"{table_name}: starting extracting of {table_name}...")
        
        metadata = {
            'long_columns': [],
            'excluded_columns': [],
            'empty': False,
            'persisted': False,
            'primary_keys': []
        }

        scn = scn_lsn.get('oracle_scn')

        # Discover primary keys
        pk_cols = self.get_primary_key_columns(table_name)
        if pk_cols:
            metadata['primary_keys'] = pk_cols

        # Check for LONG columns
        long_columns = self.has_long_columns(table_name)

        if long_columns:
            metadata['long_columns'] = long_columns

            if self.handle_long == 'error':
                raise ValueError(f"Table {table_name} has LONG columns: {long_columns}")
            elif self.handle_long == 'skip_table':
                logger.warning(f"{table_name}: SKIPPING table {table_name} (has LONG columns: {', '.join(long_columns)})")
                return None, 0, metadata
            else:  # exclude (default)
                logger.warning(f"{table_name}: EXCLUDING LONG columns: {', '.join(long_columns)}")
                metadata['excluded_columns'] = long_columns

        # Build query
        base_query = self.build_query(table_name, scn, exclude_long_columns=long_columns)
        
        # Check if empty
        record_count = self.get_row_count(table_name, scn)
        
        if record_count == 0:
            logger.info(f"{table_name}: table is empty")
            metadata['empty'] = True
            
            # Get schema only
            schema_query = f"({base_query} WHERE 1=0)"
            df = self._read_jdbc(schema_query)

            # Fix Oracle-specific schema issues
            df = OracleSchemFixer.fix_schema(df)
            
            return df, 0, metadata
        
        logger.info(f"{table_name}: Table has {record_count:,} records")
        
        # Read data
        try:
            # Standard single-pass read
            # Use partitioning unless LONG columns present (which would be excluded)
            use_partitions = num_partitions if not long_columns else None

            if long_columns:
                logger.info(f"{table_name}: reading without partitioning (LONG columns excluded)")

            df = self._read_jdbc(f"({base_query})", num_partitions=use_partitions)

            # Fix Oracle-specific schema issues
            df = OracleSchemFixer.fix_schema(df)

            # Persist large tables
            df = self._persist_if_large(df, record_count, metadata)

            logger.info(f"{table_name}: extracted {record_count:,} records from {table_name}")

            return df, record_count, metadata
            
        except Exception as e:
            logger.warning(f"{table_name}: initial read failed, retrying without partitioning: {e}")

            df = self._read_jdbc(f"({base_query})", num_partitions=None)

            df = OracleSchemFixer.fix_schema(df)
            
            if record_count is None:
                record_count = df.count()
            
            df = self._persist_if_large(df, record_count, metadata)
            
            logger.info(f"{table_name}: loaded {record_count:,} records from {table_name}")
            
            return df, record_count, metadata
