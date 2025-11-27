#!/usr/bin/env python3
"""
Oracle to PostgreSQL Migration Script

Migrates schema and data from Oracle to PostgreSQL.
Creates database, tables, and copies data with type conversion.

Usage:
    python jobs/oracle_to_postgres_migration.py --source dev_source --table-filter "TABLE1|TABLE2"
    python jobs/oracle_to_postgres_migration.py --source dev_source --schema-only
    python jobs/oracle_to_postgres_migration.py --source dev_source --resume-from S_CONTACT
"""

import argparse
import logging
import sys
import os
import time
from typing import Dict, List, Tuple, Any
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import jaydebeapi

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lib.config_loader import get_source_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/oracle_to_postgres_migration.log')
    ]
)
logger = logging.getLogger(__name__)


class TypeConverter:
    """Convert Oracle types to PostgreSQL types"""

    @staticmethod
    def oracle_to_postgres(oracle_type: str, data_length: int = None, data_precision: int = None,
                          data_scale: int = None) -> str:
        """
        Convert Oracle data type to PostgreSQL equivalent

        Args:
            oracle_type: Oracle data type name
            data_length: Character length for string types
            data_precision: Numeric precision
            data_scale: Numeric scale

        Returns:
            PostgreSQL data type
        """
        oracle_type = oracle_type.upper()

        # String types
        if oracle_type in ('VARCHAR2', 'VARCHAR'):
            if data_length and data_length <= 10485760:  # PostgreSQL max
                return f'VARCHAR({data_length})'
            return 'TEXT'

        if oracle_type == 'CHAR':
            if data_length:
                return f'CHAR({data_length})'
            return 'CHAR(1)'

        if oracle_type in ('NVARCHAR2', 'NVARCHAR'):
            if data_length:
                return f'VARCHAR({data_length})'
            return 'TEXT'

        if oracle_type == 'NCHAR':
            if data_length:
                return f'CHAR({data_length})'
            return 'CHAR(1)'

        # Large objects
        if oracle_type in ('CLOB', 'NCLOB', 'LONG'):
            return 'TEXT'

        if oracle_type in ('BLOB', 'RAW', 'LONG RAW'):
            return 'BYTEA'

        # Numeric types
        if oracle_type == 'NUMBER':
            if data_precision is None and data_scale is None:
                return 'NUMERIC'  # Arbitrary precision
            elif data_scale is None or data_scale == 0:
                # Integer types
                if data_precision is None:
                    return 'NUMERIC'
                elif data_precision <= 4:
                    return 'SMALLINT'
                elif data_precision <= 9:
                    return 'INTEGER'
                elif data_precision <= 18:
                    return 'BIGINT'
                else:
                    return f'NUMERIC({data_precision})'
            else:
                # Decimal types
                return f'NUMERIC({data_precision},{data_scale})'

        if oracle_type in ('BINARY_FLOAT', 'FLOAT'):
            return 'REAL'

        if oracle_type in ('BINARY_DOUBLE', 'DOUBLE PRECISION'):
            return 'DOUBLE PRECISION'

        # Date/Time types
        if oracle_type == 'DATE':
            return 'TIMESTAMP'  # Oracle DATE includes time

        if oracle_type.startswith('TIMESTAMP'):
            if 'WITH TIME ZONE' in oracle_type:
                return 'TIMESTAMP WITH TIME ZONE'
            return 'TIMESTAMP'

        # Other types
        if oracle_type == 'ROWID':
            return 'VARCHAR(18)'

        if oracle_type == 'XMLTYPE':
            return 'XML'

        # Default fallback
        logger.warning(f"Unknown Oracle type '{oracle_type}', defaulting to TEXT")
        return 'TEXT'


class OracleToPostgresMigration:
    """Handles migration from Oracle to PostgreSQL"""

    def __init__(self, oracle_config: Dict[str, Any], pg_host: str, pg_port: int,
                 pg_user: str, pg_password: str, pg_database: str):
        """
        Initialize migration

        Args:
            oracle_config: Oracle source configuration
            pg_host: PostgreSQL host
            pg_port: PostgreSQL port
            pg_user: PostgreSQL username
            pg_password: PostgreSQL password
            pg_database: PostgreSQL target database name
        """
        self.oracle_config = oracle_config
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_database = pg_database
        self.type_converter = TypeConverter()

        # Oracle connection
        self.oracle_conn = None
        self.oracle_cursor = None

        # PostgreSQL connections
        self.pg_admin_conn = None  # For database creation
        self.pg_conn = None  # For data operations
        self.pg_cursor = None

    def connect_oracle(self):
        """Connect to Oracle database"""
        logger.info("Connecting to Oracle...")

        # Get credentials from config (already substituted with env vars)
        db_config = self.oracle_config['database_connection']
        username = db_config['username']
        password = db_config['password']
        schema = db_config['schema']

        # Build JDBC URL
        jdbc_url = f"jdbc:oracle:thin:@{db_config['host']}:{db_config['port']}/{db_config['service_name']}"

        # Oracle JDBC driver configuration
        # Uses standard Oracle JDBC thin driver
        driver = 'oracle.jdbc.driver.OracleDriver'

        # Try to find Oracle JDBC driver JAR
        # Common locations for local development
        possible_driver_paths = [
            'jars/ojdbc8.jar',
            'jars/ojdbc11.jar',
            'drivers/ojdbc11.jar',
            'drivers/ojdbc8.jar',
            '/opt/oracle/ojdbc11.jar',
            '/opt/oracle/ojdbc8.jar',
            os.path.expanduser('~/drivers/ojdbc11.jar'),
            os.path.expanduser('~/drivers/ojdbc8.jar'),
        ]

        driver_path = None
        for path in possible_driver_paths:
            if os.path.exists(path):
                driver_path = path
                logger.info(f"Found Oracle JDBC driver: {path}")
                break

        if not driver_path:
            raise FileNotFoundError(
                "Oracle JDBC driver not found. Please download ojdbc8.jar or ojdbc11.jar and place it in:\n"
                "  - drivers/ojdbc11.jar (relative to project root), or\n"
                "  - /opt/oracle/ojdbc11.jar, or\n"
                "  - ~/drivers/ojdbc11.jar\n"
                "Download from: https://www.oracle.com/database/technologies/jdbc-downloads.html"
            )

        # Connect using jaydebeapi
        self.oracle_conn = jaydebeapi.connect(
            driver,
            jdbc_url,
            [username, password],
            driver_path
        )
        self.oracle_cursor = self.oracle_conn.cursor()

        # Set schema
        self.oracle_cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {schema}")

        logger.info("Connected to Oracle successfully")

    def connect_postgres_admin(self):
        """Connect to PostgreSQL as admin (to postgres database)"""
        logger.info("Connecting to PostgreSQL (postgres database)...")
        self.pg_admin_conn = psycopg2.connect(
            host=self.pg_host,
            port=self.pg_port,
            user=self.pg_user,
            password=self.pg_password,
            database='postgres'
        )
        self.pg_admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logger.info("Connected to PostgreSQL admin successfully")

    def connect_postgres(self):
        """Connect to PostgreSQL target database"""
        logger.info(f"Connecting to PostgreSQL ({self.pg_database} database)...")
        self.pg_conn = psycopg2.connect(
            host=self.pg_host,
            port=self.pg_port,
            user=self.pg_user,
            password=self.pg_password,
            database=self.pg_database
        )
        self.pg_cursor = self.pg_conn.cursor()
        logger.info("Connected to PostgreSQL target database successfully")

    def create_database(self):
        """Create PostgreSQL database if it doesn't exist"""
        logger.info(f"Creating database '{self.pg_database}'...")

        cursor = self.pg_admin_conn.cursor()

        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (self.pg_database,)
        )
        exists = cursor.fetchone()

        if exists:
            logger.info(f"Database '{self.pg_database}' already exists")
        else:
            cursor.execute(f'CREATE DATABASE {self.pg_database}')
            logger.info(f"Database '{self.pg_database}' created successfully")

        cursor.close()

    def get_oracle_tables(self, table_filter: str = None) -> List[str]:
        """
        Get list of tables from Oracle

        Args:
            table_filter: Optional regex filter for table names

        Returns:
            List of table names
        """
        logger.info("Fetching Oracle table list...")

        # Get schema from config
        schema = self.oracle_config['database_connection']['schema']

        query = f"""
            SELECT table_name
            FROM all_tables
            WHERE owner = '{schema.upper()}'
            AND table_name NOT LIKE 'BIN$%'
            ORDER BY table_name
        """

        self.oracle_cursor.execute(query)
        all_tables = [row[0] for row in self.oracle_cursor.fetchall()]

        if table_filter:
            import re
            pattern = re.compile(table_filter, re.IGNORECASE)
            tables = [t for t in all_tables if pattern.search(t)]
            logger.info(f"Found {len(tables)} tables matching filter '{table_filter}'")
        else:
            tables = all_tables
            logger.info(f"Found {len(tables)} tables")

        return tables

    def get_oracle_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get schema for Oracle table

        Args:
            table_name: Oracle table name

        Returns:
            List of column definitions
        """
        # Get schema from config
        schema = self.oracle_config['database_connection']['schema']

        query = f"""
            SELECT
                column_name,
                data_type,
                data_length,
                data_precision,
                data_scale,
                nullable,
                column_id
            FROM all_tab_columns
            WHERE owner = '{schema.upper()}'
            AND table_name = '{table_name}'
            ORDER BY column_id
        """

        self.oracle_cursor.execute(query)

        columns = []
        for row in self.oracle_cursor.fetchall():
            columns.append({
                'name': row[0],
                'oracle_type': row[1],
                'data_length': row[2],
                'data_precision': row[3],
                'data_scale': row[4],
                'nullable': row[5] == 'Y',
                'column_id': row[6]
            })

        return columns

    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get primary key columns for Oracle table

        Args:
            table_name: Oracle table name

        Returns:
            List of primary key column names
        """
        # Get schema from config
        schema = self.oracle_config['database_connection']['schema']

        query = f"""
            SELECT cols.column_name
            FROM all_constraints cons
            JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
              AND cons.owner = cols.owner
            WHERE cons.constraint_type = 'P'
              AND cons.owner = '{schema.upper()}'
              AND cons.table_name = '{table_name}'
            ORDER BY cols.position
        """

        self.oracle_cursor.execute(query)
        return [row[0] for row in self.oracle_cursor.fetchall()]

    def create_postgres_table(self, table_name: str, columns: List[Dict[str, Any]],
                             primary_keys: List[str]):
        """
        Create PostgreSQL table with converted schema

        Args:
            table_name: Table name
            columns: Column definitions from Oracle
            primary_keys: Primary key column names
        """
        logger.info(f"{table_name}: Creating PostgreSQL table...")

        # Check if table exists
        self.pg_cursor.execute("""
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        """, (table_name.lower(),))

        if self.pg_cursor.fetchone():
            logger.info(f"{table_name}: Table already exists, dropping...")
            self.pg_cursor.execute(f'DROP TABLE "{table_name.lower()}" CASCADE')
            self.pg_conn.commit()

        # Build CREATE TABLE statement
        column_defs = []
        for col in columns:
            pg_type = self.type_converter.oracle_to_postgres(
                col['oracle_type'],
                col['data_length'],
                col['data_precision'],
                col['data_scale']
            )

            nullable = '' if col['nullable'] else 'NOT NULL'
            column_defs.append(f'  "{col["name"].lower()}" {pg_type} {nullable}')

        # Add primary key constraint
        if primary_keys:
            pk_cols = ', '.join([f'"{pk.lower()}"' for pk in primary_keys])
            column_defs.append(f'  PRIMARY KEY ({pk_cols})')

        create_sql = f'CREATE TABLE "{table_name.lower()}" (\n' + ',\n'.join(column_defs) + '\n)'

        logger.info(f"{table_name}: Executing CREATE TABLE")
        self.pg_cursor.execute(create_sql)
        self.pg_conn.commit()

        logger.info(f"{table_name}: Table created successfully")

    def migrate_table_data(self, table_name: str, batch_size: int = 1000):
        """
        Migrate data from Oracle table to PostgreSQL

        Args:
            table_name: Table name
            batch_size: Number of rows per batch
        """
        logger.info(f"{table_name}: Starting data migration...")

        # Get row count
        self.oracle_cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        total_rows = self.oracle_cursor.fetchone()[0]
        logger.info(f"{table_name}: Total rows to migrate: {total_rows:,}")

        if total_rows == 0:
            logger.info(f"{table_name}: No data to migrate")
            return

        # Get column names
        self.oracle_cursor.execute(f'SELECT * FROM {table_name} WHERE 1=0')
        columns = [desc[0] for desc in self.oracle_cursor.description]

        # Prepare INSERT statement
        pg_table_name = table_name.lower()
        pg_columns = [f'"{col.lower()}"' for col in columns]
        placeholders = ', '.join(['%s'] * len(columns))
        insert_sql = f'INSERT INTO "{pg_table_name}" ({", ".join(pg_columns)}) VALUES ({placeholders})'

        # Fetch and insert data in batches
        self.oracle_cursor.execute(f'SELECT * FROM {table_name}')

        rows_migrated = 0
        batch = []
        start_time = time.time()

        while True:
            row = self.oracle_cursor.fetchone()
            if row is None:
                # Insert final batch
                if batch:
                    self.pg_cursor.executemany(insert_sql, batch)
                    self.pg_conn.commit()
                    rows_migrated += len(batch)
                break

            batch.append(row)

            if len(batch) >= batch_size:
                self.pg_cursor.executemany(insert_sql, batch)
                self.pg_conn.commit()
                rows_migrated += len(batch)

                # Progress update
                elapsed = time.time() - start_time
                rate = rows_migrated / elapsed if elapsed > 0 else 0
                pct = (rows_migrated / total_rows) * 100 if total_rows > 0 else 0
                logger.info(f"{table_name}: Migrated {rows_migrated:,}/{total_rows:,} rows "
                          f"({pct:.1f}%) - {rate:.0f} rows/sec")

                batch = []

        elapsed = time.time() - start_time
        rate = rows_migrated / elapsed if elapsed > 0 else 0
        logger.info(f"{table_name}: Migration complete - {rows_migrated:,} rows in {elapsed:.1f}s "
                   f"({rate:.0f} rows/sec)")

    def migrate_table(self, table_name: str, schema_only: bool = False):
        """
        Migrate a single table (schema and data)

        Args:
            table_name: Table name
            schema_only: If True, only create schema without data
        """
        logger.info(f"{table_name}: Starting migration...")

        try:
            # Get schema
            columns = self.get_oracle_table_schema(table_name)
            primary_keys = self.get_primary_keys(table_name)

            # Create table
            self.create_postgres_table(table_name, columns, primary_keys)

            # Migrate data
            if not schema_only:
                self.migrate_table_data(table_name)

            logger.info(f"{table_name}: Migration completed successfully")
            return True

        except Exception as e:
            logger.error(f"{table_name}: Migration failed - {e}", exc_info=True)
            return False

    def run_migration(self, table_filter: str = None, schema_only: bool = False,
                     resume_from: str = None):
        """
        Run full migration

        Args:
            table_filter: Optional regex filter for table names
            schema_only: If True, only create schemas without data
            resume_from: Table name to resume from (skip tables before this)
        """
        try:
            # Connect to Oracle
            self.connect_oracle()

            # Get table list
            tables = self.get_oracle_tables(table_filter)

            # Resume logic
            if resume_from:
                try:
                    resume_idx = tables.index(resume_from)
                    tables = tables[resume_idx:]
                    logger.info(f"Resuming from table '{resume_from}' ({len(tables)} tables remaining)")
                except ValueError:
                    logger.error(f"Resume table '{resume_from}' not found in table list")
                    return

            # Create PostgreSQL database
            self.connect_postgres_admin()
            self.create_database()
            self.pg_admin_conn.close()

            # Connect to target database
            self.connect_postgres()

            # Migrate tables
            success_count = 0
            failed_tables = []

            for i, table_name in enumerate(tables, 1):
                logger.info(f"[{i}/{len(tables)}] Processing {table_name}...")

                if self.migrate_table(table_name, schema_only):
                    success_count += 1
                else:
                    failed_tables.append(table_name)

            # Summary
            logger.info("=" * 80)
            logger.info("MIGRATION SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total tables: {len(tables)}")
            logger.info(f"Successful: {success_count}")
            logger.info(f"Failed: {len(failed_tables)}")

            if failed_tables:
                logger.error(f"Failed tables: {', '.join(failed_tables)}")

        finally:
            # Close connections
            if self.oracle_cursor:
                self.oracle_cursor.close()
            if self.oracle_conn:
                self.oracle_conn.close()
            if self.pg_cursor:
                self.pg_cursor.close()
            if self.pg_conn:
                self.pg_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='Migrate Oracle schema to PostgreSQL'
    )
    parser.add_argument(
        '--source',
        required=True,
        help='Source name from sources.yaml (e.g., dev_source)'
    )
    parser.add_argument(
        '--table-filter',
        help='Regex filter for table names (e.g., "S_CONTACT|S_ORG_EXT")'
    )
    parser.add_argument(
        '--schema-only',
        action='store_true',
        help='Only create schema, skip data migration'
    )
    parser.add_argument(
        '--resume-from',
        help='Resume from specific table name'
    )
    parser.add_argument(
        '--pg-host',
        default='postgres',
        help='PostgreSQL host (default: postgres)'
    )
    parser.add_argument(
        '--pg-port',
        type=int,
        default=5432,
        help='PostgreSQL port (default: 5432)'
    )
    parser.add_argument(
        '--pg-user',
        default='postgres',
        help='PostgreSQL username (default: postgres)'
    )
    parser.add_argument(
        '--pg-password',
        default='password',
        help='PostgreSQL password (default: password)'
    )
    parser.add_argument(
        '--pg-database',
        default='unknown',
        help='PostgreSQL target database (default: unknown)'
    )

    args = parser.parse_args()

    # Load Oracle configuration
    logger.info(f"Loading configuration for source '{args.source}'...")
    oracle_config = get_source_config(args.source)

    # Validate Oracle config
    if oracle_config.get('database_type') != 'oracle':
        logger.error(f"Source '{args.source}' is not an Oracle database")
        sys.exit(1)

    # Create migration instance
    migration = OracleToPostgresMigration(
        oracle_config=oracle_config,
        pg_host=args.pg_host,
        pg_port=args.pg_port,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
        pg_database=args.pg_database
    )

    # Run migration
    migration.run_migration(
        table_filter=args.table_filter,
        schema_only=args.schema_only,
        resume_from=args.resume_from
    )

    logger.info("Migration process completed")


if __name__ == '__main__':
    main()
