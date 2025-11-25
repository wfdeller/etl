"""
JDBC utilities for ETL pipeline
Provides centralized JDBC connection string building and configuration
"""

from typing import Dict, Optional


class DatabaseConnectionBuilder:
    """Builder for creating JDBC connection configurations"""

    @staticmethod
    def build_jdbc_config(source_config: dict) -> dict:
        """
        Build JDBC connection configuration from source config

        Args:
            source_config: Source configuration dict from sources.yaml

        Returns:
            dict with keys: url, user, password, schema

        Raises:
            ValueError: If database type is unsupported or config is invalid
        """

        db_type = source_config.get('database_type')
        if not db_type:
            raise ValueError("Missing 'database_type' in source configuration")

        db_config = source_config.get('database_connection')
        if not db_config:
            raise ValueError("Missing 'database_connection' in source configuration")

        # Build JDBC URL based on database type
        if db_type == 'oracle':
            jdbc_url = DatabaseConnectionBuilder._build_oracle_url(db_config)
        elif db_type == 'postgres':
            jdbc_url = DatabaseConnectionBuilder._build_postgres_url(db_config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}. Must be 'oracle' or 'postgres'")

        # Return connection dict
        return {
            'url': jdbc_url,
            'user': db_config['username'],
            'password': db_config['password'],
            'schema': db_config['schema']
        }

    @staticmethod
    def _build_oracle_url(db_config: dict) -> str:
        """
        Build Oracle JDBC URL

        Args:
            db_config: Database connection config

        Returns:
            Oracle JDBC URL string

        Raises:
            ValueError: If required config is missing
        """

        required_keys = ['host', 'port', 'service_name']
        for key in required_keys:
            if key not in db_config:
                raise ValueError(f"Missing required Oracle config key: {key}")

        return f"jdbc:oracle:thin:@{db_config['host']}:{db_config['port']}/{db_config['service_name']}"

    @staticmethod
    def _build_postgres_url(db_config: dict) -> str:
        """
        Build PostgreSQL JDBC URL

        Args:
            db_config: Database connection config

        Returns:
            PostgreSQL JDBC URL string

        Raises:
            ValueError: If required config is missing
        """

        required_keys = ['host', 'port', 'database']
        for key in required_keys:
            if key not in db_config:
                raise ValueError(f"Missing required Postgres config key: {key}")

        return f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"

    @staticmethod
    def validate_config(source_config: dict, source_name: str) -> None:
        """
        Validate that all required configuration is present

        Args:
            source_config: Source configuration dict
            source_name: Source name for error messages

        Raises:
            ValueError: If configuration is invalid or incomplete
        """

        required_top_level = ['database_type', 'kafka_topic_prefix', 'iceberg_namespace', 'database_connection']

        for key in required_top_level:
            if key not in source_config:
                raise ValueError(f"Missing required config key '{key}' in source '{source_name}'")

        # Validate database_connection
        db_config = source_config['database_connection']
        db_type = source_config['database_type']

        if db_type == 'oracle':
            required_db_keys = ['host', 'port', 'service_name', 'username', 'password', 'schema']
            for key in required_db_keys:
                if key not in db_config:
                    raise ValueError(f"Missing required database_connection key '{key}' for Oracle source '{source_name}'")

        elif db_type == 'postgres':
            required_db_keys = ['host', 'port', 'database', 'username', 'password', 'schema']
            for key in required_db_keys:
                if key not in db_config:
                    raise ValueError(f"Missing required database_connection key '{key}' for Postgres source '{source_name}'")

        else:
            raise ValueError(f"Unsupported database_type '{db_type}' for source '{source_name}'. Must be 'oracle' or 'postgres'")

    @staticmethod
    def extract_db_identifier(jdbc_url: str) -> str:
        """
        Extract a human-readable database identifier from JDBC URL

        Args:
            jdbc_url: JDBC connection URL

        Returns:
            Database identifier (hostname or connection string suffix)
        """

        # Extract host/service from URL
        if '@' in jdbc_url:
            # Oracle format: jdbc:oracle:thin:@host:port/service
            return jdbc_url.split('@')[-1]
        elif '//' in jdbc_url:
            # Postgres format: jdbc:postgresql://host:port/database
            return jdbc_url.split('//')[-1]
        else:
            return jdbc_url
