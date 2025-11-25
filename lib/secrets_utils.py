"""
Secrets management utilities for ETL pipeline
Supports both Databricks Secrets (cloud) and environment variables (local dev)
"""

import os
import logging

logger = logging.getLogger(__name__)


class SecretsManager:
    """Unified secrets management for local and Databricks environments"""

    # Cache for Databricks environment detection
    _is_databricks = None
    _dbutils = None

    @classmethod
    def is_databricks_environment(cls) -> bool:
        """
        Detect if running in Databricks environment

        Returns:
            bool: True if running in Databricks, False otherwise
        """
        if cls._is_databricks is None:
            try:
                # Try to import dbutils (only available in Databricks)
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
                if spark:
                    cls._dbutils = DBUtils(spark)
                    cls._is_databricks = True
                    logger.info("Databricks environment detected")
                else:
                    cls._is_databricks = False
            except (ImportError, Exception):
                cls._is_databricks = False
                logger.debug("Local environment detected (not Databricks)")

        return cls._is_databricks

    @classmethod
    def get_secret(cls, key: str, scope: str = "etl", default: str = None) -> str:
        """
        Get secret value from Databricks Secrets or environment variables

        Priority order:
        1. Databricks Secrets (if in Databricks environment)
        2. Environment variables (local dev or fallback)
        3. Default value (if provided)

        Args:
            key: Secret key name (e.g., 'oracle_password')
            scope: Databricks secret scope name (default: 'etl')
            default: Default value if secret not found

        Returns:
            Secret value as string

        Raises:
            ValueError: If secret not found and no default provided
        """
        # Try Databricks Secrets first if in Databricks environment
        if cls.is_databricks_environment():
            try:
                value = cls._dbutils.secrets.get(scope=scope, key=key)
                logger.debug(f"Retrieved secret '{key}' from Databricks Secrets (scope: {scope})")
                return value
            except Exception as e:
                logger.warning(f"Failed to get secret '{key}' from Databricks Secrets (scope: {scope}): {e}")
                # Fall through to environment variables

        # Try environment variable (local dev or Databricks fallback)
        env_var = key.upper()
        value = os.environ.get(env_var)
        if value:
            logger.debug(f"Retrieved secret '{key}' from environment variable {env_var}")
            return value

        # Use default if provided
        if default is not None:
            logger.debug(f"Using default value for secret '{key}'")
            return default

        # No secret found
        raise ValueError(
            f"Secret '{key}' not found. "
            f"Tried: Databricks Secrets (scope={scope}), environment variable {env_var}"
        )

    @classmethod
    def resolve_password(cls, password_value: str, scope: str = "etl") -> str:
        """
        Resolve password from config value

        Supports multiple formats:
        - ${SECRET_KEY} - Databricks Secret or environment variable
        - ${SECRET_KEY:default} - With default fallback
        - plain_text_password - Direct password (not recommended)

        Args:
            password_value: Password config value
            scope: Databricks secret scope

        Returns:
            Resolved password string

        Examples:
            ${ORACLE_PASSWORD} -> Gets from Databricks Secrets or env var
            ${ORACLE_PASSWORD:mypass} -> Gets from secrets, falls back to 'mypass'
            my_plain_password -> Returns as-is (not recommended for production)
        """
        if not password_value:
            raise ValueError("Password value is empty")

        # Check if it's a secret reference: ${KEY} or ${KEY:default}
        if password_value.startswith("${") and password_value.endswith("}"):
            # Extract key and optional default
            content = password_value[2:-1]  # Remove ${ and }

            if ":" in content:
                key, default = content.split(":", 1)
            else:
                key = content
                default = None

            return cls.get_secret(key.strip(), scope=scope, default=default)

        # Plain text password (not recommended but supported)
        logger.warning("Using plain text password from config (not recommended for production)")
        return password_value


def get_secret(key: str, scope: str = "etl", default: str = None) -> str:
    """
    Convenience function for getting secrets

    See SecretsManager.get_secret() for full documentation
    """
    return SecretsManager.get_secret(key, scope, default)


def resolve_password(password_value: str, scope: str = "etl") -> str:
    """
    Convenience function for resolving passwords

    See SecretsManager.resolve_password() for full documentation
    """
    return SecretsManager.resolve_password(password_value, scope)
