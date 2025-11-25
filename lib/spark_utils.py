"""
Spark session utilities for ETL pipeline
Provides centralized SparkSession creation with consistent configuration
"""

import os
from pathlib import Path
from typing import Optional, List
from pyspark.sql import SparkSession


class SparkSessionFactory:
    """Factory for creating Spark sessions with ETL-specific configuration"""

    # Default JAR paths relative to project root
    DEFAULT_JAR_DIR = Path(__file__).parent.parent / 'jars'

    # JAR filenames
    ICEBERG_JAR = 'iceberg-spark-runtime.jar'
    ORACLE_JAR = 'ojdbc8.jar'
    POSTGRES_JAR = 'postgresql.jar'

    # Kafka package (maven coordinates)
    KAFKA_PACKAGE = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'

    @classmethod
    def create(cls,
               app_name: str,
               db_type: Optional[str] = None,
               enable_kafka: bool = False,
               jar_dir: Optional[Path] = None,
               performance_tuning: bool = True,
               local_mode: bool = True) -> SparkSession:
        """
        Create a Spark session with appropriate configuration

        Args:
            app_name: Application name for Spark UI
            db_type: Database type ('oracle', 'postgres', or None for Iceberg-only)
            enable_kafka: Whether to include Kafka support
            jar_dir: Custom JAR directory (default: PROJECT_ROOT/jars)
            performance_tuning: Apply performance tuning configs
            local_mode: Configure for local development (bind address)

        Returns:
            Configured SparkSession

        Raises:
            ValueError: If required environment variables are missing
        """

        # Validate required environment variables
        catalog_name = cls._get_required_env('CATALOG_NAME')
        warehouse_path = cls._get_required_env('WAREHOUSE_PATH')

        # Use default JAR directory if not specified
        if jar_dir is None:
            jar_dir = cls.DEFAULT_JAR_DIR

        # Build Spark session
        builder = SparkSession.builder.appName(app_name)

        # Add JARs and classpath
        cls._add_jars_and_classpath(builder, db_type, jar_dir)

        # Add Kafka package if needed
        if enable_kafka:
            builder.config("spark.jars.packages", cls.KAFKA_PACKAGE)

        # Add local mode configuration
        if local_mode:
            cls._add_local_mode_config(builder)

        # Add Iceberg configuration
        cls._add_iceberg_config(builder, catalog_name, warehouse_path)

        # Add performance tuning
        if performance_tuning:
            cls._add_performance_config(builder)

        return builder.getOrCreate()

    @staticmethod
    def _get_required_env(var_name: str) -> str:
        """Get required environment variable or raise error"""
        value = os.environ.get(var_name)
        if not value:
            raise ValueError(f"{var_name} environment variable must be set")
        return value

    @classmethod
    def _add_jars_and_classpath(cls, builder: SparkSession.builder,
                                  db_type: Optional[str],
                                  jar_dir: Path) -> None:
        """Add JARs and classpath configuration"""

        jars = []

        # Always include Iceberg JAR
        iceberg_jar = jar_dir / cls.ICEBERG_JAR
        if iceberg_jar.exists():
            jars.append(str(iceberg_jar))

        # Add database-specific JDBC driver
        if db_type == 'oracle':
            oracle_jar = jar_dir / cls.ORACLE_JAR
            if oracle_jar.exists():
                jars.append(str(oracle_jar))
        elif db_type == 'postgres':
            postgres_jar = jar_dir / cls.POSTGRES_JAR
            if postgres_jar.exists():
                jars.append(str(postgres_jar))
        elif db_type is not None:
            raise ValueError(f"Unsupported database type: {db_type}. Must be 'oracle', 'postgres', or None")

        # Configure JARs if any were found
        if jars:
            classpath = ":".join(jars)
            builder.config("spark.jars", ",".join(jars))
            builder.config("spark.driver.extraClassPath", classpath)
            builder.config("spark.executor.extraClassPath", classpath)

    @staticmethod
    def _add_local_mode_config(builder: SparkSession.builder) -> None:
        """Add local development mode configuration"""
        builder.config("spark.driver.bindAddress", "127.0.0.1")
        builder.config("spark.driver.host", "localhost")

    @staticmethod
    def _add_iceberg_config(builder: SparkSession.builder,
                            catalog_name: str,
                            warehouse_path: str) -> None:
        """Add Iceberg catalog configuration"""
        builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}.type",
            "hadoop"
        )
        builder.config(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            warehouse_path
        )

    @staticmethod
    def _add_performance_config(builder: SparkSession.builder) -> None:
        """Add performance tuning configuration"""
        builder.config("spark.sql.adaptive.enabled", "true")
        builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        builder.config("spark.driver.maxResultSize", "2g")
        builder.config("spark.rpc.message.maxSize", "256")
        builder.config("spark.network.timeout", "600s")
        builder.config("spark.executor.heartbeatInterval", "60s")
