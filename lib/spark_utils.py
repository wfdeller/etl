"""
Spark session utilities for ETL pipeline
Provides centralized SparkSession creation with consistent configuration
Supports local development, Databricks, and S3/MinIO object storage
"""

import os
import logging
from pathlib import Path
from typing import Optional, List, Dict
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


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

    # Hadoop AWS package for S3 support
    HADOOP_AWS_PACKAGE = 'org.apache.hadoop:hadoop-aws:3.3.4'

    @staticmethod
    def is_databricks_environment() -> bool:
        """
        Detect if running in Databricks environment

        Returns:
            bool: True if running in Databricks
        """
        return 'DATABRICKS_RUNTIME_VERSION' in os.environ

    @classmethod
    def create(cls,
               app_name: str,
               db_type: Optional[str] = None,
               enable_kafka: bool = False,
               enable_s3: bool = False,
               s3_config: Optional[Dict[str, str]] = None,
               jar_dir: Optional[Path] = None,
               performance_tuning: bool = True,
               local_mode: bool = True) -> SparkSession:
        """
        Create a Spark session with appropriate configuration

        Args:
            app_name: Application name for Spark UI
            db_type: Database type ('oracle', 'postgres', or None for Iceberg-only)
            enable_kafka: Whether to include Kafka support
            enable_s3: Whether to configure S3/MinIO support
            s3_config: S3 configuration dict with keys: endpoint, access_key, secret_key, path_style_access
            jar_dir: Custom JAR directory (default: PROJECT_ROOT/jars)
            performance_tuning: Apply performance tuning configs
            local_mode: Configure for local development (bind address)

        Returns:
            Configured SparkSession

        Raises:
            ValueError: If required environment variables are missing

        Examples:
            # Local development with Iceberg
            spark = SparkSessionFactory.create("MyApp")

            # Databricks with Unity Catalog (auto-detected)
            spark = SparkSessionFactory.create("MyApp")

            # Local with MinIO/S3
            spark = SparkSessionFactory.create(
                "MyApp",
                enable_s3=True,
                s3_config={
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin"
                }
            )
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

        # Add packages if needed (Kafka and/or S3)
        packages = []
        if enable_kafka:
            packages.append(cls.KAFKA_PACKAGE)
        if enable_s3:
            packages.append(cls.HADOOP_AWS_PACKAGE)

        if packages:
            builder.config("spark.jars.packages", ",".join(packages))

        # Add local mode configuration
        if local_mode:
            cls._add_local_mode_config(builder)

        # Add Iceberg configuration
        cls._add_iceberg_config(builder, catalog_name, warehouse_path)

        # Add S3 configuration if needed
        if enable_s3 and s3_config:
            cls._add_s3_config(builder, s3_config)

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
    def _add_s3_config(builder: SparkSession.builder, s3_config: Dict[str, str]) -> None:
        """Add S3/MinIO configuration"""
        # Set S3A endpoint (for MinIO or custom S3-compatible storage)
        if 'endpoint' in s3_config:
            builder.config("spark.hadoop.fs.s3a.endpoint", s3_config['endpoint'])

        # Set S3A credentials
        if 'access_key' in s3_config:
            builder.config("spark.hadoop.fs.s3a.access.key", s3_config['access_key'])
        if 'secret_key' in s3_config:
            builder.config("spark.hadoop.fs.s3a.secret.key", s3_config['secret_key'])

        # Path style access (required for MinIO, optional for AWS S3)
        path_style = s3_config.get('path_style_access', 'true')
        builder.config("spark.hadoop.fs.s3a.path.style.access", path_style)

        # S3A filesystem implementation
        builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Connection settings for better reliability
        builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # MinIO default
        builder.config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        builder.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        builder.config("spark.hadoop.fs.s3a.connection.timeout", "200000")

    @staticmethod
    def _add_performance_config(builder: SparkSession.builder) -> None:
        """Add performance tuning configuration"""
        builder.config("spark.sql.adaptive.enabled", "true")
        builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        builder.config("spark.driver.maxResultSize", "2g")
        builder.config("spark.rpc.message.maxSize", "256")
        builder.config("spark.network.timeout", "600s")
        builder.config("spark.executor.heartbeatInterval", "60s")
