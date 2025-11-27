# ETL Pipeline - Database to Iceberg Lakehouse

An ETL pipeline for extracting data from Oracle and PostgreSQL databases and loading it into an Apache Iceberg data lakehouse with Change Data Capture (CDC) support.

## Features

- **Dual-Phase Architecture**: Initial bulk load + continuous CDC streaming
- **Multi-Database Support**: Oracle and PostgreSQL with automatic primary key discovery
- **Parallel Processing**: Configurable table-level and partition-level parallelism
- **Fault Tolerance**: Checkpoint/resume capability with automatic retry logic
- **Schema Handling**: Automatic Oracle NUMBER type fixes, LONG column handling
- **CDC Integration**: Debezium-based change capture via Kafka with lag tracking
- **Status Tracking**: Built-in progress tracking in Iceberg
- **Schema Change Tracking**: Automatic detection and audit trail of schema changes
- **Monitoring & Observability**: CloudWatch metrics, structured logging, performance tracking, Dynatrace integration
- **Distributed Tracing**: Correlation IDs for end-to-end request tracking across pipeline stages
- **Enhanced Monitoring**: CDC lag metrics, throughput tracking, Spark JMX metrics for Dynatrace OneAgent
- **Data Quality Validation**: Row count checks, null constraints, type validation, checksums
- **Idempotency**: Hash-based duplicate detection prevents reprocessing
- **Databricks Ready**: Seamless deployment to Databricks with Unity Catalog support
- **Cloud Storage**: AWS S3 and MinIO (S3-compatible for local testing) support
- **Security**: Environment variables + Databricks Secrets management

## Architecture

```
Source DB → Bulk Load → Iceberg Lakehouse
    ↓
Debezium → Kafka → CDC Consumer → Iceberg (MERGE/DELETE)
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture documentation.

## Deployment Options

This ETL pipeline supports two deployment modes:

### 1. Local Development
- Self-managed Spark environment
- Local filesystem storage
- Environment variable-based secrets
- **Best for**: Development, testing, and small-scale deployments

### 2. Databricks Production
- Managed Spark clusters with auto-scaling
- Unity Catalog for governance
- AWS S3 for cloud storage
- Databricks Secrets for credential management
- **Best for**: Production cloud deployments

**The core PySpark code works identically in both modes.** The main differences are deployment method, storage configuration, and secrets management.

---

# Local Development Setup

This section provides step-by-step instructions for setting up the ETL pipeline on your local machine for development and testing.

## Prerequisites

Before starting, ensure you have the following installed:

- **Operating System**: macOS, Linux, or Windows with WSL2
- **Java**: OpenJDK 11 or 17 (required for Apache Spark)
- **Python**: Version 3.9, 3.10, 3.11, or 3.12
- **Git**: For cloning the repository
- **Database Access**: Network access to your Oracle or PostgreSQL source databases

## Step 1: Install Java

Apache Spark requires Java 11 or 17. Check if Java is installed:

```bash
java -version
```

If Java is not installed or you have an incompatible version:

### macOS (using Homebrew)
```bash
# Install Homebrew if not already installed
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Install Java 17
brew install openjdk@17

# Set JAVA_HOME environment variable
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install openjdk-17-jdk

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

### Windows (WSL2)
Follow the Linux instructions above within your WSL2 terminal.

## Step 2: Clone the Repository

```bash
# Clone the repository
git clone <repository-url>
cd etl

# Verify you're in the correct directory
pwd  # Should show: /path/to/etl
ls   # Should show: README.md, lib/, jobs/, config/, etc.
```

## Step 3: Install Python and Create Virtual Environment

### Check Python Version

```bash
python3 --version
```

You should see Python 3.9 or newer (e.g., `Python 3.12.0`).

### Install Python if Needed

**macOS:**
```bash
brew install python@3.12
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install python3.12 python3.12-venv python3-pip
```

### Create Virtual Environment

A virtual environment isolates Python dependencies for this project:

```bash
# Create virtual environment (do this inside the etl/ directory)
python3 -m venv venv

# Activate the virtual environment
# macOS/Linux:
source venv/bin/activate

# Windows WSL2:
source venv/bin/activate

# Your prompt should now show (venv) at the beginning
```

**Important**: You need to activate the virtual environment every time you open a new terminal session:
```bash
cd /path/to/etl
source venv/bin/activate
```

## Step 4: Install Python Dependencies

With the virtual environment activated:

```bash
# Upgrade pip to latest version
pip install --upgrade pip

# Install required Python packages
pip install pyspark==3.5.0 pyyaml psycopg2-binary jaydebeapi jpype1

# Verify installation
pip list | grep -E "pyspark|PyYAML|psycopg2|jaydebeapi|jpype1"
```

You should see:
```
pyspark         3.5.0
PyYAML          6.0.x
psycopg2-binary 2.9.x
jaydebeapi      1.2.x
jpype1          1.6.x
```

**Package purposes:**
- `pyspark` - Apache Spark framework
- `pyyaml` - YAML configuration file parsing
- `psycopg2-binary` - PostgreSQL database adapter
- `jaydebeapi` - JDBC driver access from Python
- `jpype1` - Java-Python bridge (required by jaydebeapi)

## Step 5: Download Required JARs

Download JDBC drivers and Apache Iceberg runtime to the `jars/` directory:

```bash
# Create jars directory in project root
mkdir -p jars

# Download Oracle JDBC Driver (ojdbc8 for Oracle 12c+)
curl -L https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar \
  -o jars/ojdbc8.jar

# Download PostgreSQL JDBC Driver
curl -L https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
  -o jars/postgresql.jar

# Download Apache Iceberg Spark Runtime (for Spark 3.5 with Scala 2.12)
curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
  -o jars/iceberg-spark-runtime.jar

# Verify downloads
ls -lh jars/
```

You should see:
```
iceberg-spark-runtime.jar  (approx 28 MB)
ojdbc8.jar                 (approx 4-5 MB)
postgresql.jar             (approx 1 MB)
```

**JAR purposes:**
- `iceberg-spark-runtime.jar` - Apache Iceberg data lakehouse format support
- `ojdbc8.jar` - Oracle database connectivity
- `postgresql.jar` - PostgreSQL database connectivity

## Step 6: Set Up Environment Variables

The pipeline uses environment variables for configuration and secrets.

### Create Environment Configuration File

Create a file to store your environment variables:

```bash
# Create .env file (this file will NOT be committed to git)
touch .env
chmod 600 .env  # Restrict permissions for security
```

Edit `.env` and add the following:

```bash
# Project directories (update paths to match your system)
export PROJECT_ROOT="$(pwd)"
export CONFIG_DIR="${PROJECT_ROOT}/config"
export CATALOG_NAME="local"
export WAREHOUSE_PATH="file://${PROJECT_ROOT}/warehouse"

# Python/Spark configuration
export PYSPARK_PYTHON="${PROJECT_ROOT}/venv/bin/python"
export PYSPARK_DRIVER_PYTHON="${PROJECT_ROOT}/venv/bin/python"

# Java configuration
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"  # macOS
# export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"  # Linux

# Database credentials (replace with your actual passwords)
export ORACLE_DEV_SIEBEL_PASSWORD="your_oracle_password_here"
export POSTGRES_PROD_PASSWORD="your_postgres_password_here"

# Add more database passwords as needed
# export ORACLE_PROD_PASSWORD="..."
# export POSTGRES_DEV_PASSWORD="..."
```

### Load Environment Variables

Every time you start a new terminal session, load the environment:

```bash
cd /path/to/etl
source venv/bin/activate  # Activate Python virtual environment
source .env               # Load environment variables

# Verify environment variables are set
echo $CONFIG_DIR
echo $WAREHOUSE_PATH
```

**Tip**: Add this to your shell profile for convenience:
```bash
# Add to ~/.bashrc or ~/.zshrc
alias etl-env='cd /path/to/etl && source venv/bin/activate && source .env'

# Then just run:
etl-env
```

## Step 7: Configure Spark (Required for spark-sql CLI)

If you have a standalone Spark installation (not just PySpark), configure it to use the JARs system-wide.

### Check for Spark Installation

```bash
# Check if you have standalone Spark installed
which spark-sql
# If found, continue with configuration below
```

### Configure spark-defaults.conf

If you have Spark installed at `SPARK_HOME` (e.g., `/opt/spark` on macOS, `/usr/local/spark` on Linux):

```bash
# Find your SPARK_HOME
echo $SPARK_HOME
# or
dirname $(dirname $(which spark-submit))

# Edit spark-defaults.conf (may require sudo)
sudo nano $SPARK_HOME/conf/spark-defaults.conf
```

Add the following configuration:

```properties
# Apache Iceberg Configuration
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.local org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type hadoop
spark.sql.catalog.local.warehouse file:///path/to/etl/warehouse

# JAR files (update paths to match your project location)
spark.jars /path/to/etl/jars/iceberg-spark-runtime.jar,/path/to/etl/jars/ojdbc8.jar,/path/to/etl/jars/postgresql.jar
```

**Important**: Replace `/path/to/etl` with your actual project path (e.g., `/Users/yourusername/Development/etl`).

### Verify Configuration

```bash
# Test spark-sql with Iceberg catalog
spark-sql -e "SHOW NAMESPACES IN local"

# If successful, you should see your namespaces (e.g., bronze)
```

**Note**: If you don't have standalone Spark installed (only PySpark), skip this step. The Python jobs will still work using PySpark's built-in configuration.

## Step 8: Create Directory Structure

```bash
# Create required directories
mkdir -p warehouse
mkdir -p logs
mkdir -p checkpoints

# Verify structure
ls -la
```

You should see:
```
config/          # Configuration files
jars/            # JDBC drivers
jobs/            # Python job scripts
lib/             # Python library modules
docs/            # Documentation
warehouse/       # Iceberg table storage (will contain data)
logs/            # Application logs
checkpoints/     # Spark streaming checkpoints
venv/            # Python virtual environment
.env             # Environment variables (DO NOT commit)
```

## Step 9: Configure Database Sources

### Create Configuration from Template

```bash
# Copy template to create your configuration
cp config/sources.yaml.template config/sources.yaml

# Copy Kafka configuration template
cp config/kafka_clusters.yaml.template config/kafka_clusters.yaml
```

### Edit Database Configuration

Edit `config/sources.yaml` to add your database connection details:

```yaml
sources:
  # Example: Oracle Siebel database
  dev_siebel:
    database_type: oracle
    kafka_topic_prefix: dev.siebel
    iceberg_namespace: bronze.siebel

    database_connection:
      host: oracle.example.com       # Your Oracle host
      port: 1521
      service_name: DEVDB            # Your Oracle service name
      username: siebel_user          # Your database username
      password: ${ORACLE_DEV_SIEBEL_PASSWORD}  # References .env variable
      schema: SIEBEL                 # Schema to extract

    bulk_load:
      parallel_tables: 8              # JDBC partitions per table
      parallel_workers: 4             # Tables processed simultaneously
      batch_size: 50000
      skip_empty_tables: false
      handle_long_columns: exclude    # exclude | skip_table | error
      checkpoint_enabled: true
      checkpoint_interval: 50
      max_retries: 3
      retry_backoff: 2
      large_table_threshold: 100000
```

**Important Security Notes**:
- **Never commit** `config/sources.yaml` or `config/kafka_clusters.yaml` with real credentials
- Always use environment variable references: `${VAR_NAME}`
- Add these files to `.gitignore` (already configured in this project)

### Edit Kafka Configuration

Edit `config/kafka_clusters.yaml`:

```yaml
kafka_clusters:
  primary:
    bootstrap_servers: localhost:9092  # Your Kafka broker
    consumer_group_prefix: etl-cdc
    auto_offset_reset: earliest
```

## Step 10: Verify Installation

Run a quick validation to ensure everything is set up correctly:

```bash
# Ensure environment is loaded
source venv/bin/activate
source .env

# Check Python and dependencies
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"

# Check Java
java -version

# Check JDBC drivers
ls -lh jars/

# Check configuration
ls -lh config/sources.yaml config/kafka_clusters.yaml

# Check environment variables
echo "CONFIG_DIR: $CONFIG_DIR"
echo "WAREHOUSE_PATH: $WAREHOUSE_PATH"
echo "CATALOG_NAME: $CATALOG_NAME"
```

If all checks pass, you're ready to run the pipeline!

---

# Running the Pipeline Locally

## Phase 1: Initial Bulk Load

Extract all tables from your source database and load them into Iceberg:

```bash
# Ensure environment is loaded
source venv/bin/activate
source .env

# Run bulk load for a source (using source name from sources.yaml)
python jobs/direct_bulk_load.py --source dev_siebel

# Filter specific tables using SQL LIKE pattern
python jobs/direct_bulk_load.py --source dev_siebel --table-filter "CUSTOMER_%"

# Adjust parallelism for better performance
python jobs/direct_bulk_load.py \
  --source dev_siebel \
  --parallel-tables 16 \
  --parallel-workers 8

# Force full reload (ignore checkpoint and resume from scratch)
python jobs/direct_bulk_load.py --source dev_siebel --no-resume

# Fresh start: Drop ALL tables and reload (DESTRUCTIVE - requires confirmation)
python jobs/direct_bulk_load.py --source dev_siebel --fresh-start

# Fresh start with auto-confirmation (use in automation scripts)
python jobs/direct_bulk_load.py --source dev_siebel --fresh-start --yes
```

### Understanding the Output

The bulk load will:
1. **Discover primary keys** automatically from Oracle/PostgreSQL metadata
2. **Extract tables** in parallel with point-in-time consistency
3. **Write to Iceberg** at: `warehouse/<namespace>/<table_name>/`
4. **Track status** in: `warehouse/<namespace>/_cdc_status/`
5. **Generate Debezium config** at: `config/debezium_<source>_cdc.json`
6. **Log progress** to: `logs/direct_load.log`

Monitor progress:
```bash
# Watch logs in real-time
tail -f logs/direct_load.log

# Check completed tables
python jobs/validate_iceberg_tables.py --source dev_siebel --hide-empty
```

## Phase 2: Configure Debezium CDC

The bulk load automatically generates a Debezium connector configuration. Deploy it to Kafka Connect:

```bash
# Review the generated configuration
cat config/debezium_dev_siebel_cdc.json

# Deploy to Kafka Connect (adjust URL to your Kafka Connect instance)
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium_dev_siebel_cdc.json

# Check connector status
curl http://localhost:8083/connectors/dev_siebel_cdc/status
```

## Phase 3: Start CDC Consumer

Stream changes from Kafka to Iceberg:

```bash
# Ensure environment is loaded
source venv/bin/activate
source .env

# Start CDC consumer
python jobs/cdc_kafka_to_iceberg.py --source dev_siebel

# Higher throughput (process more messages per batch)
python jobs/cdc_kafka_to_iceberg.py --source dev_siebel --batch-size 5000

# Press Ctrl+C to gracefully stop
```

The CDC consumer will:
- Load **primary key metadata** from status tracker
- Use **dynamic MERGE/DELETE** operations (supports composite keys)
- Filter duplicate events using SCN/LSN positions
- Handle schema evolution automatically

Monitor CDC consumer:
```bash
# Watch CDC logs
tail -f logs/cdc_consumer.log

# Check CDC positions
python -c "
from pyspark.sql import SparkSession
from lib.status_tracker import CDCStatusTracker

spark = SparkSession.builder.appName('check').getOrCreate()
tracker = CDCStatusTracker(spark, 'bronze.siebel')
tracker.get_status().show(truncate=False)
"
```

## Validation

Verify that data was loaded correctly:

```bash
# List all tables with row counts
python jobs/validate_iceberg_tables.py --source dev_siebel

# Hide empty tables from output
python jobs/validate_iceberg_tables.py --source dev_siebel --hide-empty

# Skip checking for missing tables
python jobs/validate_iceberg_tables.py --source dev_siebel --no-check-missing
```

---

# Querying Data

Once data is loaded, you can query it using either spark-sql CLI or PySpark.

## Option 1: Using spark-sql CLI

If you configured `spark-defaults.conf` in Step 7, you can use spark-sql directly:

```bash
# Activate environment
source venv/bin/activate
source .env

# List all namespaces (databases)
spark-sql -e "SHOW NAMESPACES IN local"

# List tables in a namespace
spark-sql -e "SHOW TABLES IN local.bronze.siebel_oracle"

# Describe a table
spark-sql -e "DESCRIBE local.bronze.siebel_oracle.S_CONTACT"

# Query data
spark-sql -e "SELECT COUNT(*) FROM local.bronze.siebel_oracle.S_CONTACT"

# Interactive mode
spark-sql
```

In interactive mode:
```sql
-- Show namespaces
SHOW NAMESPACES IN local;

-- Query tables
SELECT * FROM local.bronze.siebel_oracle.S_CONTACT LIMIT 10;

-- Check CDC status
SELECT * FROM local.bronze.siebel_oracle._cdc_status;

-- Time travel query
SELECT * FROM local.bronze.siebel_oracle.S_CONTACT
TIMESTAMP AS OF '2025-01-15 12:00:00'
LIMIT 10;

-- Exit
quit;
```

**Note**: The table path format is `catalog.namespace.table`. For example:
- Catalog: `local` (from `CATALOG_NAME` env var)
- Namespace: `bronze.siebel_oracle` (from `iceberg_namespace` in sources.yaml)
- Table: `S_CONTACT`

## Option 2: Using PySpark

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Query Iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "file:///path/to/etl/warehouse") \
    .getOrCreate()

# List tables
spark.sql("SHOW TABLES IN local.bronze.siebel_oracle").show()

# Query data
spark.sql("SELECT * FROM local.bronze.siebel_oracle.S_CONTACT LIMIT 10").show()

# Check load status
spark.sql("""
    SELECT table_name, load_status, record_count,
           array_size(primary_keys) as pk_count,
           initial_load_end
    FROM local.bronze.siebel_oracle._cdc_status
    WHERE load_status = 'completed'
    ORDER BY initial_load_end DESC
""").show(truncate=False)

# Time travel (query data as of a specific timestamp)
spark.sql("""
    SELECT * FROM local.bronze.siebel_oracle.S_CONTACT
    TIMESTAMP AS OF '2025-01-15 12:00:00'
    LIMIT 10
""").show()
```

---

# Databricks Production Deployment

For production deployments on Databricks with Unity Catalog, S3 storage, and enterprise features, see the comprehensive deployment guide:

**[Databricks Deployment Guide](docs/DATABRICKS_DEPLOYMENT.md)**

The Databricks guide covers:
- Unity Catalog setup and configuration
- S3 storage configuration with IAM roles
- Databricks Secrets management
- Cluster configuration and tuning
- Workflow/job scheduling
- Migration from local to Databricks
- Production best practices

Key differences in Databricks deployment:
- **Storage**: S3 instead of local filesystem
- **Catalog**: Unity Catalog instead of Hadoop catalog
- **Secrets**: Databricks Secrets instead of environment variables
- **Compute**: Managed clusters with auto-scaling
- **Orchestration**: Databricks Workflows instead of manual execution

---

# Troubleshooting

## Common Setup Issues

### Python Import Errors

**Problem**: `ModuleNotFoundError: No module named 'pyspark'`

**Solution**:
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install pyspark==3.5.0 pyyaml

# Verify installation
pip list | grep pyspark
```

### Java Not Found

**Problem**: `java: command not found` or `JAVA_HOME is not set`

**Solution**:
```bash
# Check Java installation
java -version

# If not installed, install Java (see Step 1)
# Set JAVA_HOME in .env file
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"  # macOS
# or
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"  # Linux
```

### JDBC Driver Not Found

**Problem**: `java.lang.ClassNotFoundException: oracle.jdbc.driver.OracleDriver`

**Solution**:
```bash
# Ensure JDBC drivers are in jars/ directory
ls -lh jars/

# If missing, download them (see Step 5)
curl -L https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.9.0.0/ojdbc8-21.9.0.0.jar \
  -o jars/ojdbc8.jar
```

### Database Connection Errors

**Problem**: `ORA-12154: TNS:could not resolve the connect identifier` or connection timeouts

**Solution**:
```bash
# Verify database connectivity
ping oracle.example.com

# Test Oracle connection
sqlplus username/password@//oracle.example.com:1521/DEVDB

# Check firewall rules allow port 1521 (Oracle) or 5432 (PostgreSQL)
# Verify database credentials in sources.yaml and .env
```

## Bulk Load Issues

### Oracle LONG Column Errors

**Problem**: Warnings about LONG columns or subquery errors

**Solution**: This is expected behavior. Oracle LONG columns cannot be read via Spark JDBC.

The pipeline handles this automatically:
- **Default mode**: `exclude` - Skips LONG columns, loads the rest
- CDC (Debezium) will capture LONG column changes going forward

See [docs/ORACLE_LONG_LIMITATIONS.md](docs/ORACLE_LONG_LIMITATIONS.md) for detailed explanation.

### Out of Memory Errors

**Problem**: `java.lang.OutOfMemoryError: Java heap space`

**Solution**:
```bash
# Reduce parallelism
python jobs/direct_bulk_load.py --source dev_siebel --parallel-workers 2

# Lower large_table_threshold in sources.yaml
# This triggers more disk persistence
large_table_threshold: 50000  # Down from 100000
```

### Permission Denied on warehouse/

**Problem**: `PermissionError: [Errno 13] Permission denied: 'warehouse/bronze.siebel'`

**Solution**:
```bash
# Ensure warehouse directory is writable
chmod -R 755 warehouse/
```

## CDC Consumer Issues

### No Events Being Processed

**Problem**: CDC consumer starts but processes no events

**Solution**:
```bash
# 1. Check Debezium connector status
curl http://localhost:8083/connectors/dev_siebel_cdc/status

# 2. Verify Kafka topics exist
kafka-topics.sh --bootstrap-server localhost:9092 --list | grep siebel

# 3. Check consumer is subscribed (look in logs)
grep "Subscribing to Kafka topics" logs/cdc_consumer.log

# 4. Verify Kafka broker is accessible
telnet localhost 9092
```

### Primary Key Not Found Warnings

**Problem**: Logs show "no primary key metadata found, using first column"

**Solution**: This warning appears for tables discovered via CDC before bulk load runs.

To fix:
1. Run bulk load first to discover and store primary keys
2. Or manually configure primary keys in `sources.yaml`:

```yaml
sources:
  dev_siebel:
    # ... other config ...
    primary_keys:
      MY_TABLE: [UNIQUE_ID]           # Single column PK
      OTHER_TABLE: [COL1, COL2]       # Composite key
```

---

# Configuration Reference

## Bulk Load Parameters

| Parameter               | Description                                     | Default |
| ----------------------- | ----------------------------------------------- | ------- |
| `parallel_tables`       | JDBC partitions per table (more = faster)      | 8       |
| `parallel_workers`      | Tables processed simultaneously                 | 4       |
| `batch_size`            | JDBC fetch size (rows per fetch)               | 50000   |
| `skip_empty_tables`     | Skip tables with 0 rows                         | false   |
| `handle_long_columns`   | Oracle LONG handling (exclude/skip_table/error) | exclude |
| `checkpoint_enabled`    | Resume capability after failures                | true    |
| `checkpoint_interval`   | Checkpoint every N tables                       | 50      |
| `max_retries`           | Retry attempts per table                        | 3       |
| `retry_backoff`         | Exponential backoff multiplier                  | 2       |
| `large_table_threshold` | Rows triggering disk persistence                | 100000  |

## CDC Consumer Parameters

| Parameter        | Description                     | Default |
| ---------------- | ------------------------------- | ------- |
| `--batch-size`   | Kafka messages per micro-batch  | 1000    |
| Trigger interval | Micro-batch processing interval | 10s     |

## Primary Key Configuration

The pipeline automatically discovers primary keys from database metadata. To override:

```yaml
# In sources.yaml under each source
primary_keys:
  TABLE_NAME: [COLUMN1]              # Single column
  COMPOSITE_TABLE: [COL1, COL2]      # Composite key
  NO_PK_TABLE: [UNIQUE_ID]           # Manual specification
```

---

# Directory Structure

```
etl/
├── README.md                      # This file
├── TODO.md                        # Planned improvements
├── .env                           # Environment variables (DO NOT commit)
├── docs/
│   ├── ARCHITECTURE.md            # Detailed architecture
│   ├── DATABRICKS_DEPLOYMENT.md   # Databricks deployment guide
│   └── ORACLE_LONG_LIMITATIONS.md # Oracle LONG column handling
├── config/
│   ├── sources.yaml.template      # Database config template
│   ├── kafka_clusters.yaml.template # Kafka config template
│   ├── sources.yaml               # Your database config (DO NOT commit)
│   └── kafka_clusters.yaml        # Your Kafka config (DO NOT commit)
├── lib/                           # Python library modules
│   ├── config_loader.py           # Configuration loader
│   ├── status_tracker.py          # CDC status tracking
│   ├── secrets_utils.py           # Secrets management
│   ├── spark_utils.py             # Spark session factory
│   ├── jdbc_utils.py              # JDBC connection builder
│   ├── iceberg_utils.py           # Iceberg table manager
│   ├── correlation.py             # Correlation ID management for distributed tracing
│   ├── monitoring.py              # Metrics collection and structured logging
│   ├── data_quality.py            # Data quality validation framework
│   ├── schema_tracker.py          # Schema change tracking and notification
│   ├── extractors/                # Database-specific extractors
│   │   ├── base_extractor.py
│   │   ├── oracle_extractor.py
│   │   └── postgres_extractor.py
│   └── schema_fixers/             # Schema transformation logic
│       ├── oracle_fixer.py
│       └── postgres_fixer.py
├── jobs/                          # Main entry point scripts
│   ├── direct_bulk_load.py        # Phase 1: Bulk load
│   ├── cdc_kafka_to_iceberg.py    # Phase 2: CDC consumer
│   ├── validate_iceberg_tables.py # Validation utility
│   └── query_schema_changes.py    # Schema change query tool
├── jars/                          # JDBC drivers
│   ├── iceberg-spark-runtime.jar  # Apache Iceberg Spark runtime
│   ├── ojdbc8.jar                 # Oracle JDBC driver
│   └── postgresql.jar             # PostgreSQL JDBC driver
├── venv/                          # Python virtual environment (local only)
├── warehouse/                     # Iceberg table storage (local only)
├── checkpoints/                   # Spark streaming checkpoints
└── logs/                          # Application logs
    ├── direct_load.log
    └── cdc_consumer.log
```

---

# Security Best Practices

## Credentials Management

**Local Development:**
- Store credentials in `.env` file (never commit)
- Use environment variable references in `sources.yaml`: `${VAR_NAME}`
- Set restrictive permissions: `chmod 600 .env`

**Databricks Production:**
- Use Databricks Secrets for all credentials
- Configure secrets in Databricks workspace
- Reference in config: `${SECRET_NAME}` (automatic fallback to env vars)

## .gitignore Configuration

Ensure these files are in `.gitignore`:
```
.env
config/sources.yaml
config/kafka_clusters.yaml
warehouse/
logs/
checkpoints/
venv/
*.pyc
__pycache__/
```

## Network Security

- Use VPN or private networks for database connectivity
- Enable TLS/SSL for database connections in production
- Configure Kafka with SASL/SSL authentication
- Use IAM roles for S3 access (Databricks)

## Database Permissions

Grant minimal required permissions:

**Oracle:**
```sql
GRANT SELECT ON schema.* TO etl_user;
GRANT SELECT ON v$database TO etl_user;  -- For SCN tracking
```

**PostgreSQL:**
```sql
GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl_user;
GRANT SELECT ON pg_catalog.pg_replication_slots TO etl_user;  -- For CDC
```

---

# Additional Resources

## Documentation

- [Databricks Deployment Guide](docs/DATABRICKS_DEPLOYMENT.md) - Production deployment to Databricks
- [Architecture Documentation](docs/ARCHITECTURE.md) - Detailed system architecture
- [Monitoring Integration Guide](docs/MONITORING_INTEGRATION_GUIDE.md) - CloudWatch, Dynatrace, and observability setup
- [Oracle LONG Limitations](docs/ORACLE_LONG_LIMITATIONS.md) - Oracle LONG column handling
- [Schema Change Tracking](docs/SCHEMA_CHANGE_TRACKING.md) - Schema evolution monitoring

## External Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

---

# Contributing

See [TODO.md](TODO.md) for planned improvements and contribution opportunities.

---

# License

[Specify your license]

---

# Support

For issues, questions, or contributions:
- Create an issue in the repository
- Contact: [Your contact information]
