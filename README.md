# ETL Pipeline - Database to Iceberg Lakehouse

Production-grade ETL pipeline for extracting data from Oracle and PostgreSQL databases and loading it into an Apache Iceberg data lakehouse with Change Data Capture (CDC) support.

## Features

- **Dual-Phase Architecture**: Initial bulk load + continuous CDC streaming
- **Multi-Database Support**: Oracle (11g+) and PostgreSQL
- **Parallel Processing**: Configurable table-level and partition-level parallelism
- **Fault Tolerance**: Checkpoint/resume capability with automatic retry logic
- **Schema Handling**: Automatic Oracle NUMBER type fixes, LONG column handling
- **CDC Integration**: Debezium-based change capture via Kafka
- **Status Tracking**: Built-in progress tracking in Iceberg
- **Security**: Environment variable credential management

## Architecture

```
Source DB → Bulk Load → Iceberg Lakehouse
    ↓
Debezium → Kafka → CDC Consumer → Iceberg (MERGE/DELETE)
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture documentation.

## Prerequisites

### Software Requirements

- Python 3.7+
- Apache Spark 3.5+ (with Iceberg extensions)
- JDBC Drivers:
  - Oracle: `ojdbc8.jar` (or newer)
  - PostgreSQL: `postgresql-42.6.0.jar` (or newer)
- Apache Kafka (for CDC)
- Debezium connectors (Oracle or PostgreSQL)

### System Requirements

- Minimum 8GB RAM for Spark driver
- Sufficient disk space for:
  - Iceberg warehouse (data storage)
  - Spark checkpoints
  - Application logs

## Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd etl
```

### 2. Install Python Dependencies

```bash
pip install pyspark==3.5.0 pyyaml
```

### 3. Download JDBC Drivers

```bash
# Oracle
wget https://download.oracle.com/otn-pub/otn_software/jdbc/ojdbc8.jar \
  -O /opt/spark/jars/ojdbc8.jar

# PostgreSQL
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
  -O /opt/spark/jars/postgresql-42.6.0.jar
```

### 4. Configure Spark

Ensure Spark is configured with Iceberg support:

```bash
# In spark-defaults.conf or pass as --config
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type=hadoop
spark.sql.catalog.local.warehouse=file:///opt/data/lakehouse/warehouse
```

### 5. Create Directory Structure

```bash
mkdir -p /opt/pipeline/{lib,config,logs,checkpoints}
mkdir -p /opt/data/lakehouse/warehouse
```

### 6. Install Pipeline Code

```bash
# Copy library modules
cp -r lib /opt/pipeline/

# Copy job scripts
cp -r jobs /opt/pipeline/

# Copy configuration templates
cp config/*.template /opt/pipeline/config/
```

## Configuration

### 1. Database Source Configuration

Create `/opt/pipeline/config/sources.yaml` from template:

```bash
cp config/sources.yaml.template /opt/pipeline/config/sources.yaml
```

Edit the file to add your database sources:

```yaml
sources:
  my_oracle_db:
    database_type: oracle
    kafka_topic_prefix: prod.myapp
    iceberg_namespace: bronze.myapp

    database_connection:
      host: oracle.example.com
      port: 1521
      service_name: PRODDB
      username: etl_user
      password: ${ORACLE_PASSWORD}  # Use environment variable
      schema: APP_SCHEMA

    bulk_load:
      parallel_tables: 8
      parallel_workers: 4
      batch_size: 50000
      skip_empty_tables: false
      handle_long_columns: exclude  # exclude | skip_table | error
      checkpoint_enabled: true
      max_retries: 3
      retry_backoff: 2
      large_table_threshold: 100000
```

### 2. Set Environment Variables

Set credentials as environment variables:

```bash
export ORACLE_PASSWORD='your_secure_password'
export POSTGRES_PASSWORD='your_secure_password'
```

For persistent configuration, add to `~/.bashrc` or use a secrets management system.

### 3. Kafka Configuration

Create `/opt/pipeline/config/kafka_clusters.yaml`:

```yaml
kafka_clusters:
  primary:
    bootstrap_servers: kafka1:9092,kafka2:9092,kafka3:9092
    consumer_group_prefix: etl-cdc
    auto_offset_reset: earliest
```

## Usage

### Phase 1: Initial Bulk Load

Extract all tables from source database and load into Iceberg:

```bash
cd /opt/pipeline

# Basic usage
python jobs/direct_bulk_load.py --source my_oracle_db

# Filter specific tables (SQL LIKE pattern)
python jobs/direct_bulk_load.py --source my_oracle_db --table-filter "CUSTOMER_%"

# Tune parallelism
python jobs/direct_bulk_load.py \
  --source my_oracle_db \
  --parallel-tables 16 \
  --parallel-workers 8

# Force full reload (ignore checkpoint)
python jobs/direct_bulk_load.py --source my_oracle_db --no-resume

# Fresh start - drop all tables and reload (requires confirmation)
python jobs/direct_bulk_load.py --source my_oracle_db --fresh-start

# Fresh start with auto-confirmation (for automation)
python jobs/direct_bulk_load.py --source my_oracle_db --fresh-start --yes
```

**Output**:
- Data written to: `local.{iceberg_namespace}.{table_name}`
- Status tracking: `local.{iceberg_namespace}._cdc_status`
- Debezium config: `/opt/pipeline/config/debezium_{source}_cdc.json`
- Logs: `/opt/pipeline/logs/direct_load.log`

### Phase 2: Configure Debezium

The bulk load automatically generates a Debezium connector configuration. Deploy it to your Kafka Connect cluster:

```bash
# Review generated config
cat /opt/pipeline/config/debezium_my_oracle_db_cdc.json

# Deploy to Kafka Connect
curl -X POST http://kafka-connect:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/opt/pipeline/config/debezium_my_oracle_db_cdc.json
```

### Phase 3: Start CDC Consumer

Stream changes from Kafka to Iceberg:

```bash
cd /opt/pipeline

# Basic usage
python jobs/cdc_kafka_to_iceberg.py --source my_oracle_db

# Higher throughput
python jobs/cdc_kafka_to_iceberg.py --source my_oracle_db --batch-size 5000

# Using spark-submit for production
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 8g \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=file:///opt/data/lakehouse/warehouse \
  jobs/cdc_kafka_to_iceberg.py --source my_oracle_db
```

Press `Ctrl+C` to gracefully stop the consumer.

### Validation

Validate that data was loaded correctly:

```bash
# List all tables with row counts
python jobs/validate_iceberg_tables.py --source my_oracle_db

# Hide empty tables
python jobs/validate_iceberg_tables.py --source my_oracle_db --hide-empty

# Skip checking for missing tables
python jobs/validate_iceberg_tables.py --source my_oracle_db --no-check-missing
```

## Querying Data

Query loaded data using Spark SQL:

```bash
spark-sql

-- List tables
SHOW TABLES IN local.bronze.myapp;

-- Query data
SELECT * FROM local.bronze.myapp.customers LIMIT 10;

-- Check status
SELECT table_name, load_status, record_count, initial_load_end
FROM local.bronze.myapp._cdc_status
WHERE load_status = 'completed'
ORDER BY initial_load_end DESC;

-- Time travel (Iceberg feature)
SELECT * FROM local.bronze.myapp.customers
TIMESTAMP AS OF '2025-01-01 12:00:00';
```

## Monitoring

### Check Bulk Load Progress

```bash
# Tail logs
tail -f /opt/pipeline/logs/direct_load.log

# Query status table
spark-sql -e "
  SELECT
    load_status,
    COUNT(*) as count,
    SUM(record_count) as total_records
  FROM local.bronze.myapp._cdc_status
  GROUP BY load_status
"

# List failed tables
spark-sql -e "
  SELECT table_name, error_message
  FROM local.bronze.myapp._cdc_status
  WHERE load_status = 'failed'
"
```

### Check CDC Consumer Health

```bash
# Tail logs
tail -f /opt/pipeline/logs/cdc_consumer.log

# Check Kafka consumer lag
kafka-consumer-groups.sh --bootstrap-server kafka1:9092 \
  --group etl-cdc-my_oracle_db --describe

# Query CDC positions
spark-sql -e "
  SELECT
    table_name,
    oracle_scn,
    last_processed_timestamp
  FROM local.bronze.myapp._cdc_status
  WHERE last_processed_timestamp IS NOT NULL
  ORDER BY last_processed_timestamp DESC
"
```

## Troubleshooting

### Bulk Load Issues

**Problem**: Table extraction fails with "ORA-01555: snapshot too old"
```bash
# Solution: Reduce parallel_tables to lower memory pressure
python jobs/direct_bulk_load.py --source my_oracle_db --parallel-tables 4
```

**Problem**: LONG column error or warning
```bash
# Solution: Configure handle_long_columns in sources.yaml
# Options:
#   exclude (default) - Skip LONG columns, load rest of table
#   skip_table - Skip entire table if LONG columns exist
#   error - Fail the job if LONG columns found
```

**Oracle LONG Column Handling:**

Oracle LONG columns **cannot be read via Spark JDBC** due to fundamental Oracle restrictions (subquery limitations).

**Recommended approach**: Use `exclude` mode (default)
- LONG columns are automatically excluded from bulk load
- Rest of table loads with full point-in-time consistency
- CDC (Debezium) captures LONG column changes going forward

For detailed explanation, alternatives, and workarounds, see [docs/ORACLE_LONG_LIMITATIONS.md](docs/ORACLE_LONG_LIMITATIONS.md)

**Problem**: Out of memory errors
```bash
# Solution: Adjust large_table_threshold or increase Spark memory
# Lower threshold = more disk persistence
```

**Problem**: Need to completely restart bulk load (corrupted data, schema changes, etc.)
```bash
# WARNING: This will DROP ALL TABLES in the namespace and clear checkpoints

# Interactive mode (requires typing 'YES' to confirm)
python jobs/direct_bulk_load.py --source my_oracle_db --fresh-start

# Automated mode (auto-confirms - use with caution)
python jobs/direct_bulk_load.py --source my_oracle_db --fresh-start --yes
```

### CDC Consumer Issues

**Problem**: Consumer not processing events
```bash
# Check Debezium connector status
curl http://kafka-connect:8083/connectors/my-cdc-connector/status

# Verify Kafka topics exist
kafka-topics.sh --bootstrap-server kafka1:9092 --list | grep myapp

# Check consumer is subscribed
# Look for "Subscribing to Kafka topics: ..." in logs
```

**Problem**: Duplicate CDC events
```bash
# Check if SCN/LSN filtering is working
grep "Will filter events before" /opt/pipeline/logs/cdc_consumer.log

# Verify status table has correct positions
spark-sql -e "SELECT * FROM local.bronze.myapp._cdc_status"
```

**Problem**: Checkpoint corruption
```bash
# Delete checkpoint and restart (will replay from earliest)
rm -rf /opt/pipeline/checkpoints/bronze.myapp/
python jobs/cdc_kafka_to_iceberg.py --source my_oracle_db
```

## Configuration Reference

### Bulk Load Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `parallel_tables` | JDBC partitions per table | 8 |
| `parallel_workers` | Tables processed simultaneously | 4 |
| `batch_size` | JDBC fetch size | 50000 |
| `skip_empty_tables` | Skip tables with 0 rows | true |
| `handle_long_columns` | Oracle LONG handling (exclude/skip_table/error) | exclude |
| `checkpoint_enabled` | Resume capability | true |
| `max_retries` | Retry attempts per table | 3 |
| `retry_backoff` | Exponential backoff multiplier | 2 |
| `large_table_threshold` | Rows triggering disk persistence | 100000 |

### CDC Consumer Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--batch-size` | Kafka messages per micro-batch | 1000 |
| Trigger interval | Micro-batch processing interval | 10s |

## Directory Structure

```
etl/
├── README.md                 # This file
├── TODO.md                   # Planned improvements
├── docs/
│   └── ARCHITECTURE.md       # Detailed architecture docs
├── config/
│   ├── sources.yaml          # Database configurations
│   ├── kafka_clusters.yaml   # Kafka configurations
│   └── *.template            # Configuration templates
├── lib/                      # Python library modules
│   ├── config_loader.py
│   ├── status_tracker.py
│   ├── extractors/
│   └── schema_fixers/
├── jobs/                     # Main entry point scripts
│   ├── direct_bulk_load.py
│   ├── cdc_kafka_to_iceberg.py
│   └── validate_iceberg_tables.py
├── checkpoints/              # Spark streaming checkpoints
└── logs/                     # Application logs
```

## Security Considerations

### Credentials

- **Never commit** `sources.yaml` or `kafka_clusters.yaml` with real credentials to version control
- Use environment variables: `password: ${ENV_VAR_NAME}`
- Consider integrating with:
  - AWS Secrets Manager
  - HashiCorp Vault
  - Kubernetes Secrets

### Network Security

- Ensure firewall rules allow:
  - JDBC access to source databases
  - Kafka access (typically port 9092)
  - Iceberg storage access (S3/HDFS/local filesystem)

### Data Access

- Grant minimal required permissions:
  - **Oracle**: SELECT on source tables, SELECT on v$database
  - **Postgres**: SELECT on source tables, SELECT on replication catalog
- Iceberg access control via catalog permissions

## Performance Tuning

### Bulk Load Optimization

```bash
# For small tables (< 1M rows)
--parallel-tables 4 --parallel-workers 8

# For large tables (> 10M rows)
--parallel-tables 16 --parallel-workers 2

# For mixed workload
--parallel-tables 8 --parallel-workers 4
```

### Spark Memory Tuning

```bash
spark-submit \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --conf spark.driver.maxResultSize=4g \
  --conf spark.network.timeout=800s \
  ...
```

### CDC Throughput Optimization

- Increase `--batch-size` for higher throughput (risk: larger memory usage)
- Decrease trigger interval for lower latency (risk: more overhead)
- Use Kafka partitioning to distribute load

## Contributing

See [TODO.md](TODO.md) for planned improvements and contribution opportunities.

## License

[Specify your license]

## Support

For issues, questions, or contributions:
- Create an issue in the repository
- Contact: [Your contact information]

## Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
