# Local Setup Guide

This guide explains how to run the ETL pipeline locally for testing the direct bulk load capability.

## Required Environment Variables

The pipeline requires the following environment variables to be set:

```bash
# Configuration directory
export CONFIG_DIR=/Users/wfdeller/Development/etl/config

# Iceberg catalog configuration
export CATALOG_NAME=local
export WAREHOUSE_PATH=file:///Users/wfdeller/Development/etl/warehouse

# Oracle password (for dev_siebel source)
export ORACLE_DEV_SIEBEL_PASSWORD=your_password_here
```

## Prerequisites

### 1. Python Dependencies

```bash
pip install pyspark==3.5.0 pyyaml
```

### 2. Oracle JDBC Driver

Download the Oracle JDBC driver and place it where Spark can access it:

```bash
# Create directory for JDBC drivers
mkdir -p ~/spark-jars

# Download ojdbc8.jar
wget https://download.oracle.com/otn-pub/otn_software/jdbc/ojdbc8.jar \
  -O ~/spark-jars/ojdbc8.jar
```

### 3. Apache Iceberg Runtime

Download the Iceberg Spark runtime JAR:

```bash
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar \
  -O ~/spark-jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar
```

### 4. Update Configuration

Update `config/sources.yaml` to match your environment:

```yaml
sources:
  dev_siebel:
    database_connection:
      host: oracle              # Change from oracle.localdomain to oracle
      port: 1521
      service_name: DEVLPDB1    # Matches your environment
      username: siebel
      password: ${ORACLE_DEV_SIEBEL_PASSWORD}
      schema: SIEBEL
```

## Running the Bulk Load

### Option 1: Direct Python execution

Set environment variables and run directly:

```bash
# Set environment variables
export CONFIG_DIR=/Users/wfdeller/Development/etl/config
export CATALOG_NAME=local
export WAREHOUSE_PATH=file:///Users/wfdeller/Development/etl/warehouse
export ORACLE_DEV_SIEBEL_PASSWORD=your_password

# Run the bulk load
cd /Users/wfdeller/Development/etl
python jobs/direct_bulk_load.py --source dev_siebel
```

### Option 2: Using environment file

Create a file `local_env.sh`:

```bash
#!/bin/bash
export CONFIG_DIR=/Users/wfdeller/Development/etl/config
export CATALOG_NAME=local
export WAREHOUSE_PATH=file:///Users/wfdeller/Development/etl/warehouse
export ORACLE_DEV_SIEBEL_PASSWORD=your_password
```

Source it and run:

```bash
source local_env.sh
python jobs/direct_bulk_load.py --source dev_siebel
```

## Additional Options

### Filter specific tables

```bash
python jobs/direct_bulk_load.py \
  --source dev_siebel \
  --table-filter "S_%"  # Only tables starting with S_
```

### Tune parallelism

```bash
python jobs/direct_bulk_load.py \
  --source dev_siebel \
  --parallel-tables 4 \
  --parallel-workers 2
```

### Fresh start (drop all tables)

```bash
python jobs/direct_bulk_load.py \
  --source dev_siebel \
  --fresh-start
```

## Validation

After the bulk load completes, validate the tables:

```bash
python jobs/validate_iceberg_tables.py --source dev_siebel
```

## Troubleshooting

### Error: CONFIG_DIR environment variable must be set

Make sure you've exported all required environment variables before running the script.

### Error: Cannot connect to Oracle

Check that:
- The Oracle database is accessible from your machine
- The hostname in sources.yaml matches your Oracle server (oracle, not oracle.localdomain)
- The password is correct
- The service name (DEVLPDB1) is correct

### Error: JDBC driver not found

Make sure ojdbc8.jar is in a location Spark can access. You can specify it explicitly:

```bash
export PYSPARK_SUBMIT_ARGS="--jars ~/spark-jars/ojdbc8.jar,~/spark-jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar pyspark-shell"
```

## Databricks Deployment

When deploying to Databricks with Unity Catalog:

```bash
# Set Unity Catalog configuration
export CATALOG_NAME=main  # Or your Unity Catalog name
export WAREHOUSE_PATH=s3://your-bucket/warehouse  # Or abfss:// for Azure

# CONFIG_DIR can point to DBFS or workspace files
export CONFIG_DIR=/Workspace/Shared/etl/config

# Run on Databricks cluster
databricks workspace import jobs/direct_bulk_load.py /Workspace/Shared/etl/jobs/
```

Note: Unity Catalog handles warehouse location internally, but WAREHOUSE_PATH is still required by the script (can be a dummy value).

## Directory Structure After Setup

```
/Users/wfdeller/Development/etl/
├── config/
│   ├── sources.yaml           # Your database configurations
│   └── kafka_clusters.yaml    # Kafka configurations (for CDC)
├── lib/                       # Python libraries (already present)
├── jobs/                      # Job scripts (already present)
├── logs/                      # Application logs (auto-created)
├── checkpoints/               # Spark checkpoints (auto-created)
├── warehouse/                 # Iceberg warehouse (auto-created)
│   └── bronze.siebel/         # Your tables will be here
└── LOCAL_SETUP.md             # This file
```

## Next Steps

1. Configure environment variables
2. Update sources.yaml with your Oracle connection details
3. Run a test bulk load: `python jobs/direct_bulk_load.py --source dev_siebel`
4. Validate tables: `python jobs/validate_iceberg_tables.py --source dev_siebel`
5. Query data using Spark SQL or configure CDC for ongoing changes
