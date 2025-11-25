# Databricks Deployment Guide

This guide explains how to deploy and run the ETL pipeline on Databricks.

## Table of Contents

1. [Overview](#overview)
2. [Architecture Differences](#architecture-differences)
3. [Prerequisites](#prerequisites)
4. [Configuration Changes](#configuration-changes)
5. [Secrets Management](#secrets-management)
6. [Storage Configuration](#storage-configuration)
7. [Deployment Steps](#deployment-steps)
8. [Running Jobs](#running-jobs)
9. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
10. [Cost Optimization](#cost-optimization)
11. [Testing Locally with MinIO](#testing-locally-with-minio)

## Overview

The ETL pipeline is designed to run seamlessly on both local development environments and Databricks. The core PySpark logic remains unchanged between environments - the main differences are in deployment method, secrets management, and storage configuration.

### Key Benefits of Databricks Deployment

- **Managed Infrastructure**: No need to manage Spark clusters
- **Unity Catalog**: Centralized metadata and governance
- **Scalability**: Auto-scaling clusters for variable workloads
- **Security**: Built-in secrets management and access controls
- **Collaboration**: Shared notebooks and scheduled jobs
- **Monitoring**: Built-in observability and logging

## Architecture Differences

| Component | Local Development | Databricks |
|-----------|------------------|------------|
| **Spark Session** | Self-managed via SparkSessionFactory | Managed by Databricks Runtime |
| **Catalog** | Local catalog (`local`) | Unity Catalog (`prod_catalog`) |
| **Storage** | Local filesystem | AWS S3 / DBFS |
| **Secrets** | Environment variables | Databricks Secrets (with env var fallback) |
| **Job Scheduling** | Cron / manual | Databricks Jobs / Workflows |
| **Dependencies** | Local virtualenv | Cluster libraries or init scripts |
| **Monitoring** | Local logs | Databricks job logs + metrics |

## Prerequisites

### Databricks Workspace Setup

1. **Unity Catalog**: Ensure Unity Catalog is enabled
2. **Catalog and Schema**: Create target catalog and schemas
   ```sql
   CREATE CATALOG IF NOT EXISTS prod_catalog;
   CREATE SCHEMA IF NOT EXISTS prod_catalog.bronze;
   CREATE SCHEMA IF NOT EXISTS prod_catalog.silver;
   CREATE SCHEMA IF NOT EXISTS prod_catalog.gold;
   ```

3. **Storage Configuration**:
   - Configure S3 bucket with appropriate IAM roles/policies
   - Set up external location in Unity Catalog pointing to your S3 data lake
   - Ensure Databricks has access via instance profiles or access keys

4. **Network Access**: Ensure Databricks can reach source databases
   - Configure VPC peering for private connectivity
   - Update database firewall rules to allow Databricks cluster IPs
   - Consider AWS PrivateLink for enhanced security

### Required Databricks Permissions

- Workspace admin (for initial setup)
- Catalog CREATE, USE privileges
- Schema CREATE, USE privileges
- External location access (for cloud storage)
- Secrets management (for creating secret scopes)

## Configuration Changes

### 1. Update `config/sources.yaml`

Change your source configuration to use Databricks-appropriate settings:

```yaml
sources:
  databricks_prod_siebel:
    database_type: oracle
    kafka_topic_prefix: prod.siebel
    # Unity Catalog format: catalog.schema.table_prefix
    iceberg_namespace: prod_catalog.bronze.siebel

    database_connection:
      host: oracle-prod.example.com
      port: 1521
      service_name: PRODDB
      username: etl_service
      password: ${ORACLE_PROD_SIEBEL_PASSWORD}  # From Databricks Secrets
      schema: SIEBEL

    bulk_load:
      parallel_tables: 16        # Scale up for larger clusters
      parallel_workers: 8
      batch_size: 100000         # Larger batches with more memory
      skip_empty_tables: true
      handle_long_columns: exclude
      checkpoint_enabled: true
      checkpoint_interval: 100
      max_retries: 3
      retry_backoff: 2
      large_table_threshold: 1000000  # Higher for cloud storage
```

See `config/sources.yaml.template` for complete examples.

### 2. Environment Variables

Set these as Databricks cluster environment variables or job parameters:

```bash
# Required
CATALOG_NAME=prod_catalog
WAREHOUSE_PATH=s3://my-data-lake/warehouse  # or abfss://container@account.dfs.core.windows.net/warehouse

# Optional (if not using Databricks Secrets)
CONFIG_DIR=/Workspace/Shared/etl/config
ORACLE_PROD_SIEBEL_PASSWORD=<password>
```

**Setting in Databricks UI:**
1. Cluster Configuration → Advanced Options → Environment Variables
2. OR Job Configuration → Parameters → Environment Variables

## Secrets Management

The ETL pipeline automatically detects Databricks and uses the appropriate secrets backend.

### Creating Databricks Secrets

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Create a secret scope (if not exists)
databricks secrets create-scope --scope prod_etl

# Add secrets
databricks secrets put --scope prod_etl --key oracle_prod_siebel_password
databricks secrets put --scope prod_etl --key postgres_salesforce_password
databricks secrets put --scope prod_etl --key aws_access_key
databricks secrets put --scope prod_etl --key aws_secret_key
```

### Secret Reference Format

In `sources.yaml`:
```yaml
password: ${ORACLE_PROD_SIEBEL_PASSWORD}
```

The `SecretsManager` class will:
1. Try to fetch from Databricks Secrets (scope: `etl`, key: `oracle_prod_siebel_password`)
2. Fall back to environment variable `ORACLE_PROD_SIEBEL_PASSWORD`
3. Raise error if not found

### Using Custom Secret Scopes

To use a different secret scope, pass it when calling secrets:
```python
from secrets_utils import SecretsManager
password = SecretsManager.get_secret('oracle_password', scope='prod_etl')
```

## Storage Configuration

### AWS S3

#### Option 1: Instance Profiles (Recommended)

Attach IAM instance profile to Databricks cluster:

1. Create IAM role with S3 access:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::my-data-lake",
        "arn:aws:s3:::my-data-lake/*"
      ]
    }
  ]
}
```

2. Attach role to cluster in Databricks
3. Set environment variables:
```bash
WAREHOUSE_PATH=s3://my-data-lake/warehouse
```

#### Option 2: Access Keys (Less Secure)

Store in Databricks Secrets and configure in cluster:
```bash
WAREHOUSE_PATH=s3://my-data-lake/warehouse
AWS_ACCESS_KEY=${AWS_ACCESS_KEY}
AWS_SECRET_KEY=${AWS_SECRET_KEY}
```

### Unity Catalog External Locations

Best practice: Use Unity Catalog to manage storage credentials

```sql
-- Create external location
CREATE EXTERNAL LOCATION etl_warehouse
URL 's3://my-data-lake/warehouse'
WITH (STORAGE CREDENTIAL my_s3_credential);

-- Grant access
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION etl_warehouse TO `etl_users`;
```

Then set:
```bash
WAREHOUSE_PATH=s3://my-data-lake/warehouse
```

## Deployment Steps

### Method 1: Databricks Repos (Recommended)

1. **Connect Git Repository**:
   - Go to Repos → Add Repo
   - Connect to your Git repository
   - Select branch to deploy

2. **Upload Configuration**:
   ```bash
   # Copy your sources.yaml to Databricks workspace
   databricks workspace import config/sources.yaml /Workspace/Shared/etl/config/sources.yaml
   ```

3. **Install Dependencies**:
   - Create cluster with required libraries
   - Cluster Configuration → Libraries → Install New
   - PyPI: `pyyaml`
   - Maven:
     - `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.0`
     - `org.apache.hadoop:hadoop-aws:3.3.4` (for S3)

4. **Set Environment Variables**:
   - Cluster → Advanced Options → Environment Variables
   - Add: `CATALOG_NAME=prod_catalog`
   - Add: `WAREHOUSE_PATH=s3://your-bucket/warehouse`
   - Add: `CONFIG_DIR=/Workspace/Shared/etl/config`

### Method 2: JAR Upload

1. Package the ETL code:
   ```bash
   zip -r etl-pipeline.zip jobs/ lib/ config/
   ```

2. Upload to Databricks workspace:
   ```bash
   databricks workspace import etl-pipeline.zip /Workspace/Shared/etl/etl-pipeline.zip
   ```

3. Configure job to use uploaded code

### Method 3: Notebooks

Convert Python scripts to notebooks:
1. Add `# Databricks notebook source` at top of file
2. Upload to workspace
3. Create Databricks Job with notebook task

## Running Jobs

### Interactive Execution

Run directly from Databricks notebook or Repos:
```python
%run /Workspace/Repos/your-repo/jobs/direct_bulk_load.py --source databricks_prod_siebel
```

### Scheduled Jobs

1. **Create Job**:
   - Jobs → Create Job
   - Task Type: Python script or Notebook
   - Source: Workspace file or Git repository

2. **Configure Task**:
   ```
   Type: Python file
   Source: Git repository
   Repository: your-repo
   Branch: main
   Python file: jobs/direct_bulk_load.py
   Parameters: ["--source", "databricks_prod_siebel"]
   ```

3. **Cluster Configuration**:
   - New cluster or existing cluster
   - Recommended: New job cluster (auto-terminates)
   - Instance type: Memory-optimized for large data
   - Workers: Start with 2-4, scale based on data volume

4. **Schedule**:
   - Trigger: Scheduled
   - Cron expression: `0 2 * * *` (daily at 2 AM)

### CDC Consumer (Long-Running)

For the CDC Kafka consumer, use an always-on cluster:

1. Create dedicated streaming cluster
2. Configure job with:
   ```
   Type: Python file
   Python file: jobs/cdc_kafka_to_iceberg.py
   Parameters: ["--source", "databricks_prod_siebel", "--batch-size", "1000"]
   Cluster: existing-streaming-cluster
   ```
3. Set max concurrent runs: 1
4. Enable automatic restart on failure

## Monitoring and Troubleshooting

### Job Monitoring

1. **Databricks UI**:
   - Jobs → Your Job → Runs
   - View logs, metrics, and Spark UI
   - Monitor resource utilization

2. **Logs Location**:
   - Driver logs: `/databricks/driver/logs/`
   - Application logs: Check job run output
   - Custom logs: Use `logging` module (written to driver logs)

3. **Spark UI**:
   - Access from job run details
   - Monitor stage execution
   - Identify bottlenecks

### Common Issues

#### Issue: "Catalog not found"

**Solution**: Ensure Unity Catalog name is correct and you have USE CATALOG permission
```python
spark.sql("USE CATALOG prod_catalog")
spark.sql("SHOW SCHEMAS")
```

#### Issue: "Access denied" to S3

**Solution**:
- Verify IAM role/instance profile has correct S3 permissions
- Check Unity Catalog external location configuration
- Validate storage credentials in Databricks Secrets (if using access keys)
- Ensure bucket policy allows Databricks account access

#### Issue: "Secret not found"

**Solution**:
- Verify secret scope exists: `databricks secrets list-scopes`
- Verify secret key exists: `databricks secrets list --scope prod_etl`
- Check fallback environment variables are set

#### Issue: JDBC connection timeout

**Solution**:
- Verify network connectivity (VPC peering, firewall rules)
- Test with `telnet <host> <port>` from cluster
- Check database firewall allows Databricks IP ranges

### Debugging Tips

1. **Test in Notebook First**:
   - Run code interactively before scheduling
   - Use `display()` to inspect DataFrames
   - Validate configurations

2. **Enable Debug Logging**:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

3. **Small Dataset Testing**:
   - Use `--table-filter` to test single table first
   - Validate end-to-end before full migration

## Cost Optimization

### Cluster Configuration

1. **Use Job Clusters**: Auto-terminate after job completion
2. **Right-size Workers**: Start small, scale up based on metrics
3. **Spot Instances**: Use for non-critical jobs (70-80% cost savings)
4. **Instance Types**:
   - Memory-optimized (r5/r6i) for large aggregations
   - Compute-optimized (c5/c6i) for pure transformations

### Job Optimization

1. **Parallel Processing**:
   ```yaml
   parallel_tables: 16      # Scale based on cluster size
   parallel_workers: 8      # Concurrent table processing
   ```

2. **Partition Data**:
   - Partition Iceberg tables by date/region
   - Enables partition pruning for faster queries

3. **Checkpoint Management**:
   ```yaml
   checkpoint_enabled: true
   checkpoint_interval: 100  # Checkpoint every N tables
   ```
   - Allows resume on failure without reprocessing

4. **Batch Sizing**:
   ```yaml
   batch_size: 100000  # Larger batches = fewer round trips
   ```

### Storage Optimization

1. **Table Maintenance**:
   ```sql
   -- Compact small files
   OPTIMIZE prod_catalog.bronze.siebel.large_table;

   -- Remove old snapshots
   CALL prod_catalog.system.expire_snapshots('bronze.siebel.large_table', TIMESTAMP '2024-01-01 00:00:00');
   ```

2. **Lifecycle Policies**:
   - Move old data to cheaper storage tiers
   - Use S3 Intelligent-Tiering for automatic optimization
   - Archive cold data to S3 Glacier or Glacier Deep Archive

## Testing Locally with MinIO

Before deploying to Databricks, test S3 functionality locally with MinIO.

### 1. Setup MinIO

```bash
# Docker Compose
docker run -d \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

### 2. Create Bucket

```bash
# Install MinIO client
brew install minio/stable/mc

# Configure
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create bucket
mc mb local/iceberg-warehouse
```

### 3. Update Configuration

```yaml
# config/sources.yaml
sources:
  local_siebel_minio:
    database_type: oracle
    iceberg_namespace: local.bronze.siebel

    database_connection:
      host: oracle.localdomain
      port: 1521
      service_name: DEVLPDB1
      username: siebel
      password: ${ORACLE_DEV_SIEBEL_PASSWORD}
      schema: SIEBEL

    s3_storage:
      endpoint: http://localhost:9000
      access_key: minioadmin
      secret_key: minioadmin
      path_style_access: true  # Required for MinIO
```

### 4. Set Environment Variables

```bash
export CATALOG_NAME=local
export WAREHOUSE_PATH=s3a://iceberg-warehouse/dev
export ORACLE_DEV_SIEBEL_PASSWORD=your_password
```

### 5. Run ETL Job

```bash
python jobs/direct_bulk_load.py --source local_siebel_minio --table-filter "SMALL_TABLE"
```

### 6. Verify Data in MinIO

```bash
# List files
mc ls local/iceberg-warehouse/dev/

# Download a file to inspect
mc cp local/iceberg-warehouse/dev/bronze/siebel/metadata/snap-*.avro ./
```

This validates:
- S3A filesystem configuration
- Iceberg table creation on object storage
- Data writing and reading from S3-compatible storage

Once validated, the same code will work on Databricks with real S3/ADLS.

## Next Steps

1. Review `config/sources.yaml.template` for example configurations
2. Set up Unity Catalog and external locations
3. Configure Databricks Secrets for credentials
4. Test with MinIO locally (optional)
5. Deploy code to Databricks workspace
6. Create and schedule Databricks Jobs
7. Monitor initial runs and optimize based on metrics

## Additional Resources

- [Databricks Unity Catalog Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Apache Iceberg on Databricks](https://docs.databricks.com/en/delta/uniform.html)
- [Databricks Secrets Management](https://docs.databricks.com/en/security/secrets/index.html)
- [Databricks Jobs and Workflows](https://docs.databricks.com/en/workflows/index.html)
