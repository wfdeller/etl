# ETL Pipeline Architecture

## Overview

This is a production-grade ETL pipeline that extracts data from transactional databases (Oracle, PostgreSQL) and loads it into an Apache Iceberg data lakehouse. The system implements a two-phase pattern:

1. **Phase 1: Initial Bulk Load** - Full snapshot of all tables
2. **Phase 2: Change Data Capture (CDC)** - Continuous streaming of changes via Kafka/Debezium

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         SOURCE SYSTEMS                           │
│  ┌──────────────────┐              ┌──────────────────┐         │
│  │  Oracle Database │              │ PostgreSQL DB    │         │
│  │  (any schema)    │              │  (Aurora, etc)   │         │
│  └────────┬─────────┘              └────────┬─────────┘         │
│           │                                 │                    │
└───────────┼─────────────────────────────────┼────────────────────┘
            │                                 │
            │ JDBC                            │ JDBC
            │                                 │
┌───────────▼─────────────────────────────────▼────────────────────┐
│                    PHASE 1: BULK LOAD                             │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  direct_bulk_load.py                                       │  │
│  │  • Captures SCN/LSN before load                            │  │
│  │  • Extracts all tables in parallel                         │  │
│  │  • Handles Oracle LONG columns                             │  │
│  │  • Checkpoint/resume capability                            │  │
│  │  • Generates Debezium config with starting position        │  │
│  └────────────────────────────────────────────────────────────┘  │
│           │                                                       │
│           │ Writes to                                             │
│           ▼                                                       │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │         Apache Iceberg Lakehouse (Bronze Layer)            │  │
│  │                                                             │  │
│  │  Catalog: Configurable (local, Unity Catalog)              │  │
│  │  Format: Parquet (default)                                  │  │
│  │  Location: Configurable via $WAREHOUSE_PATH                 │  │
│  │           (file://, s3://, dbfs://)                        │  │
│  │                                                             │  │
│  │  Namespaces:                                                │  │
│  │    • {catalog}.bronze.mydb (Oracle tables)               │  │
│  │    • {catalog}.bronze.aurora_db1 (Postgres tables)         │  │
│  │    • {catalog}.bronze.*._cdc_status (Status tracking)      │  │
│  └────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────┐
│                    CHANGE DATA CAPTURE FLOW                        │
│                                                                    │
│  ┌──────────────┐                                                 │
│  │  Debezium    │  Reads from database transaction log           │
│  │  Connectors  │  (Oracle: LogMiner, Postgres: pgoutput)        │
│  └──────┬───────┘                                                 │
│         │ CDC Events (JSON)                                       │
│         ▼                                                          │
│  ┌──────────────────────────────────────┐                        │
│  │      Apache Kafka                    │                        │
│  │  Topics: {prefix}.{schema}.{table}   │                        │
│  │    e.g., dev.mydb.MYSCHEMA.CUSTOMERS │                        │
│  └──────┬───────────────────────────────┘                        │
│         │                                                          │
│         │ Spark Structured Streaming                              │
│         ▼                                                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  cdc_kafka_to_iceberg.py                                    │ │
│  │  • Subscribes to topic pattern                              │ │
│  │  • Filters events based on SCN/LSN from bulk load          │ │
│  │  • Applies CDC operations (INSERT/UPDATE/DELETE)           │ │
│  │  • Auto-creates new tables from CDC events                 │ │
│  │  • Updates position tracking                               │ │
│  └─────────────────────────────────────────────────────────────┘ │
│         │                                                          │
│         │ MERGE/DELETE operations                                 │
│         ▼                                                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │         Apache Iceberg Lakehouse (Bronze Layer)             │ │
│  │  • Upserts maintain latest state                            │ │
│  │  • Deletes remove records                                   │ │
│  │  • Time travel via Iceberg snapshots                        │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────┘
```

## Component Architecture

### Core Components

```
etl/
├── config/                   # Configuration files
│   ├── sources.yaml          # Database source definitions
│   ├── kafka_clusters.yaml   # Kafka connection config
│   └── debezium_*.json       # Generated Debezium configs
│
├── lib/                      # Reusable library modules
│   ├── config_loader.py      # YAML config with env var substitution
│   ├── status_tracker.py     # CDC status tracking in Iceberg
│   │
│   ├── extractors/           # Database extraction
│   │   ├── base_extractor.py
│   │   ├── oracle_extractor.py
│   │   └── postgres_extractor.py
│   │
│   └── schema_fixers/        # Database-specific schema handling
│       ├── oracle_schema_fixer.py
│       └── postgres_schema_fixer.py
│
├── jobs/                     # Main entry points
│   ├── direct_bulk_load.py
│   ├── cdc_kafka_to_iceberg.py
│   └── validate_iceberg_tables.py
│
├── checkpoints/              # Spark streaming checkpoints
├── logs/                     # Application logs
└── docs/                     # Documentation
```

### Schema Change Tracking

**Purpose**: Notify transformation developers when source database schemas evolve (columns added/removed/type changed)

**Problem**: Current implementation silently merges schema changes during CDC writes. Transformation developers building silver/gold layers have no visibility when bronze table schemas change, leading to broken pipelines.

**Architecture**:

**Audit Table**: `{catalog}.{namespace}._schema_changes`
- Tracks: table_name, change_timestamp, change_type, schema_before, schema_after, column_name
- Created automatically by SchemaTracker on first use
- Queryable via Spark SQL for reporting

**Detection Points**:
1. **Bulk Load** (jobs/direct_bulk_load.py:179):
   - New table: Records baseline schema (version 1), no notification
   - Existing table: Compares schemas, notifies if different (re-load scenario)

2. **CDC Processing** (jobs/cdc_kafka_to_iceberg.py:165):
   - Before mergeSchema write: Compares incoming vs existing schema
   - Always notifies on changes (primary detection point)

**Notification Flow**:
- SchemaTracker detects change → Records in _schema_changes table
- SchemaChangeNotifier sends alerts via configured channels (email/Slack/SNS)
- Updates notification_sent flag and timestamp

**Unity Catalog Integration**:
- Stores schema version in table properties (ALTER TABLE SET TBLPROPERTIES)
- Correlates changes with Iceberg snapshot IDs
- Uses information_schema for validation

**Query Example**:
```sql
SELECT table_name, change_type, change_timestamp, column_name
FROM local.bronze.mydb._schema_changes
WHERE change_timestamp > current_date() - INTERVAL 30 DAYS
ORDER BY change_timestamp DESC;
```

## Data Flow

### Phase 1: Bulk Load

```
1. Capture Position
   ├─→ Oracle: Query v$database.current_scn
   └─→ Postgres: Query pg_current_wal_lsn()

2. Discover Tables
   ├─→ Oracle: Query all_tables
   └─→ Postgres: Query information_schema.tables

3. Extract Tables (Parallel)
   ├─→ JDBC read with partitioning
   ├─→ Handle Oracle LONG columns (exclude/skip/error)
   ├─→ Apply schema fixes (NUMBER → Double)
   └─→ Persist large tables to disk

4. Write to Iceberg
   ├─→ Create table if not exists
   ├─→ Append data
   └─→ Record status in _cdc_status table

5. Generate Debezium Config
   └─→ Set starting SCN/LSN from step 1
```

### Phase 2: CDC Streaming

```
1. Initialize
   ├─→ Read starting position from _cdc_status
   ├─→ Build per-table SCN/LSN map
   └─→ Subscribe to Kafka topic pattern

2. Process Micro-batch
   ├─→ Parse Debezium JSON events
   ├─→ Extract operation type (c/u/d/r)
   ├─→ Filter events before starting SCN/LSN
   └─→ Group by table

3. Apply Changes
   ├─→ DELETE: Remove records by PK
   ├─→ INSERT: Add new records
   ├─→ UPDATE: MERGE with existing data
   └─→ Auto-create tables for new CDC events

4. Update Position
   ├─→ Record latest SCN/LSN per table
   └─→ Checkpoint Kafka offsets
```

## Key Design Decisions

### 1. Two-Phase Pattern

**Rationale**: Separate bulk load and CDC allows:
- Fast initial load without CDC overhead
- Consistent snapshot via SCN/LSN
- CDC starts from known point (no data loss)
- Independent scaling of batch and streaming

**Alternative Considered**: Debezium snapshot mode
- **Rejected**: Slower, locks tables, limited parallelism

### 2. Apache Iceberg as Target

**Rationale**:
- ACID transactions
- Schema evolution
- Time travel (snapshots)
- Efficient MERGE/DELETE operations
- Open format (not vendor locked)

**Alternative Considered**: Delta Lake
- **Rejected**: AWS Glue compatibility, OSS Iceberg momentum

### 3. Status Tracking in Iceberg

**Rationale**:
- Same storage as data (consistency)
- ACID guarantees for status updates
- Query status with Spark SQL
- No external dependency

**Alternative Considered**: External database (PostgreSQL)
- **Rejected**: Additional infrastructure, consistency challenges

### 4. Parallel Table Processing

**Rationale**:
- Dramatically reduces bulk load time
- Configurable parallelism per use case
- Table-level granularity for retry

**Configuration**:
- `parallel_tables`: Partitions within a table (JDBC partitioning)
- `parallel_workers`: Number of tables processed simultaneously

### 5. Oracle LONG Column Handling

**Rationale**:
- LONG type can't be partitioned in JDBC
- Often contains unstructured data
- Rarely queried in analytics

**Options**:
- `exclude`: Read table without LONG columns (default)
- `skip_table`: Skip entire table
- `error`: Fail the load

### 6. Environment Variable Credentials

**Rationale**:
- Avoid plaintext passwords in config files
- Container-friendly (Kubernetes secrets)
- CI/CD pipeline compatible
- Easy rotation

**Syntax**: `${ENV_VAR_NAME}` or `${ENV_VAR_NAME:default}`

## Scalability & Performance

### Horizontal Scaling

**Bulk Load**:
- Run multiple instances with different `--table-filter` patterns
- Example: Worker 1 processes `S_%`, Worker 2 processes `A_%`
- Status tracking prevents duplicate work

**CDC Streaming**:
- Kafka consumer groups enable parallel processing
- Multiple consumers read from different partitions
- Iceberg handles concurrent writes

### Performance Tuning

**Bulk Load Parameters**:
```yaml
parallel_tables: 8           # JDBC partitions per table
parallel_workers: 4          # Concurrent table extractions
batch_size: 50000            # JDBC fetch size
large_table_threshold: 100000  # Persist to disk threshold
```

**CDC Parameters**:
```python
--batch-size 1000           # Kafka messages per micro-batch
trigger: 10 seconds         # Micro-batch interval
```

**Spark Configuration**:
```python
spark.sql.adaptive.enabled: true
spark.driver.maxResultSize: 2g
spark.network.timeout: 600s
```

## Fault Tolerance

### Bulk Load

**Checkpoint/Resume**:
- Tracks completed tables in `_cdc_status`
- Restart picks up where it left off
- `--no-resume` flag forces full reload

**Retry Logic**:
- Per-table retry with exponential backoff
- Configurable: `max_retries`, `retry_backoff`
- Failed tables logged for manual intervention

### CDC Streaming

**Kafka Offset Management**:
- Spark checkpoints maintain Kafka offsets
- Crash recovery restarts from last checkpoint
- Exactly-once not guaranteed (at-least-once)

**SCN/LSN Filtering**:
- Prevents duplicate processing of bulk-loaded data
- Per-table position tracking
- New tables auto-created with CDC position recorded

## Data Consistency

### Bulk Load Consistency

**Oracle**:
- Captures SCN before extraction
- All table queries use `AS OF SCN {scn}`
- Guarantees consistent point-in-time snapshot

**Postgres**:
- Captures LSN before extraction
- Debezium starts from captured LSN
- Snapshot consistency via transaction isolation

### CDC Consistency

**Ordering**:
- Kafka partitioning ensures per-table ordering
- Within-table operations are sequential
- Cross-table ordering not guaranteed

**Deduplication**:
- Latest event wins (by timestamp)
- Primary key-based MERGE operations

## Security

### Credentials Management

**Current**:
- Environment variables via `${VAR}` syntax
- Supports default values: `${VAR:default}`

**Future Considerations**:
- AWS Secrets Manager integration
- HashiCorp Vault integration
- Kubernetes secrets mounting

### Network Security

**Requirements**:
- JDBC access to source databases
- Kafka access (typically port 9092)
- Iceberg storage access (S3/HDFS/local)

### Data Access Control

**Iceberg**:
- Catalog-level access control
- Namespace-level permissions
- Integration with Ranger/Sentry (future)

## Monitoring & Observability

### Logging

**Current Implementation**:
- File-based: `logs/*.log` (configurable via $LOG_DIR)
- Console output (stdout/stderr)
- Structured format with thread names
- Supports local filesystem, DBFS, and S3 storage

**Log Levels**:
- INFO: Normal operations, progress
- WARNING: Retries, skipped tables
- ERROR: Failures, exceptions

### Metrics (Future)

**Key Metrics to Track**:
- Tables processed per hour
- Records loaded per second
- Failed table count
- CDC lag (Kafka consumer lag)
- SCN/LSN progression rate
- Table processing duration

**Monitoring Integrations**:
- Prometheus metrics endpoint
- CloudWatch logs/metrics
- Datadog APM

### Alerting (Future)

**Alert Conditions**:
- Bulk load job failure
- CDC consumer stopped
- Kafka consumer lag > threshold
- Failed table count > 0
- Disk space low
- SCN/LSN not progressing

## Operational Procedures

### Running Bulk Load

```bash
# Full load
python jobs/direct_bulk_load.py --source dev_mydb

# Filtered load
python jobs/direct_bulk_load.py --source dev_mydb --table-filter "S_%"

# Performance tuning
python jobs/direct_bulk_load.py \
  --source dev_mydb \
  --parallel-tables 16 \
  --parallel-workers 8

# Force reload (no checkpoint)
python jobs/direct_bulk_load.py --source dev_mydb --no-resume
```

### Running CDC Consumer

```bash
# Standard run
python jobs/cdc_kafka_to_iceberg.py --source dev_mydb

# Higher throughput
python jobs/cdc_kafka_to_iceberg.py --source dev_mydb --batch-size 5000

# Using Spark submit
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 8g \
  jobs/cdc_kafka_to_iceberg.py --source dev_mydb
```

### Validating Data

```bash
# List all tables with row counts
python jobs/validate_iceberg_tables.py --source dev_mydb

# Hide empty tables
python jobs/validate_iceberg_tables.py --source dev_mydb --hide-empty

# Check for missing tables
python jobs/validate_iceberg_tables.py --source dev_mydb --no-check-missing
```

## Disaster Recovery

### Backup Strategy

**What to Backup**:
1. Iceberg metadata (critical)
2. Configuration files
3. Checkpoint directories
4. Status tables (`_cdc_status`)

**Backup Frequency**:
- Iceberg: Snapshots are immutable (no backup needed)
- Checkpoints: Daily
- Config: Version controlled in Git

### Recovery Procedures

**Scenario 1: Bulk Load Failure**
```bash
# Review logs
tail -f logs/direct_load.log

# Check status table
spark-sql -e "SELECT * FROM {catalog}.bronze.mydb._cdc_status WHERE load_status = 'failed'"

# Resume from checkpoint
python jobs/direct_bulk_load.py --source dev_mydb
```

**Scenario 2: CDC Consumer Crash**
```bash
# Check checkpoint exists (path configured via $CHECKPOINT_PATH)
ls checkpoints/bronze.mydb/
# Or for cloud: aws s3 ls s3://bucket/checkpoints/bronze.mydb/

# Restart consumer (auto-resumes)
python jobs/cdc_kafka_to_iceberg.py --source dev_mydb
```

**Scenario 3: Corrupted Status Table**
```bash
# Drop and recreate (resets all state)
spark-sql -e "DROP TABLE local.bronze.mydb._cdc_status"

# Rerun bulk load (will recreate status table)
python jobs/direct_bulk_load.py --source dev_mydb --no-resume
```

**Scenario 4: Lost Checkpoint Directory**
- Consumer will restart from `startingOffsets: earliest`
- SCN/LSN filtering prevents duplicate data (if status intact)
- May reprocess old CDC events (no data corruption)

## Future Enhancements

See [TODO.md](../TODO.md) for complete list of planned improvements.

**High Priority**:
1. Explicit primary key configuration
2. Idempotency for bulk loads
3. Schema change tracking and notification (see Schema Change Tracking section above)
4. Metrics and monitoring
5. Integration tests

**Architecture Evolution**:
1. Move to S3/HDFS for Iceberg storage
2. Implement exactly-once CDC semantics
3. Unity Catalog as schema registry (Databricks deployment)
4. Distributed status tracking (beyond single table)
5. Multi-region replication

## References

- [Apache Iceberg](https://iceberg.apache.org/)
- [Debezium](https://debezium.io/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Kafka](https://kafka.apache.org/)
