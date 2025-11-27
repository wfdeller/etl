# Monitoring and Data Quality Integration Guide

## Overview

This guide provides step-by-step instructions for integrating the monitoring and data quality libraries into the ETL pipeline jobs.

## Prerequisites

The following libraries have been created and are ready for integration:
- `lib/monitoring.py` - Metrics, structured logging, performance tracking
- `lib/data_quality.py` - Data quality validation checks
- `lib/config_loader.py` - Enhanced with bulk_load and data_quality config validation

## Integration Points

### 1. Direct Bulk Load (`jobs/direct_bulk_load.py`)

#### Step 1.1: Imports (COMPLETED)
```python
from monitoring import MetricsCollector, StructuredLogger
from data_quality import DataQualityChecker
```

#### Step 1.2: Initialize Monitoring in Main Function
**Location**: After line 400 (after creating spark, tracker, schema_tracker)

```python
# Initialize monitoring and data quality
metrics = MetricsCollector('bulk_load', source_name, enable_cloudwatch=True)
struct_logger = StructuredLogger('bulk_load', source_name)
dq_checker = DataQualityChecker(spark)

# Get data quality config
dq_config = source_config.get('data_quality', {})

# Log job start
struct_logger.log_job_start({
    'parallel_workers': parallel_workers,
    'parallel_partitions': parallel_partitions,
    'tables_to_process': len(tables_to_process)
})

job_start_time = time.time()
```

#### Step 1.3: Update process_single_table Signature
**Location**: Line 120

Add parameters:
```python
def process_single_table(table_name: str, extractor, tracker, schema_tracker, iceberg_mgr, namespace: str,
                        jdbc_config: dict, scn_lsn: dict, parallel_partitions: int, max_retries: int,
                        metrics: MetricsCollector = None, struct_logger: StructuredLogger = None,
                        dq_checker: DataQualityChecker = None, dq_config: dict = None):
```

#### Step 1.4: Add Monitoring to process_single_table
**Location**: After line 130 (start of function)

```python
# Track table processing duration
table_start_time = time.time()

# Log table start
if struct_logger:
    struct_logger.log_table_start(table_name, {
        'parallel_partitions': parallel_partitions,
        'scn': scn_lsn.get('oracle_scn'),
        'lsn': scn_lsn.get('postgres_lsn')
    })
```

**Location**: Before successful return (around line 240)

```python
# Calculate duration
table_duration = time.time() - table_start_time

# Emit metrics
if metrics:
    metrics.emit_counter('table_processed', 1, dimensions={'table': table_name})
    metrics.emit_counter('records_processed', record_count, dimensions={'table': table_name})
    metrics.emit_duration('table_load_duration', table_duration, dimensions={'table': table_name})
    metrics.emit_success('table_load', dimensions={'table': table_name})

# Log table complete
if struct_logger:
    struct_logger.log_table_complete(table_name, record_count, table_duration, {
        'scn': scn_lsn.get('oracle_scn'),
        'lsn': scn_lsn.get('postgres_lsn')
    })

# Data quality check (row count validation)
if dq_checker and dq_config.get('check_row_count', False):
    try:
        # Read source count from extractor
        source_count = record_count  # Already know this from extraction

        # Get destination count from Iceberg
        dest_df = spark.table(f"{namespace}.{table_name}")
        dest_count = dest_df.count()

        # Validate
        tolerance = dq_config.get('row_count_tolerance', 0.0)
        passed, details = dq_checker.validate_row_count(
            source_count, dest_count, table_name, tolerance=tolerance
        )

        if not passed:
            logger.warning(f"{table_name}: DATA QUALITY ISSUE - {details}")
            if metrics:
                metrics.emit_counter('dq_row_count_failed', 1, dimensions={'table': table_name})
        else:
            if metrics:
                metrics.emit_counter('dq_row_count_passed', 1, dimensions={'table': table_name})
    except Exception as e:
        logger.error(f"{table_name}: Data quality check failed: {e}")
```

**Location**: In exception handler (around line 260)

```python
# Emit failure metrics
if metrics:
    metrics.emit_failure('table_load', error_type=type(e).__name__,
                        dimensions={'table': table_name})

# Log table failure
if struct_logger:
    struct_logger.log_table_failure(table_name, str(e), {
        'retries_remaining': max_retries - (attempt + 1),
        'error_type': type(e).__name__
    })
```

#### Step 1.5: Update Function Calls
**Location**: All calls to process_single_table (around lines 430, 450)

Add parameters:
```python
process_single_table(..., metrics=metrics, struct_logger=struct_logger,
                    dq_checker=dq_checker, dq_config=dq_config)
```

#### Step 1.6: Add Job Completion Metrics
**Location**: End of main function (after all processing, around line 480)

```python
# Calculate job duration
job_duration = time.time() - job_start_time

# Count completed tables
completed_count = len([result for result in all_results if result[0]])
total_records = sum([result[2] for result in all_results if result[0]])

# Emit job metrics
metrics.emit_counter('job_tables_processed', completed_count)
metrics.emit_counter('job_records_processed', total_records)
metrics.emit_duration('job_duration', job_duration)

# Log job complete
struct_logger.log_job_complete(completed_count, total_records, job_duration, {
    'failed_tables': len([result for result in all_results if not result[0]])
})

# Print metrics summary
metrics_summary = metrics.get_metrics_summary()
logger.info(f"Metrics Summary: {json.dumps(metrics_summary, indent=2)}")
```

### 2. CDC Consumer (`jobs/cdc_kafka_to_iceberg.py`)

#### Step 2.1: Add Imports
**Location**: After line 28

```python
from monitoring import MetricsCollector, StructuredLogger
```

#### Step 2.2: Initialize Monitoring
**Location**: In run_cdc_consumer function (after line 475)

```python
# Initialize monitoring
metrics = MetricsCollector('cdc_consumer', source_name, enable_cloudwatch=True)
struct_logger = StructuredLogger('cdc_consumer', source_name)

# Log job start
struct_logger.log_job_start({
    'namespace': namespace,
    'batch_size': batch_size,
    'starting_scn': start_position.get('oracle_scn'),
    'starting_lsn': start_position.get('postgres_lsn')
})
```

#### Step 2.3: Pass Metrics to process_kafka_batch
**Location**: Line 522

Update function signature:
```python
def process_kafka_batch(spark: SparkSession, iceberg_mgr: IcebergTableManager, kafka_config: dict, source_config: dict,
                        tracker: CDCStatusTracker, table_scn_map: Dict[str, int],
                        primary_keys_map: Dict[str, list], schema_tracker: SchemaTracker = None,
                        metrics: MetricsCollector = None, struct_logger: StructuredLogger = None,
                        batch_size: int = 1000):
```

#### Step 2.4: Add Batch Metrics
**Location**: In process_batch function (after line 445)

```python
# Emit batch metrics
if metrics:
    metrics.emit_counter('cdc_events_processed', events_processed)
    metrics.emit_counter('cdc_events_skipped', events_skipped)
    metrics.emit_counter('cdc_new_tables', new_tables_created)
    metrics.emit_counter('cdc_batch_processed', 1)

# Log batch processing
if struct_logger:
    struct_logger.log_event('cdc_batch_complete', {
        'batch_id': batch_id,
        'events_processed': events_processed,
        'events_skipped': events_skipped,
        'new_tables': new_tables_created
    })
```

### 3. Standardized Logging Format

#### Chosen Format
```
%(asctime)s - [%(threadName)s] - %(name)s - %(levelname)s - %(message)s
```

#### Implementation Status
- ✅ `jobs/direct_bulk_load.py` - Already using standard format (line 51)
- ✅ `jobs/cdc_kafka_to_iceberg.py` - Already using standard format (line 46)
- ⚠️ `lib/extractors/oracle_extractor.py` - Uses `{table_name}: message` format
- ⚠️ `lib/extractors/postgres_extractor.py` - Uses `{table_name}: message` format

#### Standardization Approach
**Keep extractor format as-is** - The `{table_name}: message` format is intentional for clarity in parallel processing. The timestamp and thread info are added by the root logger.

**Rationale**:
- Extractors log from multiple threads, table name prefix is crucial for debugging
- Root logger configuration already adds timestamps and thread names
- Combined format: `2025-01-25 10:30:45 - [Thread-3] - extractors.oracle - INFO - CUSTOMERS: Extracting 1000 records`

### 4. Configuration Examples

#### sources.yaml - Add monitoring and DQ config

```yaml
sources:
  dev_mydb:
    database_type: oracle
    # ... existing config ...

    # Monitoring configuration (optional)
    monitoring:
      enable_cloudwatch: true  # Enable CloudWatch metrics
      cloudwatch_namespace: 'ETL/Pipeline'  # Custom namespace

    # Data quality configuration (optional)
    data_quality:
      check_row_count: true  # Validate row counts
      row_count_tolerance: 0.01  # Allow 1% difference

      # Required columns per table (cannot be null)
      required_columns:
        CUSTOMERS:
          - CUSTOMER_ID
          - CREATED
        ORDERS:
          - ORDER_ID
          - ORDER_DATE

      # Checksum validation for critical tables (expensive)
      checksum_tables:
        - CUSTOMERS
        - ORDERS
```

## Testing Integration

### Test Monitoring

1. **Local Test (without CloudWatch)**:
```bash
export CONFIG_DIR=./config
python jobs/direct_bulk_load.py --source dev_mydb --table-filter "CUSTOMERS"
```

Check logs for:
- `METRIC: {"metric_name": "table_processed", ...}`
- `EVENT: {"event_type": "table_processing_complete", ...}`

2. **Databricks Test (with CloudWatch)**:
```bash
# Install boto3 in cluster
pip install boto3

# Run job - metrics will be sent to CloudWatch
```

Check CloudWatch console for namespace `ETL/Pipeline`.

### Test Data Quality

1. Add to sources.yaml:
```yaml
data_quality:
  check_row_count: true
  row_count_tolerance: 0.0  # Exact match required
```

2. Run load:
```bash
python jobs/direct_bulk_load.py --source dev_mydb --table-filter "CUSTOMERS"
```

3. Check logs for:
- `CUSTOMERS: Row count validation PASSED - source: 1000, dest: 1000`

## Rollback Instructions

If integration causes issues:

1. **Remove monitoring imports**:
```bash
# Edit direct_bulk_load.py
# Remove lines: from monitoring import ...
# Remove lines: from data_quality import ...
```

2. **Remove monitoring calls**:
   - Keep existing logger.info() calls
   - Comment out all metrics.emit_*() calls
   - Comment out all struct_logger.log_*() calls
   - Comment out all dq_checker.*() calls

3. **Revert config_loader.py** (if needed):
```bash
git checkout lib/config_loader.py
```

## Performance Impact

**Expected Overhead**:
- **Structured Logging**: Negligible (~0.1% CPU)
- **Metrics Collection**: Negligible (~0.1% CPU)
- **CloudWatch Metrics**: ~1-2ms per metric (asynchronous)
- **Row Count DQ Check**: ~1-2 seconds per table (requires count query)
- **Checksum DQ Check**: ~30-60 seconds per large table (not recommended for all tables)

**Recommendations**:
- Enable CloudWatch in production for alerting
- Enable row count checks for all tables (cheap, valuable)
- Enable checksum checks only for critical small tables
- Use structured logging for downstream analytics (Splunk, DataDog, etc.)

## Troubleshooting

### Issue: boto3 not found
```
ImportError: No module named 'boto3'
```

**Solution**: Install boto3 or disable CloudWatch
```python
metrics = MetricsCollector('bulk_load', source_name, enable_cloudwatch=False)
```

### Issue: CloudWatch permissions
```
botocore.exceptions.ClientError: An error occurred (AccessDenied)
```

**Solution**: Add CloudWatch permissions to IAM role
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["cloudwatch:PutMetricData"],
    "Resource": "*"
  }]
}
```

### Issue: DQ checks slow
```
Data quality checks taking too long
```

**Solution**: Disable expensive checks
```yaml
data_quality:
  check_row_count: true  # Keep this (fast)
  checksum_tables: []    # Disable checksums (slow)
```

## Dynatrace Integration

### Overview

Dynatrace provides advanced application performance monitoring (APM) with automatic instrumentation, distributed tracing, and AI-powered anomaly detection. The ETL pipeline has been enhanced with:

1. **Correlation IDs** - For distributed tracing across Kafka → Spark → Iceberg
2. **JMX Metrics** - Spark executor and JVM metrics
3. **CDC Lag Tracking** - Real-time monitoring of replication lag
4. **Throughput Metrics** - Processing rate monitoring
5. **Enhanced Structured Logging** - Dynatrace-enriched log events

### Prerequisites

- Dynatrace SaaS or Managed environment
- Dynatrace API token with `metrics.ingest` permission
- Databricks cluster with init script capability (for OneAgent)

### Phase 1: Core Infrastructure (Already Implemented)

#### Correlation IDs

The `lib/correlation.py` module provides distributed tracing capabilities:

```python
from correlation import get_correlation_id, set_correlation_id

# Automatically generated per job/batch
correlation_id = get_correlation_id()

# Propagate through Kafka headers
kafka_record.headers.add('correlation_id', correlation_id)

# Set from incoming message
set_correlation_id(incoming_correlation_id)
```

**Benefits:**
- End-to-end request tracking across services
- Links logs, metrics, and traces in Dynatrace
- Enables service flow mapping

#### Enhanced Structured Logging

All log events now include Dynatrace-specific fields:

```json
{
  "timestamp": "2025-01-27T10:30:45.123456",
  "correlation_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "job_name": "cdc_consumer",
  "source_name": "bronze.siebel_oracle",
  "event_type": "cdc_batch_complete",
  "environment": "prod",
  "severity": "INFO",
  "details": {
    "batch_id": 12345,
    "events_processed": 1000,
    "throughput_records_per_sec": 85.3,
    "max_lag_seconds": 2.1
  }
}
```

**Dynatrace Configuration:**
- Logs are automatically ingested by OneAgent
- Use log enrichment rules to extract custom attributes
- Create log metrics for alerting (e.g., `cdc_lag_seconds > 60`)

#### Spark JMX Metrics

Spark metrics are exported via JMX for OneAgent to scrape:

**Configured in `lib/spark_utils.py`:**
```python
spark.metrics.conf.*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
spark.metrics.namespace=${spark.app.name}
```

**Metrics Available:**
- Executor memory usage (heap/off-heap)
- Task execution time and count
- Shuffle read/write bytes
- GC time and count
- JVM thread count

**Access in Dynatrace:**
- Navigate to: Process group → Custom metrics
- Filter by namespace: `<app_name>`
- Create custom charts for memory, GC, shuffle

### Phase 2: CDC Enhancements (Already Implemented)

#### CDC Lag Tracking

The CDC consumer now tracks replication lag:

```python
# Automatically calculated from Debezium ts_ms
event_timestamp_ms = source_info.get('ts_ms')
processing_timestamp_ms = int(time.time() * 1000)
lag_seconds = (processing_timestamp_ms - event_timestamp_ms) / 1000.0

# Emitted as CloudWatch metric
metrics.emit_gauge('cdc_lag_seconds', lag_seconds,
                  unit='Seconds',
                  dimensions={'namespace': namespace})
```

**CloudWatch Metrics Emitted:**
- `cdc_lag_seconds` - Time behind source database (max across batch)
- `cdc_throughput_records_per_sec` - Processing rate
- `cdc_batch_processing_time` - Batch duration
- `cdc_events_processed` - Events processed count
- `cdc_events_skipped` - Events skipped count

**Import to Dynatrace:**
1. Configure AWS integration in Dynatrace
2. Enable CloudWatch metrics import
3. Metrics appear under: Technologies → AWS → CloudWatch
4. Create custom charts and alerts

#### Throughput Monitoring

Batch processing metrics provide visibility into pipeline performance:

```python
batch_duration = time.time() - batch_start_time
throughput = events_processed / batch_duration

metrics.emit_gauge('cdc_throughput_records_per_sec', throughput,
                  unit='Count/Second')
```

**Structured Logging:**
```json
{
  "event_type": "cdc_batch_complete",
  "details": {
    "batch_id": 12345,
    "events_processed": 1000,
    "batch_duration_seconds": 11.7,
    "throughput_records_per_sec": 85.3,
    "max_lag_seconds": 2.1
  }
}
```

### Phase 3: Dynatrace OneAgent Deployment

#### Step 1: Create Dynatrace Init Script

Create `/dbfs/databricks/init-scripts/install-dynatrace.sh`:

```bash
#!/bin/bash

# Dynatrace environment configuration
DYNATRACE_ENVIRONMENT_ID="${DYNATRACE_ENV_ID}"  # From secret scope
DYNATRACE_API_TOKEN="${DYNATRACE_API_TOKEN}"    # From secret scope

# Download OneAgent installer
wget -O Dynatrace-OneAgent.sh \
  "https://${DYNATRACE_ENVIRONMENT_ID}.live.dynatrace.com/api/v1/deployment/installer/agent/unix/default/latest?Api-Token=${DYNATRACE_API_TOKEN}"

# Install OneAgent
/bin/sh Dynatrace-OneAgent.sh \
  --set-app-log-content-access=true \
  --set-infra-only=false \
  --set-host-group=databricks-etl \
  --set-host-tag=environment=prod \
  --set-host-tag=team=data-engineering

# Verify installation
if [ -f /opt/dynatrace/oneagent/agent/lib64/liboneagentproc.so ]; then
  echo "Dynatrace OneAgent installed successfully"
else
  echo "Dynatrace OneAgent installation failed"
  exit 1
fi
```

#### Step 2: Configure Databricks Cluster

**Cluster Configuration JSON:**
```json
{
  "cluster_name": "etl-cdc-consumer",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "i3.xlarge",
  "num_workers": 4,
  "init_scripts": [
    {
      "dbfs": {
        "destination": "dbfs:/databricks/init-scripts/install-dynatrace.sh"
      }
    }
  ],
  "spark_env_vars": {
    "DYNATRACE_ENV_ID": "{{secrets/dynatrace/environment_id}}",
    "DYNATRACE_API_TOKEN": "{{secrets/dynatrace/api_token}}",
    "DT_CUSTOM_PROP": "application=etl-pipeline,component=cdc-consumer,env=prod"
  }
}
```

#### Step 3: Create Databricks Secrets

```bash
# Create secret scope (one-time setup)
databricks secrets create-scope --scope dynatrace

# Add secrets
databricks secrets put --scope dynatrace --key environment_id --string-value "<your-env-id>"
databricks secrets put --scope dynatrace --key api_token --string-value "<your-token>"
```

#### Step 4: Verify Dynatrace Integration

After cluster starts:

1. **Check OneAgent Status:**
```bash
# SSH to cluster driver node
sudo systemctl status oneagent

# Check OneAgent logs
sudo tail -f /var/log/dynatrace/oneagent/oneagent.log
```

2. **Verify in Dynatrace UI:**
- Navigate to: Hosts → Filter by "databricks-etl"
- Click on host → Process groups
- Verify Spark driver and executor processes are detected
- Check: Technologies → Java → View JMX metrics

3. **Validate Distributed Tracing:**
- Navigate to: Distributed traces
- Filter by: `correlation_id` exists
- Verify end-to-end traces from Kafka to Iceberg

### Dynatrace Dashboards

#### CDC Monitoring Dashboard

**Tiles:**
1. **CDC Lag** - Line chart of `cdc_lag_seconds` over time
   - Alert threshold: > 60 seconds
   - Critical threshold: > 300 seconds

2. **Throughput** - Line chart of `cdc_throughput_records_per_sec`
   - Color: Green > 50, Yellow 10-50, Red < 10

3. **Batch Processing Time** - Line chart of `cdc_batch_processing_time`
   - Alert if > 30 seconds (falling behind trigger interval)

4. **Events Processed vs Skipped** - Stacked area chart
   - `cdc_events_processed` and `cdc_events_skipped`

5. **Spark Executor Memory** - Line chart from JMX
   - Heap used / Heap max per executor

6. **GC Time** - Line chart from JMX
   - Total GC time per minute
   - Alert if > 10% of processing time

#### Service Flow Map

Automatic topology discovery via correlation IDs:

```
[Oracle DB] → [Debezium] → [Kafka] → [Spark CDC Consumer] → [Iceberg Tables]
```

**To view:**
1. Navigate to: Services
2. Find: `spark-etl-cdc-consumer`
3. Click: View service flow
4. Dynatrace automatically maps dependencies

### Alerting

#### Recommended Alerts

**CDC Lag Alert:**
```
Metric: cdc_lag_seconds
Condition: > 60 for 5 minutes
Severity: Warning

Condition: > 300 for 2 minutes
Severity: Critical
```

**Low Throughput Alert:**
```
Metric: cdc_throughput_records_per_sec
Condition: < 10 for 10 minutes
Severity: Warning
```

**Batch Processing Time Alert:**
```
Metric: cdc_batch_processing_time
Condition: > 30 seconds for 3 consecutive batches
Severity: Warning
(Indicates falling behind)
```

**JVM Memory Alert:**
```
Metric: jvm.memory.heap.used / jvm.memory.heap.max
Condition: > 0.9 (90%)
Severity: Warning
```

### Troubleshooting with Dynatrace

#### Scenario 1: High CDC Lag

1. **Check Distributed Trace:**
   - Filter by `correlation_id` of slow batch
   - Identify bottleneck: Kafka read, CDC apply, Iceberg write

2. **Check Spark Metrics:**
   - Executor memory usage - is it spilling to disk?
   - GC time - excessive garbage collection?
   - Shuffle metrics - data skew?

3. **Check Logs:**
   - Filter by `event_type: cdc_batch_complete`
   - Look for: `max_lag_seconds` trend
   - Correlate with `batch_duration_seconds`

#### Scenario 2: Falling Behind Warning

**Message:** "Current batch is falling behind. The trigger interval is 10000 milliseconds, but spent 13232 milliseconds"

**Investigation:**
1. Check `cdc_batch_processing_time` metric
2. Compare to trigger interval (10 seconds)
3. If consistently > 10s, options:
   - Reduce batch size (lower `maxOffsetsPerTrigger`)
   - Increase parallelism (more executors/cores)
   - Optimize CDC operations (check table-specific metrics)

**Dynatrace Analysis:**
```
1. View service: spark-etl-cdc-consumer
2. Check: Response time distribution
3. Identify: Slow percentiles (p95, p99)
4. Drill down: Slow requests → Find specific tables/operations
```

### Best Practices

1. **Correlation ID Propagation:**
   - Generate once at job/batch start
   - Pass through all operations
   - Include in Kafka headers when producing

2. **Log Volume:**
   - Use structured logging for all events
   - Set appropriate log levels (INFO for key events, DEBUG for details)
   - Dynatrace automatically indexes and searches

3. **Metric Naming:**
   - Use consistent prefixes: `cdc_`, `bulk_load_`
   - Include dimensions: `namespace`, `table_name`
   - Follow CloudWatch naming conventions

4. **Dashboard Design:**
   - Create separate dashboards per job type
   - Include both application and infrastructure metrics
   - Add links to related logs and traces

5. **Alert Tuning:**
   - Start with conservative thresholds
   - Monitor for 1-2 weeks before adjusting
   - Use Dynatrace AI for baseline detection

## Next Steps

1. Complete integration following steps above
2. Test in development environment
3. Deploy to staging with CloudWatch enabled
4. Deploy Dynatrace OneAgent to Databricks clusters
5. Configure Dynatrace dashboards and alerts
6. Monitor for 1 week and tune thresholds
7. Document runbooks for common issues
8. Train team on Dynatrace navigation and troubleshooting
