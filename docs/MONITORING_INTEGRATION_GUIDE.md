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
  dev_siebel:
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

## Next Steps

1. Complete integration following steps above
2. Test in development environment
3. Deploy to staging with CloudWatch enabled
4. Monitor CloudWatch dashboard for 1 week
5. Set up CloudWatch alarms for failure metrics
6. Document any custom metrics needed
7. Create Databricks SQL Analytics dashboards
