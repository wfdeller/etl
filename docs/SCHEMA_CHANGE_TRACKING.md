# Schema Change Tracking

## Overview

The ETL pipeline includes automated schema change tracking to help transformation developers stay informed about upstream database schema modifications. When a table's schema changes (columns added, removed, or type changed), the system automatically detects and records these changes in an audit table.

This feature provides:
- **Automatic Detection**: Schema changes are detected during both bulk loads and CDC processing
- **Non-Breaking**: Schema changes do not stop the pipeline; they are logged for review
- **Historical Audit Trail**: Complete history of all schema changes with timestamps
- **Query Tool**: CLI tool to query and export schema change history

## Architecture

### Components

1. **SchemaTracker** (`lib/schema_tracker.py`)
   - Core library for schema change detection and recording
   - Manages the `_schema_changes` audit table in Iceberg

2. **Direct Bulk Load Integration** (`jobs/direct_bulk_load.py`)
   - Records baseline schema when a table is first loaded
   - Establishes the initial schema snapshot

3. **CDC Consumer Integration** (`jobs/cdc_kafka_to_iceberg.py`)
   - Detects schema changes during CDC event processing
   - Logs warnings when changes are detected

4. **Query Tool** (`jobs/query_schema_changes.py`)
   - CLI tool to query schema change history
   - Supports filtering and CSV export

### Audit Table Schema

The `_schema_changes` table is created in each namespace (e.g., `bronze.siebel._schema_changes`) with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `change_id` | string | Unique change identifier (timestamp-based) |
| `table_name` | string | Name of the table that changed |
| `change_type` | string | Type of change (see below) |
| `column_name` | string | Column affected (null for baseline) |
| `old_data_type` | string | Previous data type (null for additions) |
| `new_data_type` | string | New data type (null for removals) |
| `detected_timestamp` | timestamp | When the change was detected |
| `source_db` | string | Source database identifier |
| `change_details` | string | JSON with additional metadata |

### Change Types

| Change Type | Description |
|-------------|-------------|
| `baseline` | Initial schema snapshot (first load) |
| `column_added` | New column detected in source |
| `column_removed` | Column no longer present in source |
| `column_modified` | Column data type changed |

## How It Works

### 1. Baseline Schema Capture (Initial Load)

When a table is loaded for the first time via `direct_bulk_load.py`, the pipeline:

1. Reads the source table schema
2. Records a `baseline` entry in `_schema_changes`
3. Stores the complete schema as JSON in `change_details`

**Example baseline record:**
```json
{
  "change_id": "S_CONTACT_20250125_143022_123456_baseline",
  "table_name": "S_CONTACT",
  "change_type": "baseline",
  "column_name": null,
  "old_data_type": null,
  "new_data_type": null,
  "detected_timestamp": "2025-01-25T14:30:22",
  "source_db": "oracle.example.com:1521/PROD",
  "change_details": "{\"fields\": [{\"name\": \"ROW_ID\", \"type\": \"StringType\", \"nullable\": false}, ...]}"
}
```

### 2. Schema Change Detection (CDC)

During CDC processing in `cdc_kafka_to_iceberg.py`, before applying each CDC event:

1. Extract the schema from the CDC event payload
2. Compare against the latest known schema in `_schema_changes`
3. Detect any additions, removals, or type modifications
4. Record each detected change as a separate row
5. Log warnings for transformation developer awareness

**Example column addition:**
```json
{
  "change_id": "S_CONTACT_20250125_153045_789012_add_EMAIL_ADDR",
  "table_name": "S_CONTACT",
  "change_type": "column_added",
  "column_name": "EMAIL_ADDR",
  "old_data_type": null,
  "new_data_type": "StringType",
  "detected_timestamp": "2025-01-25T15:30:45",
  "source_db": "oracle.example.com:1521/PROD",
  "change_details": "{\"action\": \"added\", \"column\": \"EMAIL_ADDR\", \"type\": \"StringType\"}"
}
```

### 3. Logged Warnings

When schema changes are detected during CDC processing, the system logs warnings:

```
2025-01-25 15:30:45 - WARNING - Schema change detected in S_CONTACT: column_added - column EMAIL_ADDR (None -> StringType)
```

These warnings appear in:
- CDC consumer logs (stdout/log files)
- Databricks job run logs (for Databricks deployments)

## Querying Schema Changes

### Using the Query Tool

The `query_schema_changes.py` CLI tool provides flexible querying capabilities:

#### List All Tables with Changes

```bash
python jobs/query_schema_changes.py --source dev_siebel --list-tables
```

Output:
```
+-------------+-----+
|   table_name|count|
+-------------+-----+
|    S_CONTACT|    3|
|     S_ACCNT |    1|
| S_ACT_PLAN  |    2|
+-------------+-----+
```

#### Query All Changes for a Table

```bash
python jobs/query_schema_changes.py --source dev_siebel --table S_CONTACT
```

#### Filter by Change Type

```bash
python jobs/query_schema_changes.py --source dev_siebel --table S_CONTACT --change-type column_added
```

#### Filter by Date

```bash
python jobs/query_schema_changes.py --source dev_siebel --since 2025-01-01
```

#### Export to CSV

```bash
python jobs/query_schema_changes.py --source dev_siebel --table S_CONTACT --output csv --file /tmp/changes.csv
```

### Querying Directly with Spark SQL

You can also query the audit table directly using Spark SQL or Databricks notebooks:

```sql
-- All changes for a table
SELECT
  change_type,
  column_name,
  old_data_type,
  new_data_type,
  detected_timestamp,
  source_db
FROM local.bronze.siebel._schema_changes
WHERE table_name = 'S_CONTACT'
ORDER BY detected_timestamp DESC;

-- Recent changes across all tables
SELECT
  table_name,
  change_type,
  column_name,
  detected_timestamp
FROM local.bronze.siebel._schema_changes
WHERE detected_timestamp >= current_date() - INTERVAL 7 DAYS
ORDER BY detected_timestamp DESC;

-- Count of changes by type
SELECT
  change_type,
  COUNT(*) as change_count
FROM local.bronze.siebel._schema_changes
WHERE change_type != 'baseline'
GROUP BY change_type
ORDER BY change_count DESC;
```

## Configuration

### Environment Variables

No additional environment variables are required. Schema tracking is automatically enabled when:
- The `schema_tracker` library is imported
- The tracker is initialized with `SchemaTracker(spark, namespace)`

### Disabling Schema Tracking

To disable schema tracking (not recommended for production):

1. **For Bulk Loads**: Comment out the `schema_tracker.record_baseline_schema()` calls in `direct_bulk_load.py`
2. **For CDC**: Remove or comment out the schema change detection logic in `cdc_kafka_to_iceberg.py`

## Best Practices

### For Data Engineers

1. **Monitor Schema Changes Regularly**
   - Set up weekly queries to check for new schema changes
   - Use `--since` filter to check changes since last review

2. **Integrate with Alerting**
   - Query the `_schema_changes` table from monitoring systems
   - Send Slack/email notifications when `change_type != 'baseline'`
   - Example: Query every hour and alert if new changes detected

3. **Document Schema Changes**
   - Export schema change history when documenting data lineage
   - Include `change_details` JSON in technical documentation

4. **Review Before Production Releases**
   - Always check for schema changes before promoting transformations to production
   - Test transformation logic against new schemas in dev/staging

### For Transformation Developers

1. **Check Schema Changes Before Coding**
   ```bash
   # Check if your source table has recent changes
   python jobs/query_schema_changes.py --source dev_siebel --table YOUR_TABLE --since 2025-01-01
   ```

2. **Handle Schema Evolution Gracefully**
   - Use dynamic schema handling where possible
   - Avoid hardcoding column names in transformations
   - Use `SELECT *` cautiously; prefer explicit column lists with null coalescing for new columns

3. **Test with Schema Changes**
   - When a column is added, ensure transformations handle nulls appropriately
   - When a column is removed, ensure transformations don't fail
   - When types change, validate type casting logic

4. **Coordinate with Source Teams**
   - Use schema change audit as evidence for discussions with source database teams
   - Request advance notice for major schema changes
   - Negotiate DDL change freeze periods during critical releases

## Limitations

1. **No Advance Notice**
   - Changes are detected **after** they occur in the source database
   - Pipeline does not prevent schema changes from propagating

2. **No Automatic Transformation Updates**
   - Downstream transformations must be manually updated
   - Schema changes may break existing transformation logic

3. **Baseline Required**
   - Schema change detection requires a baseline record
   - If baseline is missing, the next load creates a new baseline (no delta detected)

4. **Type Detection Granularity**
   - Type detection is based on PySpark's schema inference
   - Subtle type changes (e.g., VARCHAR(50) → VARCHAR(100)) may not be detected
   - Nullability changes are tracked in `change_details` but not as separate change records

## Future Enhancements

The TODO list includes potential future enhancements:

1. **Notification System** (`lib/notification_handler.py`)
   - Email/Slack/SNS notifications when schema changes detected
   - Configurable notification thresholds and recipients
   - Integration with Databricks job notifications

2. **Schema Evolution Policies**
   - Configurable policies for handling different change types
   - Automatic transformation updates for simple cases (e.g., adding nullable columns)
   - Blocking policies to prevent breaking changes from propagating

3. **Impact Analysis**
   - Track which downstream transformations use which columns
   - Automatically identify affected transformations when schema changes
   - Leverage Unity Catalog's column-level lineage

4. **Schema Registry Integration**
   - Use Unity Catalog as centralized schema registry
   - Bidirectional sync between schema changes and catalog
   - Leverage Iceberg's schema evolution features

## Troubleshooting

### Schema Changes Not Detected

**Symptom**: Expected schema changes are not appearing in `_schema_changes` table

**Causes & Solutions**:

1. **Baseline Missing**
   - Check if baseline exists: `SELECT * FROM _schema_changes WHERE table_name = 'YOUR_TABLE' AND change_type = 'baseline'`
   - Solution: Re-run bulk load for the table to establish baseline

2. **CDC Consumer Not Running**
   - Verify CDC consumer is actively processing events
   - Check consumer logs for schema change warnings

3. **Schema Tracker Not Initialized**
   - Verify `schema_tracker = SchemaTracker(...)` exists in job code
   - Check that tracker is passed to `apply_cdc_event()` function

### False Positives

**Symptom**: Schema changes reported but source schema hasn't changed

**Causes & Solutions**:

1. **Type Inference Variation**
   - PySpark may infer types differently across runs
   - Solution: Use explicit schema definitions where possible

2. **Column Ordering Changes**
   - Column order changes don't constitute schema changes
   - Current implementation is order-independent

### Query Tool Errors

**Symptom**: `query_schema_changes.py` fails with exceptions

**Causes & Solutions**:

1. **Source Configuration Missing**
   - Verify source exists in `sources.yaml`
   - Check namespace configuration is correct

2. **Audit Table Doesn't Exist**
   - Table created automatically on first schema tracker initialization
   - Run a bulk load or CDC consumer to create the table

3. **Spark Session Errors**
   - Ensure Spark is properly configured
   - Check Iceberg catalog connectivity

## Related Documentation

- [Architecture Documentation](ARCHITECTURE.md) - Overall system design
- [Databricks Deployment Guide](DATABRICKS_DEPLOYMENT.md) - Production deployment
- [Oracle LONG Limitations](ORACLE_LONG_LIMITATIONS.md) - Data type handling

## Support

For questions or issues with schema change tracking:

1. Check the logs in `logs/` directory (local) or Databricks job logs
2. Query the `_schema_changes` table directly to verify contents
3. Review source code in `lib/schema_tracker.py` for implementation details
4. Consult the project TODO list for known limitations and planned enhancements
