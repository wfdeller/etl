# Oracle LONG Datatype Limitations

## Overview

Oracle's LONG datatype is a legacy column type that predates modern LOB (Large Object) types like CLOB. It has significant restrictions that make it incompatible with modern ETL tools, particularly when using Spark JDBC for bulk data extraction.

## The Problem

**LONG columns cannot be read via Spark JDBC with point-in-time consistency.**

### Why This Happens

1. **Oracle Restriction**: LONG columns cannot be selected through subqueries
   ```sql
   -- This fails with ORA-00923:
   SELECT * FROM (SELECT long_column FROM my_table) WHERE 1=0
   ```

2. **Spark JDBC Behavior**: Spark JDBC always wraps queries in a subquery for schema inference:
   ```sql
   -- Spark internally executes:
   SELECT * FROM (your_query) SPARK_GEN_SUBQ_XX WHERE 1=0
   ```

3. **Result**: Even direct SELECT statements for LONG columns fail because Spark wraps them in a subquery during schema detection.

### Additional LONG Restrictions

- Cannot use LONG columns in WHERE clauses
- Cannot use LONG columns with `AS OF SCN` (flashback queries)
- Cannot partition reads on LONG columns
- Cannot SELECT LONG through views with GROUP BY, DISTINCT, or aggregations
- Cannot convert LONG to CLOB using `CAST()` or simple `TO_LOB()`

## Testing Summary

We attempted to implement a `convert_to_clob` option that would:
1. **Pass 1**: Read all non-LONG columns with AS OF SCN (for point-in-time consistency)
2. **Pass 2**: Read LONG columns separately (without SCN, current data only)
3. Join the two result sets

**Result**: Failed due to Spark's automatic subquery wrapping during schema inference. No workaround exists within Spark JDBC framework.

### Error Example
```
java.sql.SQLSyntaxErrorException: ORA-00923: FROM keyword not found where expected
Caused by: Error : 923, Position : 31,
Sql = SELECT * FROM (SELECT ROWID as ROWID, NOTE FROM SIEBEL.EIM_ACT_DTL) SPARK_GEN_SUBQ_30 WHERE 1=0
```

## Current Solution: `exclude` Mode (Recommended)

The ETL pipeline handles LONG columns using **exclusion** - LONG columns are automatically identified and excluded from the bulk load.

### Configuration
```yaml
bulk_load:
  handle_long_columns: exclude    # Recommended
```

### Available Options

| Option | Behavior | Use Case |
|--------|----------|----------|
| `exclude` | Skip LONG columns, load rest of table | **Recommended** - Load structural data, exclude problematic columns |
| `skip_table` | Skip entire table if LONG exists | When LONG data is critical and table shouldn't be loaded incomplete |
| `error` | Fail the job if LONG columns found | Strict mode - force manual handling |

### What Gets Excluded

Example from test database (1373 tables total):
- **91 tables** (6.6%) have LONG columns
- Common LONG column names: `NOTE`, `COMMENTS`, `TEXT`, `SCRIPT`, `DESCRIPTION`
- Examples:
  - `ACTIVITY_DETAILS.NOTE`
  - `ACCOUNT_DETAILS.NOTE_TEXT`
  - `COMMUNICATIONS.TEXT`
  - `SCRIPT_DEFINITIONS.SCRIPT`

## Impact Assessment

### What Still Works Perfectly

1. **Point-in-time Consistency**: All non-LONG columns are extracted with SCN flashback
2. **Parallel Processing**: Full parallelization for tables without LONG columns
3. **CDC Integration**: Change Data Capture (Debezium) **CAN** capture LONG column changes in real-time
4. **Table Coverage**: 93.4% of tables (1282/1373) load completely without any exclusions

### ⚠️ Limitations

1. **Initial Load**: LONG column data not included in bulk load
2. **Historical Data**: Cannot backfill LONG column data from specific point in time
3. **Partitioning**: Tables with LONG columns cannot be partitioned (slower extraction)

## Workarounds and Alternatives

### Option 1: Rely on CDC (Recommended)

**Best for**: Most use cases where historical LONG data isn't critical

- Initial bulk load excludes LONG columns
- CDC (Debezium) captures all changes including LONG columns going forward
- Over time, LONG data populates through ongoing changes

**Pros**:
- No additional development needed
- Works out of the box
- CDC captures full LONG data

**Cons**:
- Historical LONG data before CDC start is lost
- Requires time for LONG data to accumulate

### Option 2: Oracle Database Migration

**Best for**: Organizations planning Oracle modernization

Migrate LONG columns to CLOB in source database:

```sql
-- Example migration
ALTER TABLE my_table ADD new_note_col CLOB;
UPDATE my_table SET new_note_col = TO_LOB(note);
ALTER TABLE my_table DROP COLUMN note;
ALTER TABLE my_table RENAME COLUMN new_note_col TO note;
```

**Pros**:
- Solves problem permanently
- Modern datatype with better performance
- Enables all Spark JDBC features

**Cons**:
- Requires production database changes
- May need application code updates
- Coordination with database team required

### Option 3: Custom Python Extraction

**Best for**: Specific critical tables requiring LONG data in initial load

Use cx_Oracle or python-oracledb directly (not Spark):

```python
import oracledb
import pyarrow as pa

connection = oracledb.connect(...)
cursor = connection.cursor()

# Direct read bypasses Spark's subquery wrapping
cursor.execute("SELECT id, long_column FROM my_table")

# Convert to Arrow/Parquet
# Write to Iceberg manually
```

**Pros**:
- Can read LONG columns directly
- Full control over extraction logic

**Cons**:
- No Spark parallelization
- Custom code maintenance
- Slower for large tables
- Still cannot use AS OF SCN with LONG columns

### Option 4: Database Link + View

**Best for**: Read-only access scenarios

Create a view that converts LONG to CLOB:

```sql
CREATE VIEW my_table_view AS
SELECT
    id,
    name,
    TO_LOB(long_column) as long_column
FROM my_table;
```

**Pros**:
- No table modification needed
- Transparent to applications

**Cons**:
- Still cannot use with AS OF SCN
- Performance overhead from TO_LOB()
- Requires INSERT statement context (not SELECT)

## Oracle's Recommendation

Per Oracle documentation:
> "Do not create tables with LONG columns. Use LOB columns (CLOB, NCLOB, BLOB) instead. LONG columns are supported only for backward compatibility."

Source: [Oracle Database SQL Language Reference](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A3C0D836-BADB-44E5-A5D4-265BA5968483)

## Summary

1. **LONG columns cannot be extracted via Spark JDBC** due to Oracle's subquery restrictions and Spark's schema inference mechanism
2. **Use `exclude` mode** (default) - excludes LONG columns, loads rest of table
3. **CDC captures LONG changes** - data accumulates over time through Change Data Capture
4. **For critical LONG data**: Consider database migration (LONG → CLOB) or custom extraction

## References

- [Oracle LONG Datatype Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-A3C0D836-BADB-44E5-A5D4-265BA5968483)
- [Oracle LOB Developer's Guide](https://docs.oracle.com/en/database/oracle/oracle-database/19/adlob/index.html)
- [Spark JDBC Documentation](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

---

**Last Updated**: 2025-11-24
**Testing**: Validated on Oracle 19c with Spark 3.5.0
