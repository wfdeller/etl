# ETL Project TODO

## Status Legend
- [DONE] Completed
- [WIP] In Progress
- [TODO] Pending
- [BLOCKED] Blocked

## Deployment Strategy

**PRIMARY TARGET: Databricks on AWS**

All TODO items and architecture decisions prioritize Databricks compatibility. Local development support exists for initial testing but production deployment is Databricks-only. All features must work with:
- Unity Catalog for metadata management
- AWS S3 for data lake storage
- Databricks Secrets for credential management
- Databricks Jobs/Workflows for orchestration
- CloudWatch for monitoring and alerting

---

## Immediate Fixes (Fix Before Production)

### Critical Bugs
- [DONE] Fix postgres_extractor thread_id bug
- [DONE] Implement proper MERGE logic in CDC consumer
- [DONE] Fix schema fixer import paths in both extractors
- [DONE] Fix duplicate initialization in cdc_kafka_to_iceberg.py

### Security Issues
- [DONE] Add environment variable support for credentials
- [DONE] Fix SQL injection vulnerabilities with parameterized queries
- [DONE] Implement Databricks Secrets integration with environment variable fallback

### Data Type Issues
- [DONE] Change oracle_scn from IntegerType to LongType

---

## Recently Completed (Current Sprint)

### Code Refactoring - Phase 1
- [DONE] Create utility library modules to eliminate code duplication
  - Created `lib/spark_utils.py` - SparkSessionFactory for centralized Spark session creation
  - Created `lib/jdbc_utils.py` - DatabaseConnectionBuilder for JDBC configuration
  - Created `lib/iceberg_utils.py` - IcebergTableManager for table operations
  - Refactored all job scripts to use utilities (~162 lines of duplication eliminated)

### Databricks Support - Phase 2
- [DONE] Add Databricks deployment support
  - Created `lib/secrets_utils.py` - Unified secrets management (Databricks + env vars)
  - Enhanced `lib/spark_utils.py` with S3/MinIO support and Databricks detection
  - Created `docs/DATABRICKS_DEPLOYMENT.md` - Comprehensive deployment guide (500+ lines)
  - Updated `config/sources.yaml.template` with Databricks and MinIO examples
  - Updated `README.md` with Databricks deployment options and features
  - AWS-focused configuration (S3, IAM roles, Unity Catalog)

### Primary Key Handling - Phase 3
- [DONE] Implement automatic primary key discovery and dynamic handling
  - Added `get_primary_key_columns()` to both Oracle and PostgreSQL extractors
  - Extended status tracker schema with `primary_keys` column (ArrayType)
  - Implemented `get_primary_keys_map()` in CDC consumer to load PK metadata at startup
  - Replaced hardcoded fallback logic with dynamic PK handling supporting composite keys (using ROWID for Oracle, first column for others)
  - Added `primary_keys` configuration section to `sources.yaml.template` for manual overrides
  - Implemented config validation in `config_loader.py` for primary_keys section
  - **Files**: `lib/extractors/oracle_extractor.py`, `lib/extractors/postgres_extractor.py`,
    `lib/status_tracker.py`, `jobs/direct_bulk_load.py`, `jobs/cdc_kafka_to_iceberg.py`,
    `config/sources.yaml.template`, `lib/config_loader.py`

### Documentation - Phase 4
- [DONE] Create `docs/ORACLE_LONG_LIMITATIONS.md` - Oracle LONG column handling documentation
- [DONE] Update all documentation with Databricks deployment information
- [DONE] Complete README rewrite with beginner-friendly local setup guide
  - Removed all hard-coded paths (e.g., `/opt/pipeline`, `/opt/data`)
  - Added comprehensive 9-step local development setup (Java, Python, venv, JDBC drivers, etc.)
  - Added two clear deployment options: Local Development vs Databricks Production
  - Added extensive troubleshooting section for common setup issues
  - Updated to use relative paths and environment variables (`.env` file)
  - Added security best practices and .gitignore configuration
  - Made suitable for users unfamiliar with Python or Databricks

### Error Handling - Phase 5
- [DONE] Replace bare exception handlers with specific types
  - Fixed `lib/status_tracker.py` - 3 locations (lines 57, 151, 223)
  - Fixed `jobs/cdc_kafka_to_iceberg.py` - 1 location (line 298)
  - Verified `jobs/direct_bulk_load.py` - already using proper exception handling
  - Added `AnalysisException` import for Spark table operations
  - Added logging to track when fallback behavior occurs

### Configuration - Phase 6
- [DONE] Remove hardcoded paths and add cloud storage support
  - Added `LOG_DIR` environment variable support (optional, for local dev)
  - Added `CHECKPOINT_PATH` environment variable (supports s3://, dbfs://, file://)
  - Updated `jobs/cdc_kafka_to_iceberg.py` - configurable logging and checkpoints
  - Updated `jobs/direct_bulk_load.py` - configurable logging and checkpoints
  - Updated `clear_checkpoint_directory()` to handle cloud storage gracefully
  - All configuration now supports Databricks deployment out-of-the-box

### Code Quality - Phase 7
- [DONE] Refactor status_tracker.py to eliminate duplication
  - Extracted `_get_status_schema()` static method (eliminates 5x duplication)
  - Extracted `_merge_status_update()` method (eliminates 3x duplication)
  - Refactored all status update methods to use helper methods
  - Reduced from 355 lines to 288 lines (19% reduction, 67 lines eliminated)
  - Improved maintainability - schema changes now require single location update
  - **File**: `lib/status_tracker.py`

### Idempotency - Phase 8
- [DONE] Implement idempotency checks for bulk load
  - Extended status_tracker.py schema with `load_hash` column (SHA256)
  - Added `_calculate_load_hash()` method (based on source_db + table + SCN/LSN + record_count)
  - Added `check_duplicate_load()` method for hash-based duplicate detection
  - Integrated SCN/LSN-based idempotency check in `direct_bulk_load.py:133`
  - Skips extraction if same SCN/LSN already loaded (prevents duplicate data)
  - Tested successfully with S_CONTACT table - duplicate load detected and skipped
  - **Files**: `lib/status_tracker.py`, `jobs/direct_bulk_load.py`

### CDC Position Tracking - Phase 9
- [DONE] Improve CDC position tracking granularity
  - Changed update interval from 100 events to 10 events (10x more granular)
  - Added `CDC_POSITION_UPDATE_INTERVAL` environment variable for configuration
  - Default: 10 events (reduces risk of loss on crash from ~100 to ~10 events)
  - Configurable for performance tuning (higher = less I/O, lower = less risk)
  - **File**: `jobs/cdc_kafka_to_iceberg.py:53, 413`

### Schema Change Tracking - Phase 10
- [DONE] Implement comprehensive schema change tracking and notification system
  - Created `lib/schema_tracker.py` - SchemaTracker class with `_schema_changes` audit table (339 lines)
  - Tracks baseline schema, column additions, removals, and type modifications
  - Integrated baseline capture in `jobs/direct_bulk_load.py` (lines 189-191, 220-222)
  - Integrated change detection in `jobs/cdc_kafka_to_iceberg.py` (lines 209-222)
  - Created `jobs/query_schema_changes.py` - CLI tool for querying schema changes (241 lines)
  - Created `docs/SCHEMA_CHANGE_TRACKING.md` - Comprehensive documentation (400+ lines)
  - Schema changes logged as warnings in CDC consumer for developer awareness
  - Supports filtering by table, change type, and date range
  - CSV export capability for reporting and analysis
  - **Files**: `lib/schema_tracker.py`, `jobs/direct_bulk_load.py`, `jobs/cdc_kafka_to_iceberg.py`,
    `jobs/query_schema_changes.py`, `docs/SCHEMA_CHANGE_TRACKING.md`

### Monitoring & Data Quality - Phase 11
- [DONE] Create monitoring and data quality infrastructure libraries
  - Created `lib/monitoring.py` - CloudWatch metrics, structured logging, performance tracking (380 lines)
    - MetricsCollector: CloudWatch integration with fallback to structured logging
    - StructuredLogger: JSON event logging for downstream analysis
    - PerformanceTracker: Operation profiling with memory tracking
  - Created `lib/data_quality.py` - Comprehensive DQ validation (420 lines)
    - Row count validation with configurable tolerance
    - Null constraint validation for required columns
    - Data type validation (schema compatibility)
    - Table checksum comparison (MD5-based integrity)
    - Column statistics validation for numeric columns
  - Enhanced `lib/config_loader.py` with validation (110+ lines added)
    - bulk_load section validation (partitions, workers, retries, checkpoint)
    - data_quality section validation (checks, tolerance, required columns)
    - Automatic validation on config load (fail-fast on misconfiguration)
  - Created `docs/MONITORING_INTEGRATION_GUIDE.md` - Step-by-step integration guide (400+ lines)
    - Complete integration instructions for all jobs
    - Configuration examples and testing procedures
    - Performance impact analysis and troubleshooting
    - Rollback instructions if needed
  - Added monitoring imports to `jobs/direct_bulk_load.py`
  - **Status**: Libraries complete, integration guide documented, ready for deployment
  - **Files**: `lib/monitoring.py`, `lib/data_quality.py`, `lib/config_loader.py`,
    `docs/MONITORING_INTEGRATION_GUIDE.md`

### Configuration - Phase 12
- [DONE] Fix config loader to validate only requested source
  - Modified `lib/config_loader.py` to load raw YAML first, then substitute env vars only for requested source
  - Added `load_config_raw()` function for YAML loading without env var substitution
  - Allows loading PostgreSQL source without Oracle credentials and vice versa
  - **Files**: `lib/config_loader.py`

### Thread Safety - Phase 13
- [DONE] Implement thread-safe locking for schema tracker
  - Added global `threading.Lock()` to prevent concurrent write errors to `_schema_changes` table
  - Wrapped write operations in `record_baseline_schema()` and `detect_and_record_changes()`
  - Fixes Iceberg metadata file contention during parallel bulk loads with 4+ workers
  - **Files**: `lib/schema_tracker.py`

### Bug Fixes - Phase 14
- [DONE] Fix Spark Row.get() bug in status tracker
  - Fixed `existing.get('primary_keys', [])` which fails because Row objects don't have `.get()` method
  - Changed to `existing['primary_keys'] if 'primary_keys' in existing.asDict() else []`
  - Eliminates "Could not retrieve existing status: get" warnings during bulk loads
  - **Files**: `lib/status_tracker.py:232`

---

## Short Term (Next Sprint)

### Monitoring
- [DONE] Monitoring infrastructure libraries created (See Phase 11 above)
- [TODO] Complete integration into job files
  - Follow `docs/MONITORING_INTEGRATION_GUIDE.md` for step-by-step instructions
  - Test in development environment
  - Deploy to Databricks staging with CloudWatch enabled
  - Set up CloudWatch alarms for failure metrics
  - Create Databricks SQL Analytics dashboards

---

## Long Term (Future Enhancements)

### Schema Change Tracking & Notification
- [DONE] Implement schema change tracking for transformation developers (See Phase 10 above)
- [TODO] Add notification handler (Phase 2 - Future Enhancement)
  - Create `lib/notification_handler.py` - Email/Slack/SNS notifications
  - Update `config/sources.yaml.template` - Add notification configuration
  - Integrate with Databricks job notifications and CloudWatch alarms
  - Configurable notification thresholds and recipients

### Schema Evolution
- [TODO] Enhance schema management (depends on Schema Change Tracking above)
  - Leverage Iceberg's built-in schema evolution
  - Validate compatibility before applying changes
  - Use Unity Catalog's column-level lineage for impact analysis

### Exactly-Once Semantics
- [TODO] Implement exactly-once CDC processing
  - Use Kafka transactions
  - Implement idempotent CDC writes
  - Add deduplication logic based on SCN/LSN + operation

### Performance Optimization
- [TODO] Add JDBC connection pooling
  - Reuse connections across table extractions
  - Reduce connection overhead

- [TODO] Improve memory management
  - Use actual memory usage instead of row count for persistence decisions
  - Monitor Spark executor memory
  - Dynamic threshold adjustment

### Data Quality
- [TODO] Add data quality checks
  - Row count validation (source vs destination)
  - Checksum comparison for critical tables
  - Data type validation
  - Null value checks for required columns

### Testing
- [TODO] Add integration tests (Databricks-compatible)
  - End-to-end bulk load test using Databricks Jobs
  - CDC event processing test with test Kafka topics
  - Failure/retry scenarios with Databricks cluster termination
  - Schema evolution scenarios using Unity Catalog
  - Use Databricks Repos for test execution

- [TODO] Add unit tests (run in Databricks notebooks or CI/CD)
  - Extractor logic (mock JDBC connections)
  - Schema fixer logic
  - Config loader with Databricks Secrets and env vars
  - Status tracker operations (test against test Unity Catalog tables)
  - Package tests for wheel/egg deployment to Databricks

### Architecture
- [TODO] Use Iceberg's built-in MERGE operations (Unity Catalog compatible)
  - Replace custom merge logic with Iceberg native operations
  - Leverage Iceberg's ACID guarantees
  - Ensure compatibility with Unity Catalog's table format

- [TODO] Implement proper transaction semantics
  - Atomic table writes using Unity Catalog guarantees
  - Coordinated status updates within same catalog
  - Rollback on failure using Iceberg snapshots

- [TODO] Use Unity Catalog as schema registry
  - Unity Catalog as centralized metadata and schema management
  - Use table properties and tags for schema metadata
  - Leverage Iceberg metadata for schema versioning
  - Query information_schema for schema discovery and validation
  - Use Unity Catalog audit logs for schema change tracking

---

## Documentation

### Completed
- [DONE] Architecture documentation (docs/ARCHITECTURE.md)
- [DONE] README with setup instructions
- [DONE] TODO tracking (this file)
- [DONE] Databricks deployment guide (docs/DATABRICKS_DEPLOYMENT.md)
- [DONE] Oracle LONG limitations documentation (docs/ORACLE_LONG_LIMITATIONS.md)
- [DONE] Schema change tracking documentation (docs/SCHEMA_CHANGE_TRACKING.md)
- [DONE] Configuration templates with examples (config/*.template)

### Pending
- [TODO] Add inline documentation for complex algorithms
- [TODO] Document configuration parameters in dedicated guide
- [TODO] Create runbook for common operations (start/stop, monitoring, backups)
- [TODO] Add examples for common use cases (filtering tables, custom transformations)

---

## Technical Debt

### Code Organization
- [TODO] Add type hints throughout codebase
  - Add to all function signatures for better type safety

- [TODO] Standardize logging format
  - Oracle: `{table_name}: message`
  - Postgres: `{table_name}: message`
  - direct_bulk_load: `{table_name}: STATUS: message`
  - Pick one and apply everywhere

### Configuration Management
- [TODO] Validate bulk_load config section
  - Currently only top-level keys are validated
  - Add validation for nested configuration

- [TODO] Add config schema validation
  - Use JSON schema or similar
  - Catch configuration errors early

---

## Known Limitations

### Current Constraints (Databricks Deployment)
1. **Single Point of Failure**: Status tracking in single Unity Catalog table
2. **No Distributed Locking**: Parallel Databricks jobs on same source could conflict
3. **Limited Retry Logic**: Simple exponential backoff, no circuit breaker
4. **Kafka Offset Management**: Checkpoint deletion (DBFS/S3) causes full replay
5. **Schema Changes**: No automatic handling of DDL changes (see Schema Change Tracking in Long Term)
6. **Large Transactions**: No batching for very large CDC transactions
7. **Cross-Region**: No multi-region Unity Catalog support in design

### Workarounds (Databricks Context)
- Use Databricks job concurrency limits (max concurrent runs = 1 per source)
- Store checkpoints in S3 with versioning enabled
- Use Unity Catalog's schema evolution features where possible
- Monitor for schema changes using AWS Glue or Unity Catalog audit logs
- Split large tables using table-filter parameter in separate jobs

---

## Questions for Product/Architecture Review (Databricks-Focused)

1. **Secrets Management**: [RESOLVED] Databricks Secrets implemented with AWS parameter store integration.
2. **Monitoring**: CloudWatch for metrics/alarms + Databricks job monitoring sufficient, or need additional tooling?
3. **Primary Keys**: [RESOLVED] Automatic discovery from database metadata with optional manual override in sources.yaml. Supports composite keys.
4. **Schema Registry**: Unity Catalog native features sufficient, or need separate schema versioning system?
5. **Exactly-Once**: Is at-least-once acceptable or do we need exactly-once guarantees for CDC?
6. **Idempotency**: Should bulk loads be idempotent (checksum-based) or accept manual re-runs?
7. **Table Priority**: Should certain tables have priority or guaranteed processing order in Databricks jobs?
8. **SLA Requirements**: What are the latency requirements for CDC processing? (affects cluster sizing)
9. **Unity Catalog Strategy**: Single catalog for all environments or separate per-environment (dev/stage/prod)?
10. **Cluster Configuration**: Job clusters (ephemeral) vs all-purpose clusters for CDC streaming?
11. **Cost Optimization**: Spot instances acceptable for non-critical bulk loads?
12. **DR/HA**: Multi-region deployment requirements or single-region with backup strategy sufficient?

---

## Maintenance Schedule (Databricks Operations)

### Weekly
- Review Databricks job run history for failures
- Query Unity Catalog status table for stuck loads
- Monitor Kafka consumer lag via CloudWatch or Kafka tools
- Verify S3/DBFS checkpoint integrity and size

### Monthly
- Review Databricks job logs (auto-retained, no rotation needed)
- Analyze cluster utilization and cost metrics via Databricks UI
- Update Python dependencies and rebuild deployment packages
- Run Iceberg table maintenance (OPTIMIZE, expire_snapshots) on large tables
- Review S3 storage costs and lifecycle policies

### Quarterly
- Audit Databricks Secrets and IAM role permissions
- Performance optimization review (cluster sizing, auto-scaling)
- Architecture review for scaling needs and Unity Catalog governance
- Test disaster recovery procedures (catalog backup, S3 cross-region replication)
- Review Databricks runtime version for updates
- Validate VPC peering and network security configuration

---

## References

### Internal Documentation
- [Databricks Deployment Guide](docs/DATABRICKS_DEPLOYMENT.md)
- [Architecture Documentation](docs/ARCHITECTURE.md)
- [Oracle LONG Limitations](docs/ORACLE_LONG_LIMITATIONS.md)

### External Documentation
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Databricks Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Debezium Documentation](https://debezium.io/documentation/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
