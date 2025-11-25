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

### Documentation
- [DONE] Create `docs/ORACLE_LONG_LIMITATIONS.md` - Oracle LONG column handling documentation
- [DONE] Update all documentation with Databricks deployment information

---

## Short Term (Next Sprint)

### Primary Key Handling
- [TODO] Add explicit primary key configuration per table in sources.yaml
  - Remove ROW_ID assumption in CDC consumer
  - Support composite primary keys
  - Validate PK exists before processing CDC events
  - **File**: `jobs/cdc_kafka_to_iceberg.py:213,227`

### Idempotency
- [TODO] Implement idempotency checks for bulk load
  - Add checksum/hash column to track data versions
  - Prevent duplicate loads of same data
  - **File**: `jobs/direct_bulk_load.py`

### Configuration
- [TODO] Remove hardcoded local paths, use Databricks-compatible locations
  - Replace `/opt/pipeline/lib` with workspace paths or Python package imports
  - Warehouse path from WAREHOUSE_PATH environment variable (S3 or DBFS)
  - Logs written to Databricks driver logs (automatically captured)
  - Checkpoints to DBFS or S3 (configured via environment variable)
  - Ensure all paths support cloud storage (s3://, dbfs://)
  - **Files**: Multiple

### Error Handling
- [TODO] Replace bare exception handlers with specific types
  - **File**: `lib/status_tracker.py:55,145,212`
  - **File**: `jobs/cdc_kafka_to_iceberg.py:151`
  - **File**: `jobs/direct_bulk_load.py:176`

### Code Quality
- [DONE] Refactor duplicate code across job scripts into utility modules
  - Created SparkSessionFactory, DatabaseConnectionBuilder, IcebergTableManager
  - Eliminated ~162 lines of duplication
- [TODO] Refactor status_tracker.py to eliminate schema duplication
  - Extract schema definition to single method
  - Extract MERGE logic to single method
  - Reduce 350+ lines to ~200 lines

### Monitoring
- [TODO] Add metrics/monitoring hooks for Databricks
  - Emit CloudWatch metrics for records processed (AWS SDK)
  - Track table processing duration in job logs
  - Alert on failures via CloudWatch Alarms or SNS
  - Use Databricks job run metrics and monitoring APIs
  - Log structured events for downstream analysis
  - Consider Databricks SQL Analytics for dashboards

### CDC Position Tracking
- [TODO] Improve CDC position tracking granularity
  - Current: Updates every 100 events (risk of loss on crash)
  - Target: Update every 10 events or use Spark checkpointing more effectively
  - **File**: `jobs/cdc_kafka_to_iceberg.py:380`

---

## Long Term (Future Enhancements)

### Schema Evolution
- [TODO] Implement schema registry integration
  - Detect schema changes between bulk load and CDC
  - Handle column additions/deletions/type changes
  - Validate compatibility before applying changes

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

- [TODO] Add schema registry integration (AWS Glue preferred for Databricks on AWS)
  - Centralized schema management in AWS Glue Data Catalog
  - Version control for schemas
  - Compatibility enforcement
  - Unity Catalog as primary metadata store

---

## Documentation

### Completed
- [DONE] Architecture documentation (docs/ARCHITECTURE.md)
- [DONE] README with setup instructions
- [DONE] TODO tracking (this file)
- [DONE] Databricks deployment guide (docs/DATABRICKS_DEPLOYMENT.md)
- [DONE] Oracle LONG limitations documentation (docs/ORACLE_LONG_LIMITATIONS.md)
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
5. **Primary Key Discovery**: Relies on ROW_ID convention or guessing
6. **Schema Changes**: No automatic handling of DDL changes
7. **Large Transactions**: No batching for very large CDC transactions
8. **Cross-Region**: No multi-region Unity Catalog support in design

### Workarounds (Databricks Context)
- Use Databricks job concurrency limits (max concurrent runs = 1 per source)
- Store checkpoints in S3 with versioning enabled
- Define primary keys explicitly in sources.yaml (future enhancement)
- Use Unity Catalog's schema evolution features where possible
- Monitor for schema changes using AWS Glue or Unity Catalog audit logs
- Split large tables using table-filter parameter in separate jobs

---

## Questions for Product/Architecture Review (Databricks-Focused)

1. **Secrets Management**: [RESOLVED] Databricks Secrets implemented with AWS parameter store integration.
2. **Monitoring**: CloudWatch for metrics/alarms + Databricks job monitoring sufficient, or need additional tooling?
3. **Primary Keys**: Can we enforce a convention or need flexible per-table configuration in sources.yaml?
4. **Schema Registry**: AWS Glue Data Catalog for schema management, or separate schema registry needed?
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
