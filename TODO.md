# ETL Project TODO

## Status Legend
- [DONE] Completed
- [WIP] In Progress
- [TODO] Pending
- [BLOCKED] Blocked

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
- [TODO] Consider integration with secrets management (AWS Secrets Manager, HashiCorp Vault)

### Data Type Issues
- [DONE] Change oracle_scn from IntegerType to LongType

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
- [TODO] Make hardcoded paths configurable
  - `/opt/pipeline/lib` → config parameter
  - `/opt/data/lakehouse/warehouse` → config parameter
  - `/opt/pipeline/logs` → config parameter
  - `/opt/pipeline/checkpoints` → config parameter
  - **Files**: Multiple

### Error Handling
- [TODO] Replace bare exception handlers with specific types
  - **File**: `lib/status_tracker.py:55,145,212`
  - **File**: `jobs/cdc_kafka_to_iceberg.py:151`
  - **File**: `jobs/direct_bulk_load.py:176`

### Code Quality
- [TODO] Refactor status_tracker.py to eliminate schema duplication
  - Extract schema definition to single method
  - Extract MERGE logic to single method
  - Reduce 350+ lines to ~200 lines

### Monitoring
- [TODO] Add metrics/monitoring hooks
  - Emit metrics for records processed
  - Track table processing duration
  - Alert on failures
  - Integration with Prometheus/CloudWatch/Datadog

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
- [TODO] Add integration tests
  - End-to-end bulk load test
  - CDC event processing test
  - Failure/retry scenarios
  - Schema evolution scenarios

- [TODO] Add unit tests
  - Extractor logic
  - Schema fixer logic
  - Config loader with env vars
  - Status tracker operations

### Architecture
- [TODO] Use Iceberg's built-in MERGE operations
  - Replace custom merge logic with Iceberg native operations
  - Leverage Iceberg's ACID guarantees

- [TODO] Implement proper transaction semantics
  - Atomic table writes
  - Coordinated status updates
  - Rollback on failure

- [TODO] Add schema registry (Confluent Schema Registry or AWS Glue)
  - Centralized schema management
  - Version control for schemas
  - Compatibility enforcement

---

## Documentation

### Completed
- [DONE] Architecture documentation
- [DONE] README with setup instructions
- [DONE] TODO tracking (this file)

### Pending
- [TODO] Add inline documentation for complex algorithms
- [TODO] Create troubleshooting guide
- [TODO] Document configuration parameters
- [TODO] Create runbook for common operations
- [TODO] Add examples for common use cases

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

### Current Constraints
1. **Single Point of Failure**: Status tracking in single Iceberg table
2. **No Distributed Locking**: Parallel runs could conflict
3. **Limited Retry Logic**: Simple exponential backoff, no circuit breaker
4. **Kafka Offset Management**: Checkpoint deletion causes full replay
5. **Primary Key Discovery**: Relies on ROW_ID convention or guessing
6. **Schema Changes**: No automatic handling of DDL changes
7. **Large Transactions**: No batching for very large CDC transactions

### Workarounds
- Run single instance per source (prevent conflicts)
- Backup checkpoint directories regularly
- Define primary keys explicitly in future version
- Monitor for schema changes manually
- Process large tables separately if needed

---

## Questions for Product/Architecture Review

1. **Secrets Management**: Which system should we integrate with? (AWS Secrets Manager, HashiCorp Vault, other)
2. **Monitoring**: Which monitoring system is standard? (Prometheus, CloudWatch, Datadog, other)
3. **Primary Keys**: Can we enforce a convention or need flexible configuration?
4. **Schema Registry**: Do we need Confluent Schema Registry or is AWS Glue sufficient?
5. **Exactly-Once**: Is at-least-once acceptable or do we need exactly-once guarantees?
6. **Idempotency**: Should bulk loads be idempotent or is manual intervention acceptable?
7. **Table Priority**: Should certain tables have priority or guaranteed processing order?
8. **SLA Requirements**: What are the latency requirements for CDC processing?

---

## Maintenance Schedule

### Weekly
- Review failed table logs
- Check status table for stuck loads
- Monitor Kafka consumer lag
- Validate checkpoint integrity

### Monthly
- Review and rotate logs
- Analyze performance metrics
- Update dependencies
- Review and archive old Iceberg snapshots

### Quarterly
- Security audit of credentials management
- Performance optimization review
- Architecture review for scaling needs
- Disaster recovery drill

---

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Debezium Documentation](https://debezium.io/documentation/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Kafka Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
