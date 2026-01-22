# Codebase Concerns

**Analysis Date:** 2026-01-21

## Tech Debt

**Complex Processor State Management:**
- Issue: `internal/pipeline/processor.go` is 1649 lines with complex synchronization of worker pools, batch state, failed event queues, and table-specific event ordering. Multiple layers of mutexes and WaitGroups create risk of deadlocks.
- Files: `internal/pipeline/processor.go`
- Impact: Difficult to debug race conditions, potential for missed edge cases in concurrent event processing, hard to extend with new features without breaking synchronization invariants.
- Fix approach: Consider splitting into smaller focused components (TableWorkerPool, FailedBatchManager, BatcherWithRetry) with clearer responsibilities. Add comprehensive integration tests for concurrent scenarios.

**GTID Extraction Gap in Snapshot:**
- Issue: Snapshot coordinator extracts binlog position from replication channel info but GTIDs are not being captured (TODO at line 441 in `internal/snapshot/coordinator.go`).
- Files: `internal/snapshot/coordinator.go:441`
- Impact: GTID-based recovery is not fully implemented. Systems that rely on GTID positioning may lose continuity or have gaps after snapshot completion.
- Fix approach: Query MySQL performance_schema or binlog metadata tables to extract actual GTID set at snapshot time and include in position recovery.

**Schema Cache Invalidation on DDL Failure:**
- Issue: DDL failures trigger cache invalidation (`internal/pipeline/processor.go:1088`) which forces next schema access to query database directly. This creates temporary inconsistency window between cache miss and DB query.
- Files: `internal/pipeline/processor.go:1080-1091`
- Impact: During DDL failures, schema queries become expensive, and if DB query also fails, the system has no cached schema to fall back to.
- Fix approach: Implement a versioned schema cache with fallback chain (cache → DB query → previous version → error). Log when fallback chain is exhausted.

**Failed Event Batch Timeout with Max Retries:**
- Issue: Failed batches have max retry limit of 10 (line 701 in `internal/pipeline/processor.go`) but no clear visibility into which events are permanently lost after max retries.
- Files: `internal/pipeline/processor.go:701-738`
- Impact: Events dropped silently after 10 retries. Operator may not realize data loss is occurring. Retry count is hardcoded (not configurable).
- Fix approach: Make max retries configurable. Add detailed logging/metrics when events are dropped. Consider dead-letter queue for failed events.

## Known Bugs

**Table Unlock Defer in Snapshot:**
- Symptoms: If `FLUSH TABLES WITH READ LOCK` succeeds but operations before unlock fail, global lock could be held indefinitely.
- Files: `internal/snapshot/coordinator.go:446-471` (fixed in fa0efb2)
- Trigger: Failure between `flushTablesWithReadLock()` and `unlockTables()` calls
- Workaround: Was fixed - now uses defer for table unlock to ensure cleanup

**Missing Binlog Metadata Causes Silent Data Loss:**
- Symptoms: If binlog has missing metadata (certain MySQL versions), rows are skipped instead of failing.
- Files: `internal/mysql/cdc.go`
- Trigger: Occurs on MySQL versions with incomplete binlog_row_metadata support
- Workaround: Fixed in commit 6016db1 - now stops CDC on missing binlog metadata instead of silently skipping rows

**Sustained Backpressure Data Loss:**
- Symptoms: Previously, if processor queue stayed full for 30 seconds, events were dropped to prevent channel send blocking.
- Files: `cmd/stream-bridge/main.go:531-575`
- Trigger: High event rate exceeding processor capacity
- Workaround: Fixed in commit 1f41d97 - now initiates graceful shutdown after 5 minutes of sustained backpressure, allowing recovery on restart

## Security Considerations

**SQL Injection via Identifier Escaping:**
- Risk: Identifiers (database/table names) must be validated and escaped before use in SQL queries. Failure allows SQL injection.
- Files: `internal/clickhouse/client.go:214-226`, `internal/state/clickhouse.go:57-102`, `internal/schema/ddl_translator.go` (partial)
- Current mitigation: `internal/security/identifier.go` validates identifiers with whitelist pattern and escapes with backticks.
- Recommendations: Audit all places where database.table names are used in queries to ensure security module is called. Add tests for malicious identifiers.

**DDL Statement Injection Risk:**
- Risk: DDL statements parsed from binlog events could be modified or crafted to cause unintended schema changes.
- Files: `internal/schema/ddl_parser.go`, `internal/schema/ddl_translator.go`
- Current mitigation: Configuration flag `allow_destructive_ddl` blocks DROP TABLE/DROP COLUMN operations.
- Recommendations: Add statement size limits to prevent buffer overflow attacks. Validate that translated DDL matches intent of original MySQL DDL.

**Credentials in Logs:**
- Risk: Database passwords could be logged in connection strings or error messages.
- Files: `cmd/stream-bridge/main.go`, `internal/mysql/cdc.go`, `internal/clickhouse/client.go`
- Current mitigation: Passwords not included in log messages; only host/port/username logged.
- Recommendations: Add integration tests that verify passwords are never logged, even in error cases.

## Performance Bottlenecks

**Single-Threaded DDL Processing:**
- Problem: DDL events are processed synchronously by table workers, blocking other events for that table while DDL executes.
- Files: `internal/pipeline/processor.go:1572-1605`
- Cause: DDL must wait for all pending batches to complete (line 1580) which can take minutes, during which that table's event queue is blocked.
- Improvement path: Implement DDL execution pipeline with separate DDL worker pool. Use version vectors to track which DML events can proceed safely before DDL completion.

**Event Grouping Overhead:**
- Problem: Events are grouped by type and table before processing (line 794 in `internal/pipeline/processor.go`), creating additional GC pressure and memory copies.
- Files: `internal/pipeline/processor.go:794-837`
- Cause: Grouping happens in worker goroutine after batch is already grouped by table, redundant grouping.
- Improvement path: Move grouping into batcher or combine with table routing to eliminate duplicate grouping.

**Schema Cache Query Per Event:**
- Problem: Each UPDATE/DELETE operation queries cache for table schema to determine engine type and handling strategy.
- Files: `internal/clickhouse/client.go`, `internal/pipeline/processor.go`
- Cause: No local schema context cached with worker or batch, must look up per operation.
- Improvement path: Pass table schema with event through pipeline, cache at worker level during batch processing.

**Channel Send Timeout Inefficiency:**
- Problem: When worker channels are full, batcher tries each worker with select/default, adding retry loop overhead.
- Files: `internal/pipeline/processor.go:589-614`
- Cause: Non-blocking sends to all workers in loop during high backpressure.
- Improvement path: Use worker load tracking to select least-loaded worker upfront, reducing retry iterations.

## Fragile Areas

**Checkpoint Creation Validation:**
- Files: `internal/pipeline/processor.go:1234-1283`
- Why fragile: Checkpoint validation happens after DDL failure with multiple fallible steps (table existence check → schema validation → checkpoint creation). If any validation step is flaky, checkpoint may be created in inconsistent state.
- Safe modification: Add defensive logging at each validation step. Consider making validation more lenient (skip unavailable validations) rather than blocking checkpoint. Add metrics for validation success/failure rates.
- Test coverage: Needs integration tests simulating partial DDL failures with concurrent DML events.

**Snapshot Resume Logic:**
- Files: `internal/snapshot/manager.go:87-101`, `internal/snapshot/coordinator.go`
- Why fragile: Resume-on-failure assumes snapshot progress table is consistent. If progress table is corrupted or has orphaned rows, resume may fail or resume from wrong state.
- Safe modification: Add data integrity checks before resuming (validate snapshot_id exists, table list is consistent, progress counts match). Add manual recovery procedures in documentation.
- Test coverage: Needs tests for corrupted/incomplete snapshot progress states.

**Table Filter Configuration:**
- Files: `internal/config/config.go:61-66`, `internal/common/table_filter.go`
- Why fragile: Table filters use regex patterns that must match between MySQL CDC and DDL processing. If filter is accidentally changed mid-operation, some events may be dropped while table still exists in ClickHouse.
- Safe modification: Add filter validation at startup. Warn if filter differs from last run. Log filter decisions for critical tables. Add dry-run mode to test filter patterns.
- Test coverage: Add unit tests for filter edge cases (empty patterns, overlapping include/exclude).

**DDL Transaction Idempotency:**
- Files: `internal/pipeline/processor.go:940-1124`
- Why fragile: Relies on generated DDL being idempotent (CREATE TABLE IF NOT EXISTS), but translation may produce non-idempotent statements in edge cases.
- Safe modification: Validate all DDL statement templates produce idempotent SQL. Add integration tests that apply same DDL twice and verify no errors on second attempt.
- Test coverage: Comprehensive DDL idempotency tests for CREATE/ALTER/DROP operations.

## Scaling Limits

**Event Channel Buffer Exhaustion:**
- Current capacity: Configured via `pipeline.buffer_size` (typical: 1000-10000)
- Limit: System cannot process events faster than workers consume them. If producer rate > consumer rate, channel fills and system initiates shutdown.
- Scaling path: Increase worker count and batch processing throughput before increasing channel size (large channels mask problems). Consider implementing priority queues for critical tables.

**Worker Pool Saturation:**
- Current capacity: Configured via `pipeline.worker_count` (typical: 4-16)
- Limit: Each worker processes batches sequentially. If batch processing time exceeds inter-batch interval, workers queue up.
- Scaling path: Increase worker count, optimize batch processing (reduce ClickHouse insert time), or use distributed workers across multiple nodes.

**ClickHouse Connection Pool:**
- Current capacity: Configured via `clickhouse.max_open_conns` (typical: 25-100)
- Limit: If all connections are occupied by long-running queries, new batches cannot execute.
- Scaling path: Monitor connection usage metrics. Tune ClickHouse query timeout. Consider connection pooling middleware.

**Checkpoint Persistence Frequency:**
- Current interval: Configured via `state.checkpoint_interval` (typical: 30s)
- Limit: Too frequent checkpoints create ClickHouse write load. Too infrequent leads to data loss on crash.
- Scaling path: Implement adaptive checkpoint intervals based on event rate and lag. Consider batch checkpoints (checkpoint only when batch completes).

**Snapshot Parallel Table Loading:**
- Current capacity: Configured via `snapshot.parallel_tables` (typical: 4-8)
- Limit: Too many parallel snapshots can overload MySQL and ClickHouse. Too few makes snapshot very slow.
- Scaling path: Implement adaptive parallelism based on source/target CPU/disk metrics. Add per-table bandwidth limits.

## Dependencies at Risk

**go-mysql-org/go-mysql Library:**
- Risk: Binlog parsing library is community-maintained with sporadic updates. No active maintenance visible in past 12 months.
- Impact: Security vulnerabilities in binlog parsing could cause crashes or data corruption.
- Migration plan: Monitor for maintenance gaps. Have fallback to hand-written binlog parser if library becomes unmaintained. Consider vendoring critical version.

**ClickHouse Driver (clickhouse-go):**
- Risk: Major version updates may introduce breaking changes. Connection pool behavior differs between versions.
- Impact: Upgrade breaks compatibility. Connection exhaustion issues may regress.
- Migration plan: Lock to specific minor version in go.mod. Test upgrade path before adopting new major version. Monitor driver issues and apply patches quickly.

**Viper Configuration Library:**
- Risk: Not actively maintained, known issues with environment variable substitution.
- Impact: Configuration loading may fail in edge cases. Custom envsubst implementation adds complexity.
- Migration plan: Consider moving to simpler configuration library or YAML parser. Remove custom envsubst workaround if Viper is replaced.

## Missing Critical Features

**No Dead-Letter Queue for Failed Events:**
- Problem: Events that exceed max retries are discarded. No way to recover or analyze failed events.
- Blocks: Cannot implement failure analysis, recovery procedures, or alerting on specific problematic events.
- Implementation: Add DLQ table in ClickHouse with failed event metadata. Log DLQ insertions for alerts.

**No Event Deduplication:**
- Problem: If checkpoint recovery happens at partial batch boundary, events may be processed twice creating duplicates.
- Blocks: Cannot guarantee exactly-once semantics in all failure scenarios.
- Implementation: Add event deduplication window using event hash or natural primary key matching.

**No Schema Versioning:**
- Problem: If schema changes (columns added/removed), older events cannot be reconciled with new schema.
- Blocks: Cannot safely handle long-running snapshots during active schema changes.
- Implementation: Tag events with schema version. Track schema versions with timestamps. Support multiple schema versions in ClickHouse.

**No Dynamic Filter Reconfiguration:**
- Problem: Changing table filters requires application restart, causing event loss.
- Blocks: Cannot adjust filtering in production without downtime.
- Implementation: Add configuration reload endpoint that validates and applies new filters. Queue events during transition.

**No Cross-Database Joins for State:**
- Problem: State manager assumes ClickHouse can connect to configuration database. No way to use external state store.
- Blocks: Cannot use cloud state services or separate state infrastructure.
- Implementation: Abstract state storage to support PostgreSQL, Redis, or cloud services.

## Test Coverage Gaps

**No Concurrent DDL + DML Tests:**
- What's not tested: Behavior when DDL executes while DML batches are being processed for same table.
- Files: `internal/pipeline/processor.go:1572-1635`, `internal/snapshot/coordinator.go`
- Risk: Race conditions between DDL synchronization barrier and DML batch processing could cause schema mismatches or data loss.
- Priority: High - this is a critical code path for data consistency.

**No Large Batch Processing Tests:**
- What's not tested: Processor behavior with very large batches (10MB+) that exceed memory limits.
- Files: `internal/pipeline/processor.go:794-820`
- Risk: Memory exhaustion or OOM killer could crash application without clean shutdown.
- Priority: High - production workloads can have large batches.

**No Sustained Backpressure Tests:**
- What's not tested: Full shutdown sequence when sustained backpressure (full queue for 5+ minutes) is detected.
- Files: `cmd/stream-bridge/main.go:531-575`
- Risk: Shutdown timeout (30s) may not be sufficient to drain pending work, losing events.
- Priority: Medium - recent fix needs verification that graceful shutdown actually works under extreme backpressure.

**No Checkpoint Recovery Boundary Tests:**
- What's not tested: Recovery at exact batch/event boundaries, especially with partial checkpoint states.
- Files: `internal/state/clickhouse.go`, `internal/mysql/cdc.go:194-210`
- Risk: Recovery could skip or duplicate events at checkpoint boundaries.
- Priority: High - data integrity depends on this.

**No Snapshot Corruption Handling Tests:**
- What's not tested: Snapshot behavior when snapshot progress table is corrupted or has orphaned records.
- Files: `internal/snapshot/manager.go:127-169`
- Risk: Corrupted progress can cause invalid resume or hang during snapshot.
- Priority: Medium - operational resilience issue.

---

*Concerns audit: 2026-01-21*
