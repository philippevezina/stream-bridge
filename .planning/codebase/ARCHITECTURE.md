# Architecture

**Analysis Date:** 2026-01-21

## Pattern Overview

**Overall:** Event-driven CDC (Change Data Capture) pipeline with real-time MySQL binlog streaming to ClickHouse.

**Key Characteristics:**
- Event-driven architecture with channel-based communication between components
- Worker pool pattern for parallel event processing with per-table ordering guarantees
- Batching strategy to group events for efficient database writes
- Checkpoint-based position persistence for crash recovery
- DDL synchronization barriers to maintain schema consistency between MySQL and ClickHouse
- Pluggable observability layer (Sentry, NewRelic) for error tracking and logging

## Layers

**Data Source Layer (MySQL CDC):**
- Purpose: Capture MySQL binlog events in real-time using binary log replication
- Location: `internal/mysql/cdc.go`, `internal/mysql/connector/connector.go`
- Contains: Binlog syncer, event streaming, position tracking, table filtering
- Depends on: State Manager (for position recovery)
- Used by: Pipeline Processor via event channels

**Schema Layer:**
- Purpose: Maintain schema synchronization between MySQL and ClickHouse, translate DDL statements
- Location: `internal/schema/` (discovery.go, ddl_translator.go, ddl_parser.go, translator.go, cache.go, ddl_cache_updater.go)
- Contains: DDL parsing, MySQL→ClickHouse type mapping, schema caching, schema discovery for snapshots
- Depends on: MySQL connector, ClickHouse client
- Used by: Pipeline Processor for DDL events, Snapshot Manager for initial schema discovery

**Processing Layer (Pipeline):**
- Purpose: Transform MySQL events into ClickHouse operations with batching and retries
- Location: `internal/pipeline/processor.go`
- Contains: Event batching, worker pool, DDL execution, DML batching, table-based event queuing
- Depends on: ClickHouse client, State Manager, Schema components
- Used by: MySQL CDC (via event channels), Application

**Persistence Layer (ClickHouse):**
- Purpose: Write events to ClickHouse with engine-specific optimizations, manage schemas
- Location: `internal/clickhouse/client.go`
- Contains: Connection pooling, bulk INSERT operations, DDL execution, schema caching
- Depends on: ClickHouse driver, schema components
- Used by: Pipeline Processor, State Manager, Snapshot Loader

**State Management Layer:**
- Purpose: Manage checkpoint persistence for position recovery, snapshot progress tracking
- Location: `internal/state/` (manager.go, interface.go, clickhouse.go)
- Contains: Checkpoint lifecycle management, position persistence, snapshot progress tracking
- Depends on: ClickHouse client
- Used by: Application, MySQL CDC, Pipeline Processor, Snapshot Manager

**Snapshot Layer:**
- Purpose: Initial bulk data load from MySQL to ClickHouse with automatic table creation
- Location: `internal/snapshot/` (manager.go, coordinator.go, loader.go, chunked_reader.go, types.go)
- Contains: Chunked data reading, concurrent table loading, progress tracking
- Depends on: MySQL connector, ClickHouse client, Schema discovery, State Manager
- Used by: Application (during startup)

**Observability Layer:**
- Purpose: Error tracking, log exporting, health monitoring
- Location: `internal/observability/` (manager.go, sentry_reporter.go, newrelic_exporter.go, noop.go)
- Contains: Error reporter abstraction, log exporter abstraction, Sentry/NewRelic integrations
- Depends on: Logger (zap)
- Used by: Application error handler, all components for error reporting

**Metrics Layer:**
- Purpose: Collect and expose application metrics via Prometheus
- Location: `internal/metrics/` (metrics.go, prometheus.go)
- Contains: Prometheus metrics registration, HTTP metrics server, gauge/counter implementations
- Depends on: Prometheus client, HTTP server
- Used by: Application for metrics updates

**Configuration Layer:**
- Purpose: Load and manage application configuration from YAML files with environment variable substitution
- Location: `internal/config/config.go`, `internal/config/envsubst.go`
- Contains: Configuration structures for all components, Viper-based loading, environment variable substitution
- Depends on: Viper, os
- Used by: Application at startup

## Data Flow

**Event Processing Flow (DML - INSERT/UPDATE/DELETE):**

1. MySQL CDC reads binlog events from MySQL replication stream
2. Events pass through table filter to determine inclusion/exclusion
3. Events are generated as `common.Event` structs with ID, type, database, table, data
4. Events are sent to Pipeline Processor via `eventChan` (buffered, size = config.pipeline.buffer_size)
5. Application's `runEventForwarder` consumes events, monitors queue for backpressure
6. Events are routed to Pipeline Processor which batches them by table
7. Processor maintains per-table event queues (`tableQueues`) to preserve event ordering within tables
8. Events are batched (size = config.pipeline.batch_size, timeout = config.pipeline.batch_timeout)
9. Batcher forwards batch to round-robin selected worker from worker pool
10. Worker sends batch to ClickHouse via bulk INSERT operation
11. On success, checkpoint is created/updated with MySQL position
12. Metrics are updated (events_processed, processing_rate, queue_length)

**DDL Processing Flow (CREATE/ALTER/DROP):**

1. MySQL CDC captures QueryEvent from binlog (DDL statements)
2. Event is marked as EventTypeDDL
3. DDL Parser extracts statement type and affected table
4. Pipeline Processor receives DDL event
5. Processor flushes all pending DML events for affected table (waits up to config.pipeline.ddl_flush_timeout)
6. Workers complete current batch processing
7. DDL Translator converts MySQL DDL to ClickHouse syntax (type mapping, engine selection)
8. DDL is executed synchronously in ClickHouse
9. Schema cache is updated via DDLCacheUpdater
10. Position checkpoint is created/updated
11. Normal DML processing resumes

**Snapshot Flow (Initial Data Load):**

1. Application checks if snapshot is enabled and if one has already completed
2. If needed, Snapshot Manager starts Schema Discovery (temporary MySQL schema introspection)
3. Coordinator discovers all tables matching table filter from MySQL
4. For each table: ChunkedReader reads data in chunks from MySQL
5. Loader converts MySQL rows to ClickHouse INSERT format
6. If table doesn't exist in ClickHouse, DDL creates it with default engine (ReplacingMergeTree)
7. Data is loaded via bulk INSERT operations
8. Progress is tracked in state storage
9. After all tables complete, Schema Discovery is stopped/disposed
10. From this point forward, schema changes are handled only via CDC DDL events
11. Final checkpoint is created marking snapshot completion

**Checkpoint & Recovery Flow:**

1. Application starts and initializes State Manager
2. State Manager queries ClickHouse for latest checkpoint
3. If checkpoint exists and position is valid in MySQL binlog, MySQL CDC resumes from that position
4. If not found or invalid, MySQL CDC starts from current master position
5. After events are written to ClickHouse, new checkpoint is created every N seconds (config.state.checkpoint_interval)
6. Checkpoint contains: MySQL binlog file + offset + GTID, timestamp, ID
7. On application crash/restart, cycle repeats from step 2

**State Management:**

- **PENDING**: Checkpoint created but not yet processed
- **PROCESSED**: Events written to ClickHouse, waiting for confirmation
- **COMMITTED**: Fully persisted in ClickHouse
- **CLEANUP**: After retention period (config.state.retention_period), old checkpoints are deleted

## Key Abstractions

**Event (`common.Event`):**
- Purpose: Core data unit flowing through pipeline
- Examples: `internal/common/types.go:Event`
- Pattern: Carries binlog position, event type (INSERT/UPDATE/DELETE/DDL), table metadata, before/after data

**BinlogPosition (`common.BinlogPosition`):**
- Purpose: Track MySQL binary log location for recovery
- Examples: `internal/common/types.go:BinlogPosition`
- Pattern: Contains binlog file name, byte offset, optional GTID for reliable positioning

**TableFilter (`common.TableFilter`):**
- Purpose: Determine if events from a specific table should be processed
- Examples: `internal/common/table_filter.go`
- Pattern: Regex-based include/exclude patterns applied at event-processing level, not binlog level

**StateStorage (interface):**
- Purpose: Pluggable position and snapshot persistence backend
- Examples: `internal/state/interface.go:StateStorage`
- Pattern: Supports ClickHouse implementation; File storage planned for future

**Checkpoint (`state.Checkpoint`):**
- Purpose: Persistent record of processed position for recovery
- Examples: `internal/state/interface.go:Checkpoint`
- Pattern: Stored in ClickHouse table `stream_bridge_checkpoints`, tracks MySQL position and timestamp

**ClickHouseDDL (`schema.ClickHouseDDL`):**
- Purpose: Translated DDL statement ready for execution
- Examples: `internal/schema/ddl_translator.go:ClickHouseDDL`
- Pattern: Contains array of ClickHouse statements, metadata, destructiveness flag

**TableInfoCache (interface):**
- Purpose: Cache MySQL table schemas to avoid repeated introspection
- Examples: `internal/clickhouse/client.go:TableInfoCache`
- Pattern: Implemented by schema cache, used by schema discovery and DDL operations

## Entry Points

**Application Main:**
- Location: `cmd/stream-bridge/main.go:main()`
- Triggers: Binary execution with flags (-config, -snapshot, -force-snapshot, -version)
- Responsibilities: Parse CLI flags, load config, initialize all components, start event loop, manage shutdown

**Application.initialize():**
- Location: `cmd/stream-bridge/main.go:initialize()`
- Triggers: Called from run() during application startup
- Responsibilities: Create and wire together all components (MySQL CDC, Processor, ClickHouse, State Manager, Snapshot Manager, Observability)

**Application.start():**
- Location: `cmd/stream-bridge/main.go:start()`
- Triggers: Called after initialize() succeeds
- Responsibilities: Start metrics manager, initialize state manager, run snapshot if enabled, start processor, start MySQL CDC, launch goroutines for event forwarding and metrics updating

**Application.runEventForwarder():**
- Location: `cmd/stream-bridge/main.go:runEventForwarder()`
- Triggers: Goroutine launched from start()
- Responsibilities: Forward events from MySQL CDC to Processor, implement backpressure detection, request shutdown on sustained backpressure

**CDC.processEvents():**
- Location: `internal/mysql/cdc.go:processEvents()`
- Triggers: Goroutine launched from CDC.Start()
- Responsibilities: Read from binlog stream, filter events by table, parse event data, emit Event objects to eventChan

**Processor.processEvents():**
- Location: `internal/pipeline/processor.go:processEvents()`
- Triggers: Goroutine launched from Processor.Start()
- Responsibilities: Consume events from eventChan, batch by table, route batches to workers, handle DDL synchronization

**SnapshotManager.ExecuteSnapshot():**
- Location: `internal/snapshot/manager.go:ExecuteSnapshot()`
- Triggers: Called from Application.start() if snapshot enabled and not previously completed
- Responsibilities: Coordinate discovery, chunked reading, and loading of all tables, track progress, create final checkpoint

## Error Handling

**Strategy:** Errors are logged, counted in metrics, reported to observability system (Sentry), and either retried or gracefully shutdown based on severity.

**Patterns:**

- **Retryable errors** (network timeouts, temporary connection issues): Exponential backoff retry with configurable max attempts
- **Non-retryable errors** (schema mismatches, permission errors): Logged and counted as failed events
- **Backpressure errors** (processor queue full): Applied gradually; if sustained >5 minutes, triggers graceful shutdown
- **DDL errors** (schema conflicts): Recovery attempted via schema validation; if recovery fails, logged and skipped
- **Fatal errors** (startup failures): Application exits with error message
- **Observability integration**: Errors captured via observability.Manager.GetErrorReporter().CaptureError() with context (component, operation, database, table)

## Cross-Cutting Concerns

**Logging:**
- Framework: Zap (uber-go/zap)
- Approach: Structured logging with component-based logger hierarchy (created via common.LoggerWithComponent)
- Levels: DEBUG, INFO, WARN, ERROR
- Output: Configurable (stdout, file) with rotation support (max_size, max_backups, max_age)

**Validation:**
- Table filtering: Regex-based include/exclude patterns applied at event processing level
- Schema consistency: MySQL and ClickHouse schemas validated before DDL execution
- Type mapping: MySQL types translated with nullability and precision preservation options

**Authentication:**
- MySQL: Username/password, optional SSL with multiple modes (disabled, preferred, required, verify_ca, verify_identity)
- ClickHouse: Username/password, optional SSL
- Configuration: Loaded from YAML with environment variable substitution (${VAR_NAME} syntax)

**Synchronization Primitives:**
- Per-component: RWMutex for state protection (running flag, current checkpoint, etc.)
- Per-table: Table-based event queues ensure ordering within tables while allowing parallelism across tables
- Worker pool: Round-robin distribution with per-worker batch channels
- Batch coordination: sync.WaitGroup per batch to track completion and enable flushing

---

*Architecture analysis: 2026-01-21*
