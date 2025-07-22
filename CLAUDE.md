# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building and Testing

```bash
# Build all packages
go build ./...

# Build main application binary
go build -o stream-bridge cmd/stream-bridge/main.go

# Run tests
go test ./...

# Build with race detection
go build -race ./...

# Clean up dependencies
go mod tidy
```

### Running the Application

```bash
# Run with default configuration
./stream-bridge -config configs/example.yaml

# Run with minimal configuration for development
./stream-bridge -config configs/minimal.yaml

# Show version
./stream-bridge -version
```

### Configuration Management

- Use `configs/minimal.yaml` for local development
- Use `configs/example.yaml` as reference for production settings
- Configuration changes require application restart

## Core Architecture

### Event Flow Architecture

The application follows an event-driven pipeline architecture:

1. **MySQL CDC** (`internal/mysql/cdc.go`) - Reads from MySQL binlog stream using `go-mysql-org/go-mysql`
2. **DDL Processing** (`internal/pipeline/processor.go`) - Processes DDL events from binlog to keep ClickHouse schemas synchronized
3. **Event Pipeline** (`internal/pipeline/processor.go`) - Batches and processes DML events with worker pools
4. **ClickHouse Writer** (`internal/clickhouse/client.go`) - Writes to ClickHouse with engine-specific optimizations
5. **State Manager** (`internal/state/`) - Manages checkpoint persistence for position recovery

### Position Persistence System

Critical for production reliability - the system creates checkpoints to survive restarts:

- **Checkpoints** are stored in ClickHouse table `stream_bridge_checkpoints`
- **Recovery** happens automatically on startup from last committed checkpoint
- **Status flow**: PENDING → PROCESSED → COMMITTED → (cleanup after retention period)
- **Configuration** via `state` section in YAML config

### Component Dependencies

```
Application
├── StateManager (must initialize first)
├── ClickHouseClient
├── Pipeline (depends on StateManager + ClickHouseClient + DDL Translator)
├── MySQL CDC (depends on StateManager)
└── MetricsManager
```

### Key Data Structures

- `common.Event` - Core event type flowing through the pipeline
- `common.BinlogPosition` - MySQL position tracking (file + offset + GTID)
- `state.Checkpoint` - Persistence checkpoint with status and metadata
- `config.Config` - Complete application configuration structure

## Configuration Architecture

### Configuration Hierarchy

The system uses Viper with these precedence levels:

1. Explicit configuration file (via `-config` flag)
2. Built-in defaults (see `internal/config/config.go:setDefaults()`)

### Critical Configuration Sections

- **mysql.table_filter** - Uses regex patterns for include/exclude table filtering
- **pipeline.batch_size** and **pipeline.batch_timeout** - Control batching behavior
- **pipeline.ddl_flush_timeout** - Maximum time to wait for active batches before DDL execution (default: 1m)
- **state.checkpoint_interval** - How often to persist position (default 30s)
- **clickhouse.addresses** - Supports multiple ClickHouse nodes
- **schema.default_engine** - ClickHouse table engine for DDL-created tables (default: ReplacingMergeTree)
- **snapshot.create_missing_tables** - Create missing ClickHouse tables during snapshot (enabled by default)

## MySQL CDC Implementation Details

### Binlog Processing and DDL Synchronization

- Requires MySQL binlog format = ROW for detailed change capture
- Processes `RowsEvent` (INSERT/UPDATE/DELETE) and `QueryEvent` (DDL)
- **DDL events use synchronization barriers** to ensure proper ordering with DML events
- **DDL Synchronization Process**:
  1. DDL events are detected and routed to dedicated processing channel
  2. All pending DML events for the affected table are flushed
  3. Workers complete processing current batches (with timeout)
  4. DDL is executed synchronously
  5. Normal DML processing resumes
- **DDL events are automatically translated and executed in ClickHouse** to keep schemas synchronized
- Position tracking updates after each processed event
- Table filtering happens at event processing level, not binlog level

### Position Management

- MySQL position = (binlog file, offset, optional GTID)
- Position persistence prevents data loss/duplication on restart
- Recovery logic: try checkpoint → fallback to current MySQL master position

## ClickHouse Integration

### Schema Translation

- Automatic MySQL → ClickHouse type mapping in `mysqlToClickHouseType()`
- Supports multiple table engines: MergeTree, ReplacingMergeTree, etc.
- UPDATE/DELETE handling varies by engine type

### Batch Operations

- INSERT operations use bulk loading for performance
- UPDATE/DELETE may use ALTER TABLE operations or ReplacingMergeTree patterns
- Engine detection determines optimal operation strategy

## Schema Synchronization Strategy

### CDC-Based DDL Processing

The application maintains ClickHouse schema synchronization through real-time DDL event processing:

- **DDL Capture**: MySQL DDL statements (CREATE, ALTER, DROP) are captured from binlog events
- **Translation**: MySQL DDL is automatically translated to ClickHouse-compatible syntax
- **Execution**: Translated DDL is executed immediately in ClickHouse to maintain schema parity
- **No Polling**: No periodic schema discovery or synchronization - changes happen in real-time

### Snapshot Table Creation

- **Initial Setup**: During snapshots, missing ClickHouse tables are created automatically
- **Schema Discovery**: Schema introspection is used only during snapshot execution
- **Post-Snapshot**: After snapshot completion, schema discovery is stopped and DDL processing takes over

### Supported DDL Operations

- **CREATE TABLE**: Full table creation with type mapping and engine selection
- **ALTER TABLE**: ADD COLUMN, DROP COLUMN, MODIFY COLUMN operations
- **DROP TABLE**: Table removal (marked as destructive operation)
- **Engine Support**: Only ReplacingMergeTree and ReplicatedReplacingMergeTree engines
- **DDL Ordering**: DDL events are processed with synchronization barriers to prevent race conditions with DML events

## State Management Deep Dive

### Checkpoint Lifecycle

1. **Creation** - Triggered by time interval or event count threshold
2. **Processing** - Marked when events batch successfully written to ClickHouse
3. **Commitment** - Marked when operation fully confirmed
4. **Cleanup** - Old checkpoints removed based on retention policy

### Storage Interface

The `StateStorage` interface allows pluggable backends:

- **ClickHouse** (production) - Uses ReplacingMergeTree for atomic updates
- **File** (planned) - For simpler deployments

### Recovery Strategy

- Always attempt recovery from last COMMITTED or PROCESSED checkpoint
- Validate recovered position still exists in MySQL binlog
- Graceful degradation to current position if checkpoint invalid

## Monitoring and Observability

### Key Metrics to Monitor

- `stream_bridge_replication_lag_seconds` - Primary health indicator
- `stream_bridge_events_failed_total` - Error rate tracking
- `stream_bridge_checkpoint_age_seconds` - Position persistence health
- `stream_bridge_connection_status` - Database connectivity

### Debugging Event Flow

1. Check MySQL CDC connection and position via logs
2. Monitor pipeline queue length and worker processing
3. Verify ClickHouse write success rates
4. Examine checkpoint creation frequency and success

## Code Analysis and Refactoring

### AST-Based Code Analysis

When performing code analysis, refactoring, or pattern matching tasks, use `ast-grep` when it could be more effective than regex-based tools:

- **Structural searches**: Finding function definitions, method calls, or specific code patterns
- **Refactoring**: Renaming symbols, updating function signatures, or modifying code structures
- **Code quality**: Identifying anti-patterns, unused code, or inconsistent patterns
- **Go-specific searches**: Finding struct definitions, interface implementations, or goroutine usage

Use `ast-grep` for precise structural matching when regex would be fragile or insufficient for Go code analysis.

## Testing Approach

### Component Testing

- Test each major component (`mysql/`, `clickhouse/`, `pipeline/`, `state/`) independently
- Mock interfaces for isolation (especially database connections)
- Focus on error handling and retry logic

### Integration Testing

- Requires running MySQL and ClickHouse instances
- Test full event flow: binlog → DDL processing → ClickHouse write → checkpoint
- Test DDL synchronization: MySQL schema changes automatically applied to ClickHouse
- Verify position recovery across application restarts
- Test snapshot functionality with automatic table creation
