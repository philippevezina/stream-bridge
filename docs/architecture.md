# Architecture

This document describes the internal architecture and event flow of Stream Bridge.

## Overview

Stream Bridge is a CDC (Change Data Capture) replication system that reads MySQL binary logs and applies changes to ClickHouse in real-time. The system is designed for:

- **Reliability**: Checkpoint-based recovery ensures no data loss on crashes
- **Performance**: Batching and worker pools maximize throughput
- **Consistency**: DDL synchronization barriers maintain schema integrity
- **Ordering**: Per-table event queues preserve binlog order

## Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Stream Bridge                               │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │  MySQL CDC   │───▶│   Pipeline   │───▶│  ClickHouse Client   │   │
│  │              │    │              │    │                      │   │
│  │ - Binlog     │    │ - Router     │    │ - Schema cache       │   │
│  │ - Position   │    │ - Batcher    │    │ - Batch writer       │   │
│  │ - Events     │    │ - Workers    │    │ - DDL executor       │   │
│  └──────────────┘    └──────────────┘    └──────────────────────┘   │
│         │                   │                       │               │
│         │                   │                       │               │
│         ▼                   ▼                       ▼               │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    State Manager                            │    │
│  │                                                             │    │
│  │  - Checkpoint creation          - Position recovery         │    │
│  │  - ClickHouse persistence       - Crash recovery            │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### MySQL CDC Engine

**Location**: `internal/mysql/cdc.go`

The CDC engine connects to MySQL's binary log stream and captures row-level changes:

- Establishes a replication connection using the MySQL replication protocol
- Reads binlog events (INSERT, UPDATE, DELETE, DDL)
- Converts raw binlog events into `common.Event` structs
- Tracks binlog position for checkpoint creation
- Handles connection failures with automatic reconnection

### Event Pipeline

**Location**: `internal/pipeline/processor.go`

The pipeline processes events through several stages:

- **Router**: Separates DML events (batched) from DDL events (synchronous)
- **Batcher**: Accumulates events per table for efficient bulk operations
- **Workers**: Process batches concurrently with configurable parallelism
- **DDL Handler**: Executes schema changes with synchronization barriers

### ClickHouse Client

**Location**: `internal/clickhouse/client.go`

Handles all ClickHouse operations:

- Maintains a schema cache for type mapping
- Executes batch INSERT operations
- Handles UPDATE/DELETE via ReplacingMergeTree patterns
- Executes DDL statements for schema synchronization

### State Manager

**Location**: `internal/state/`

Manages checkpoint persistence for crash recovery:

- Creates checkpoints at configurable intervals
- Stores checkpoints in ClickHouse with TTL-based cleanup
- Recovers position on startup from the last checkpoint

## Event Flow

### High-Level Flow

```
MySQL Binlog → CDC Engine → Event Router → Batcher/DDL Handler → Workers → ClickHouse
                                                    │
                                            Checkpoint Manager
```

### Detailed Flow

#### 1. Binlog Capture

The CDC engine reads from MySQL's binlog stream:

```
MySQL Server
     │
     │ Binlog Stream (ROW format)
     ▼
┌─────────────────────────────────┐
│ BinlogSyncer                    │
│                                 │
│ - WRITE_ROWS_EVENT  → INSERT    │
│ - UPDATE_ROWS_EVENT → UPDATE    │
│ - DELETE_ROWS_EVENT → DELETE    │
│ - QUERY_EVENT       → DDL       │
└─────────────────────────────────┘
     │
     │ common.Event
     ▼
   eventChan (buffered)
```

On startup, the CDC engine:
1. Queries the State Manager for the last checkpoint
2. If found, resumes from that binlog position
3. If not found, starts from the current MySQL position

#### 2. Event Routing

Events are routed based on type:

```
eventChan
     │
     ▼
┌─────────────────────────────────┐
│ Event Router                    │
│                                 │
│ if event.Type == DDL:           │
│   → Table Worker (synchronous)  │
│ else:                           │
│   → Batcher (for batching)      │
└─────────────────────────────────┘
```

This separation ensures DDL events are processed synchronously while DML events benefit from batching.

#### 3. Batching

DML events are batched per table for efficiency:

```
┌─────────────────────────────────────────────────────┐
│ Batcher                                             │
│                                                     │
│ buffer["db.users"] = [event1, event2, event3, ...]  │
│ buffer["db.orders"] = [event1, event2, ...]         │
│                                                     │
│ Flush triggers:                                     │
│ - Batch size reached (default: 500)                 │
│ - Batch timeout elapsed (default: 2s)               │
│ - DDL event requires flush                          │
│ - Checkpoint creation                               │
└─────────────────────────────────────────────────────┘
     │
     │ Batch of events
     ▼
Worker Pool (round-robin distribution)
```

#### 4. DDL Synchronization

DDL events require special handling to maintain consistency:

```
DDL Event (e.g., ALTER TABLE users ADD COLUMN)
     │
     ▼
┌─────────────────────────────────────────────────────┐
│ DDL Synchronization Barrier                         │
│                                                     │
│ 1. Stop accepting new DML for this table            │
│ 2. Flush all pending batches for this table         │
│ 3. Wait for in-flight batches to complete           │
│    (timeout: ddl_flush_timeout, default 1m)         │
│ 4. Execute DDL in ClickHouse                        │
│ 5. Update schema cache                              │
│ 6. Resume DML processing                            │
└─────────────────────────────────────────────────────┘
```

This barrier prevents DDL from executing while older DML events are still in flight, which would cause data corruption.

#### 5. Worker Processing

Workers receive batches and write to ClickHouse:

```
┌─────────────────────────────────────────────────────┐
│ Worker                                              │
│                                                     │
│ For each batch:                                     │
│   Group events by table and type                    │
│                                                     │
│   INSERT events → Bulk INSERT                       │
│   UPDATE events → INSERT with new _version          │
│   DELETE events → INSERT with _is_deleted=1         │
│                                                     │
│   On success: activeBatches.Done()                  │
│   On failure: Retry with exponential backoff        │
└─────────────────────────────────────────────────────┘
```

#### 6. Checkpoint Creation

Checkpoints are created periodically to enable crash recovery:

```
┌─────────────────────────────────────────────────────┐
│ Checkpoint Manager                                  │
│                                                     │
│ Triggers:                                           │
│ - Time interval (default: 30s)                      │
│ - Event count threshold (1000+ events)              │
│ - Binlog file rotation                              │
│                                                     │
│ Process:                                            │
│ 1. Capture current binlog position                  │
│ 2. Wait for all pending batches to complete         │
│ 3. Save checkpoint to ClickHouse                    │
└─────────────────────────────────────────────────────┘
```

### Example: Transaction Flow

Here's how a MySQL transaction flows through the system:

```
Timeline    Component           Action
────────────────────────────────────────────────────────────────
T+0ms       MySQL               INSERT INTO users VALUES (1, 'Alice')
            MySQL               UPDATE users SET name='Bob' WHERE id=1
            MySQL               COMMIT → Written to binlog

T+5ms       CDC                 Reads WRITE_ROWS_EVENT
            CDC                 Converts to Event{Type: INSERT, ...}
            CDC                 Sends to eventChan

T+6ms       CDC                 Reads UPDATE_ROWS_EVENT
            CDC                 Converts to Event{Type: UPDATE, ...}
            CDC                 Sends to eventChan

T+10ms      Router              Receives INSERT event
            Router              Routes to Batcher

T+11ms      Router              Receives UPDATE event
            Router              Routes to Batcher

T+12ms      Batcher             Appends both to buffer["mydb.users"]

T+500ms     Batcher             Batch timeout triggers flush
            Batcher             Creates batch with 2 events
            Batcher             Sends to Worker #1

T+510ms     Worker #1           Receives batch
            Worker #1           Groups: INSERT[1], UPDATE[1]
            Worker #1           Executes INSERT in ClickHouse
            Worker #1           Executes UPDATE in ClickHouse
            Worker #1           Calls activeBatches.Done()

T+30s       Checkpoint          Interval elapsed
            Checkpoint          Saves position to ClickHouse
```

If the application crashes after T+30s and restarts, it will:
1. Load the checkpoint from ClickHouse
2. Resume from the saved binlog position
3. Continue processing without data loss

## Data Structures

### Event

The core event structure flowing through the pipeline:

```go
type Event struct {
    ID        string                 // Unique identifier (UUID)
    Type      EventType              // INSERT, UPDATE, DELETE, DDL
    Database  string                 // Source database name
    Table     string                 // Source table name
    Timestamp time.Time              // Event timestamp from binlog
    Position  BinlogPosition         // Binlog file and offset
    Data      map[string]interface{} // New row data
    OldData   map[string]interface{} // Previous row data (UPDATE/DELETE)
    SQL       string                 // DDL statement (DDL events only)
}
```

### BinlogPosition

Tracks position in the MySQL binlog:

```go
type BinlogPosition struct {
    File   string // Binlog filename (e.g., "mysql-bin.000001")
    Offset uint32 // Byte offset within the file
    GTID   string // Optional GTID for HA setups
}
```

### Checkpoint

Persisted state for crash recovery:

```go
type Checkpoint struct {
    ID                  string         // Unique identifier
    Position            BinlogPosition // Binlog position
    LastBinlogTimestamp time.Time      // Timestamp of last event
    CreatedAt           time.Time      // When checkpoint was created
}
```

## Concurrency Model

### Channel Communication

| Channel | Buffer Size | Producer | Consumer | Purpose |
|---------|-------------|----------|----------|---------|
| `cdc.eventChan` | 10,000 | CDC | Router | Raw events from binlog |
| `worker.batchChan` | 1,000 | Batcher | Workers | Event batches |
| `tableQueue.eventChan` | 1,000 | Router | Table Worker | Per-table ordering |

### Synchronization

- **WaitGroups**: Track in-flight batches for DDL barriers and checkpoints
- **Mutexes**: Protect shared state (schema cache, batch buffers)
- **Atomic counters**: Track metrics without locking

### Backpressure

When the system is overwhelmed:

1. Worker channels fill up
2. Batcher blocks trying to send batches
3. Event channel fills up
4. CDC blocks reading from binlog
5. MySQL binlog stream pauses (TCP backpressure)

This ensures no events are lost even under heavy load.

## Failure Handling

### Connection Failures

- **MySQL**: Automatic reconnection with exponential backoff
- **ClickHouse**: Retry failed operations with configurable attempts

### Crash Recovery

1. On startup, load last checkpoint from ClickHouse
2. Validate the binlog position still exists in MySQL
3. If valid, resume from checkpoint position
4. If invalid (binlog expired), start from current position

### DDL Failures

1. Retry DDL execution with exponential backoff
2. If all retries fail, log error and continue
3. Manual intervention may be required for schema sync

## Performance Considerations

### Tuning Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `pipeline.batch_size` | 500 | Larger = better throughput, higher latency |
| `pipeline.batch_timeout` | 2s | Smaller = lower latency, more overhead |
| `pipeline.worker_count` | 4 | More workers = higher parallelism |
| `state.checkpoint_interval` | 30s | Smaller = less replay on crash, more I/O |

### Bottlenecks

1. **MySQL binlog read**: Limited by network and binlog generation rate
2. **ClickHouse writes**: Usually the bottleneck; tune batch size and workers
3. **Schema cache misses**: First event for a table requires schema lookup

## Related Documentation

- [Configuration Reference](configuration.md) - All configuration options
- [MySQL Privileges](mysql-privileges.md) - Required MySQL permissions
- [ClickHouse Privileges](clickhouse-privileges.md) - Required ClickHouse permissions
