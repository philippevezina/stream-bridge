# Codebase Structure

**Analysis Date:** 2026-01-21

## Directory Layout

```
stream-bridge/
├── cmd/
│   └── stream-bridge/
│       └── main.go              # Application entry point
├── internal/
│   ├── clickhouse/              # ClickHouse integration
│   │   └── client.go
│   ├── common/                  # Shared types and utilities
│   │   ├── types.go
│   │   ├── logging.go
│   │   └── table_filter.go
│   ├── config/                  # Configuration loading and parsing
│   │   ├── config.go
│   │   ├── envsubst.go
│   │   └── envsubst_test.go
│   ├── metrics/                 # Prometheus metrics
│   │   ├── metrics.go
│   │   └── prometheus.go
│   ├── mysql/                   # MySQL CDC implementation
│   │   ├── cdc.go
│   │   └── connector/
│   │       └── connector.go
│   ├── observability/           # Error tracking and log exporting
│   │   ├── manager.go
│   │   ├── interfaces.go
│   │   ├── types.go
│   │   ├── sentry_reporter.go
│   │   ├── newrelic_exporter.go
│   │   └── noop.go
│   ├── pipeline/                # Event processing pipeline
│   │   └── processor.go
│   ├── schema/                  # Schema management and translation
│   │   ├── discovery.go
│   │   ├── translator.go
│   │   ├── ddl_translator.go
│   │   ├── ddl_parser.go
│   │   ├── cache.go
│   │   └── ddl_cache_updater.go
│   ├── security/                # Security utilities
│   │   └── identifier.go
│   ├── snapshot/                # Initial snapshot loader
│   │   ├── manager.go
│   │   ├── coordinator.go
│   │   ├── loader.go
│   │   ├── chunked_reader.go
│   │   └── types.go
│   └── state/                   # Position and snapshot state management
│       ├── manager.go
│       ├── interface.go
│       └── clickhouse.go
├── configs/                     # YAML configuration files
│   ├── example.yaml
│   └── minimal.yaml
└── docs/                        # Documentation
```

## Directory Purposes

**`cmd/stream-bridge/`:**
- Purpose: Main application entry point and component orchestration
- Contains: Application struct, initialization logic, startup sequence, shutdown management, signal handling
- Key files: `main.go` (691 lines) - orchestrates all components, manages lifecycle

**`internal/common/`:**
- Purpose: Shared data types and utility functions used across all packages
- Contains: Core event types, binlog position tracking, table filtering logic, logging utilities
- Key files: `types.go`, `table_filter.go`, `logging.go`

**`internal/config/`:**
- Purpose: Configuration file loading, parsing, environment variable substitution
- Contains: Viper-based configuration with YAML parsing, env var interpolation
- Key files: `config.go` (defines all Config structs), `envsubst.go` (${VAR_NAME} expansion)

**`internal/mysql/`:**
- Purpose: MySQL binary log change data capture
- Contains: Binlog syncer, event streaming, connection management, position tracking
- Key files: `cdc.go` (processes binlog events), `connector/connector.go` (handles MySQL connections)

**`internal/pipeline/`:**
- Purpose: Event processing and batching for ClickHouse writes
- Contains: Worker pool, batch accumulation, DDL synchronization, event ordering per table
- Key files: `processor.go` (1000+ lines - core batching and DDL logic)

**`internal/clickhouse/`:**
- Purpose: ClickHouse database integration
- Contains: Connection pooling, bulk INSERT operations, DDL execution, schema caching, type translation
- Key files: `client.go` - manages connections, executes queries, maintains schema cache

**`internal/schema/`:**
- Purpose: Schema management, DDL translation, MySQL↔ClickHouse type mapping
- Contains: DDL parser, type translator, cache updater, schema discovery
- Key files:
  - `translator.go` - MySQL type → ClickHouse type mapping
  - `ddl_translator.go` - converts MySQL DDL to ClickHouse DDL
  - `discovery.go` - loads schema from MySQL for initial snapshot
  - `cache.go` - in-memory schema cache

**`internal/state/`:**
- Purpose: Position checkpoint and snapshot progress persistence
- Contains: Checkpoint lifecycle, ClickHouse state storage implementation, recovery logic
- Key files: `manager.go` (manages checkpoints), `clickhouse.go` (stores to ClickHouse table)

**`internal/snapshot/`:**
- Purpose: Initial bulk data load from MySQL to ClickHouse
- Contains: Chunked data reading, concurrent table loading, progress tracking
- Key files: `manager.go`, `coordinator.go`, `loader.go`, `chunked_reader.go`

**`internal/metrics/`:**
- Purpose: Prometheus metrics collection and HTTP endpoint
- Contains: Metric gauges/counters, HTTP server for /metrics endpoint
- Key files: `metrics.go` (interface), `prometheus.go` (implementation)

**`internal/observability/`:**
- Purpose: Error reporting (Sentry) and log exporting (NewRelic)
- Contains: Manager orchestrating error reporters and log exporters, provider-specific implementations
- Key files: `manager.go`, `sentry_reporter.go`, `newrelic_exporter.go`, `noop.go`

**`internal/security/`:**
- Purpose: Security utilities (identifier escaping)
- Contains: SQL identifier escaping to prevent injection
- Key files: `identifier.go`

**`configs/`:**
- Purpose: Example YAML configuration templates
- Contains: `example.yaml` (full production example), `minimal.yaml` (development example)
- Used for: `-config` CLI flag to specify configuration file path

## Key File Locations

**Entry Points:**
- `cmd/stream-bridge/main.go`: Application entry point and orchestration

**Configuration:**
- `internal/config/config.go`: All configuration structures and loading logic

**Core Logic:**
- `internal/mysql/cdc.go`: Binlog event capture and streaming
- `internal/pipeline/processor.go`: Event batching and ClickHouse writing
- `internal/state/manager.go`: Checkpoint persistence and recovery
- `internal/snapshot/manager.go`: Initial data loading

**Testing:**
- `internal/config/envsubst_test.go`: Tests for environment variable substitution

## Naming Conventions

**Files:**
- Package files use snake_case: `ddl_translator.go`, `chunked_reader.go`, `table_filter.go`
- Test files: `*_test.go` (e.g., `envsubst_test.go`)
- One primary struct per file convention mostly followed (e.g., `processor.go` for Processor)

**Directories:**
- Package names match directory names (e.g., `internal/mysql/` contains package `mysql`)
- Descriptive names for logical groupings: `pipeline`, `snapshot`, `observability`
- Flat structure within each package (no sub-directories for structs)

**Types:**
- Structs: PascalCase (e.g., `Processor`, `Event`, `Checkpoint`)
- Interfaces: PascalCase (e.g., `StateStorage`, `Flusher`)
- Constants: UPPER_SNAKE_CASE (e.g., `EventTypeInsert`, `SSLModeRequired`)
- Methods: camelCase (e.g., `Start()`, `processEvents()`, `SaveCheckpoint()`)

**Packages:**
- All packages under `internal/` for Go module encapsulation
- Main application package: `main` (in `cmd/stream-bridge/`)

## Where to Add New Code

**New Feature - Event Processing:**
- Primary code: `internal/pipeline/processor.go` (add methods to Processor struct)
- Related changes: May need updates to `internal/common/types.go` for new Event type handling
- Tests: Create `internal/pipeline/processor_test.go` alongside

**New Feature - Data Source:**
- Primary code: New directory `internal/[source]/` with main source file (e.g., `internal/postgresql/cdc.go`)
- Dependencies: Likely needs to implement similar interface patterns as MySQL CDC
- Integration: Update `cmd/stream-bridge/main.go:initialize()` to instantiate new source

**New External Integration (Error Tracking/Logging):**
- Primary code: `internal/observability/[provider]_reporter.go` or `internal/observability/[provider]_exporter.go`
- Interface: Must implement `ErrorReporter` or `LogExporter` interfaces (see `internal/observability/interfaces.go`)
- Factory: Add provider case to `internal/observability/manager.go` init functions

**New Database Backend (State Storage):**
- Primary code: New file `internal/state/[backend].go` (e.g., `file.go`)
- Interface: Must implement `StateStorage` interface from `internal/state/interface.go`
- Factory: Add case to `internal/state/manager.go:NewManager()` for backend selection

**Utilities/Helpers:**
- Shared helpers: `internal/common/` (if used across multiple packages)
- Package-specific helpers: Keep in same package (e.g., helper functions in `pipeline/processor.go`)

**Metrics:**
- New Prometheus metrics: Add methods to `Metrics` interface in `internal/metrics/metrics.go`
- Implementation: Add to `PrometheusMetrics` struct in `internal/metrics/prometheus.go`

**Configuration:**
- New config section: Add struct to `internal/config/config.go` and add field to `Config` struct
- Defaults: Add to `setDefaults()` function in `internal/config/config.go`

## Special Directories

**`configs/`:**
- Purpose: YAML configuration templates for different environments
- Generated: No
- Committed: Yes (checked into version control)
- Usage: Users copy and modify as needed, or use via `-config` CLI flag

**`internal/`:**
- Purpose: Go module encapsulation - code in `internal/` cannot be imported by external packages
- Generated: No
- Committed: Yes (all Go source code committed)

**`cmd/stream-bridge/`:**
- Purpose: Executable package containing main() function
- Generated: Binary output (not committed)
- Committed: Source code committed

**`.planning/codebase/`:**
- Purpose: Generated by GSD tools - contains analysis documents
- Generated: Yes (by gsd-codebase-mapper)
- Committed: Git status shows many .claude files, check project-specific policy

## Component Dependencies

**Direction of Dependencies (lower layers don't depend on higher layers):**

```
Application (main.go)
    ↓
State Manager ← ClickHouse Client ← Schema components
    ↓              ↓
MySQL CDC        Pipeline Processor ← Schema components
    ↓              ↓
    └→ Event Channel →
                     ↓
              Worker Pool (batching to ClickHouse)
                     ↓
              ClickHouse Client

Snapshot Manager
    ↓
    ├→ Schema Discovery
    ├→ ClickHouse Client
    └→ State Manager

Observability Manager
    └→ Runs independently

Metrics Manager
    └→ Runs independently
```

## File Import Patterns

**Circular dependency prevention:**
- Packages are layered: MySQL/Config → Common → Pipeline/ClickHouse/Schema → Application
- No bidirectional imports between packages
- Interfaces used for abstraction (e.g., Processor implements Flusher interface for CDC)

**Standard library imports grouped first:**
```go
import (
    "context"
    "fmt"
    // ... other stdlib

    "go.uber.org/zap"  // External dependencies

    "github.com/philippevezina/stream-bridge/internal/..."  // Internal packages
)
```

---

*Structure analysis: 2026-01-21*
