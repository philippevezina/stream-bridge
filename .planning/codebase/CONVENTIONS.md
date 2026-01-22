# Coding Conventions

**Analysis Date:** 2026-01-21

## Naming Patterns

**Files:**
- Snake case for file names: `envsubst.go`, `envsubst_test.go`, `go_fmt.sh`
- Test files use `_test.go` suffix
- Package names are lowercase, single word when possible: `config`, `pipeline`, `mysql`, `state`, `schema`, `clickhouse`, `metrics`, `observability`, `snapshot`, `security`, `common`

**Functions:**
- PascalCase for exported functions: `NewProcessor()`, `NewManager()`, `Initialize()`, `Start()`, `Stop()`
- camelCase for unexported functions: `setupSyncer()`, `expandEnvWithDefaults()`, `getTableList()`
- Constructor functions follow `New{Type}()` pattern: `NewClient()`, `NewCDC()`, `NewLogger()`
- Interface methods use PascalCase: `GetLatestCheckpoint()`, `SaveCheckpoint()`, `Close()`

**Variables:**
- camelCase for local variables and parameters: `eventChan`, `errorChan`, `tableFilter`, `lastEventTime`
- Short names acceptable in loops: `err`, `i`, `ctx`
- Constants use UPPER_SNAKE_CASE: `EventTypeInsert`, `EngineReplacingMergeTree`
- Struct fields use PascalCase: `ID`, `Type`, `Database`, `Table`, `Timestamp`, `Position`

**Types:**
- PascalCase for struct and interface types: `Event`, `Processor`, `Manager`, `CDC`, `Checkpoint`
- Suffix with `Config` for configuration structs: `PipelineConfig`, `MySQLConfig`, `ClickHouseConfig`
- Suffix with `Manager` for managers: `StateManager`, `SnapshotManager`, `ObservabilityManager`
- Type aliases use PascalCase: `EventType`, `TableEngine`, `TableInfoCache`

## Code Style

**Formatting:**
- Go's built-in `gofmt` used for formatting (enforced via `.claude/hooks/go-fmt.sh`)
- All files formatted with standard Go style
- Indentation: tabs (Go standard)
- Line length: no hard limit, but keep reasonable (typically <100 chars when practical)

**Linting:**
- No external linter configuration file present
- Code follows Go conventions by default
- Recommend using `golangci-lint` for CI/CD (not currently in use)

## Import Organization

**Order:**
1. Standard library imports (e.g., `"context"`, `"fmt"`, `"sync"`, `"time"`)
2. Third-party imports (e.g., `"github.com/go-mysql-org/go-mysql"`, `"go.uber.org/zap"`)
3. Local imports (e.g., `"github.com/philippevezina/stream-bridge/internal/*"`)

**Path Aliases:**
- No import aliases used in practice
- Full package paths consistently used: `github.com/philippevezina/stream-bridge/internal/config`

**Example Pattern from `cmd/stream-bridge/main.go`:**
```go
import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	// ... more internal imports
)
```

## Error Handling

**Patterns:**
- Error wrapping with `fmt.Errorf()` using `%w` verb to preserve stack trace: `fmt.Errorf("failed to create X: %w", err)`
- Every function that can fail returns `error` as last return value
- Check errors immediately after operations
- Interface-based errors returned when needed

**Error Return Style:**
```go
func (c *Client) connect() error {
	// ...
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	return nil
}
```

**Error Channels:**
- Error channels used for async error propagation: `errorChan chan error`
- Typically buffered channels with capacity (e.g., `make(chan error, 100)`, `make(chan error, 1000)`)
- Dedicated goroutines handle error channel reads to prevent starvation

**Error Context:**
- Contextual information logged with errors using zap fields: `zap.Error(err)`, `zap.String("component", name)`
- Error reporting to observability tools (`Sentry`) for critical errors
- Graceful degradation when possible (e.g., continue if optional operations fail)

## Logging

**Framework:** `go.uber.org/zap` (structured logging)

**Patterns:**
- All loggers passed as `*zap.Logger` through dependency injection
- Logger instances created via `common.NewLogger()` or `common.NewLoggerCore()`
- Component-specific loggers created via `common.LoggerWithComponent(logger, "component_name")`
- Logs include structured fields for context: `logger.Info("message", zap.String("key", value), zap.Error(err))`

**Log Levels:**
- `Info()`: Normal operations, startup messages, checkpoint events
- `Warn()`: Recoverable issues, degraded conditions, ping failures
- `Error()`: Unrecoverable errors, shutdown errors
- `Debug()`: Detailed diagnostics, cache operations (rarely used)

**Example Pattern from `cmd/stream-bridge/main.go`:**
```go
a.logger.Info("Stream Bridge started successfully",
	zap.String("version", common.GetVersion()))

a.logger.Error("Programmatic shutdown requested", zap.Error(err))

a.logger.Warn("ClickHouse ping failed", zap.Error(err))
```

## Comments

**When to Comment:**
- Document public functions and types (package-level declarations)
- Explain non-obvious implementation details or complex algorithms
- Note important ordering/synchronization requirements
- Mark TODO/FIXME/XXX items (rarely used in this codebase)

**JSDoc/Go Doc Style:**
- Go-style comments: `// Comment` starting sentences with function/type name for package documentation
- Interfaces and exported types documented: `type StateStorage interface {` (no specific pattern found for full documentation)

**Example from `internal/state/interface.go`:**
```go
type StateStorage interface {
	Initialize(ctx context.Context) error
	Close() error
	SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
	GetLatestCheckpoint(ctx context.Context) (*Checkpoint, error)
	// ... more methods
}
```

**Example from `cmd/stream-bridge/main.go`:**
```go
// runErrorHandler processes errors from a single error channel in a dedicated goroutine.
// This prevents error handling from being starved when event processing is under high load.
func (a *Application) runErrorHandler(ctx context.Context, errChan <-chan error, name, component, operation string) {
```

## Function Design

**Size:** Functions kept modular and focused
- Average function length: 30-100 lines (typical for Go)
- Complex orchestration broken into smaller functions
- Examples: `Start()`, `Stop()`, `Initialize()` are all focused

**Parameters:**
- Receiver pattern used for methods: `func (p *Processor) Start() error {`
- Context as first parameter for functions that support cancellation: `func (c *Client) PopulateCacheFromClickHouse(ctx context.Context) error`
- Configuration and dependencies injected at construction time
- Functional options pattern not used (constructor takes complete config)

**Return Values:**
- Single error return for failure scenarios: `error` as last return value
- Multiple returns for success data + error: `(*Checkpoint, error)`, `([]string, error)`
- Pointers returned for large structs or when nil is meaningful: `*Checkpoint`, `*Manager`
- Value returns for small types: `time.Duration`, `int64`

**Example Pattern from `internal/state/manager.go`:**
```go
func (m *Manager) GetLastPosition() *common.BinlogPosition {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.currentCheckpoint == nil {
		return nil
	}

	return &m.currentCheckpoint.Position
}
```

## Module Design

**Exports:**
- Only exported (PascalCase) symbols from package: `NewManager()`, `Manager struct`, `Start()`, `Close()`
- Unexported types and functions use camelCase: `worker`, `batcher`, `processorMetrics`

**Barrel Files:**
- Not used in this codebase
- Each package has specific responsibility: `config`, `pipeline`, `state`, `mysql`, etc.

**Visibility:**
- Package-level functions for setup/initialization
- Private helper functions for internal logic
- Interface types define public contracts (e.g., `StateStorage`, `Flusher`, `TableInfoCache`)

**Example from `internal/pipeline/processor.go`:**
```go
// Exported
type Processor struct {
	cfg            *config.PipelineConfig
	// ... public fields
}

// Unexported
type worker struct {
	id        int
	processor *Processor
	batchChan chan []*common.Event
}

// Exported
func NewProcessor(...) *Processor {
	// ...
}
```

## Concurrency Patterns

**Synchronization:**
- `sync.RWMutex` for protecting shared state read/write: `mu sync.RWMutex`
- `sync.WaitGroup` for coordination: `wg sync.WaitGroup`
- Atomic operations for counters: `atomic.uint64`, `atomic.AddUint64()`
- Context for cancellation and timeouts: `ctx context.Context`

**Channel Usage:**
- Buffered channels for event queues: `eventChan chan *common.Event`
- Error channels for async error propagation: `errorChan chan error`
- Shutdown signals via closed channel: `shutdown chan struct{}`
- Select statements for multiplexing: `select { case ...: case <-ctx.Done(): }`

## Interfaces

**Design:**
- Small, focused interfaces: `StateStorage`, `Flusher`, `TableInfoCache`, `Metrics`
- Interfaces define contracts at package boundaries
- No large interfaces with many methods
- Implementation pattern: concrete types implement interfaces

---

*Convention analysis: 2026-01-21*
