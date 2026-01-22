# Testing Patterns

**Analysis Date:** 2026-01-21

## Test Framework

**Runner:**
- Go's built-in `testing` package (no external test framework)
- Version: Go 1.24.5
- Standard `go test` command used for execution

**Assertion Library:**
- Manual assertions using `t.Errorf()`, `t.Error()` from standard testing package
- No external assertion libraries (testify, etc.) in use

**Run Commands:**
```bash
go test ./...              # Run all tests in codebase
go test -race ./...        # Run with race detector for concurrency issues
go test -v ./...           # Verbose output
go test -cover ./...       # Show coverage
go test ./internal/config  # Run specific package tests
```

## Test File Organization

**Location:**
- Co-located with source code (same package, `_test.go` suffix)
- Example: `internal/config/envsubst.go` → `internal/config/envsubst_test.go`

**Naming:**
- Test files: `{source}_test.go`
- Test functions: `Test{FunctionName}()` pattern
- Benchmark functions: Not found in codebase
- Helper/subtests: Use `t.Run()` for grouped test cases

**Structure:**
```
internal/config/
├── config.go           # Main source
├── envsubst.go         # Source file
└── envsubst_test.go    # Tests
```

## Test Structure

**Suite Organization:**
From `internal/config/envsubst_test.go`:

```go
func TestExpandEnvWithDefaults(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		envVars     map[string]string
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name:     "standard ${VAR} expansion",
			input:    "host: ${TEST_HOST}",
			envVars:  map[string]string{"TEST_HOST": "localhost"},
			expected: "host: localhost",
		},
		// ... more test cases
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			for key := range tt.envVars {
				os.Unsetenv(key)
			}
			for key, value := range tt.envVars {
				os.Setenv(key, value)
			}

			// Test
			result, err := expandEnvWithDefaults(tt.input)

			// Assertion
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}

			// Cleanup
			for key := range tt.envVars {
				os.Unsetenv(key)
			}
		})
	}
}
```

**Patterns:**
- Table-driven tests: Array of `struct` with test cases, loop with `t.Run()`
- Setup: Environment variable configuration (cleanup before and after each test)
- Execution: Call function with test inputs
- Assertion: Error checking with `t.Errorf()` or value comparison
- Cleanup: Restore environment state after test

## Mocking

**Framework:**
- No external mocking library in use (no `testify/mock`, `gomock`, etc.)
- Manual mocking via interface-based design
- Dependency injection enables simple stubbing

**Patterns:**
Not extensively shown in existing tests, but architecture supports mocking:
- Functions accept interfaces: `StateStorage interface`, `Flusher interface`, `TableInfoCache interface`
- Mock implementations can be created by implementing interfaces
- See `internal/state/interface.go` for mockable interfaces

**Example Interface for Mocking:**
```go
// From internal/state/interface.go
type StateStorage interface {
	Initialize(ctx context.Context) error
	Close() error
	SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
	GetLatestCheckpoint(ctx context.Context) (*Checkpoint, error)
	GetCheckpoint(ctx context.Context, id string) (*Checkpoint, error)
	ListCheckpoints(ctx context.Context, limit int) ([]*Checkpoint, error)
	HealthCheck(ctx context.Context) error
	SaveSnapshotProgress(ctx context.Context, progress *SnapshotProgress) error
	GetSnapshotProgress(ctx context.Context, id string) (*SnapshotProgress, error)
	GetLatestSnapshotProgress(ctx context.Context) (*SnapshotProgress, error)
	ListSnapshotProgress(ctx context.Context, limit int) ([]*SnapshotProgress, error)
	UpdateSnapshotStatus(ctx context.Context, id string, status string, errorMsg string) error
}
```

**What to Mock:**
- External dependencies (databases, APIs, file systems)
- Slow operations (network calls, database queries)
- Non-deterministic behavior (time, random)
- Dependencies with side effects

**What NOT to Mock:**
- Small utility functions
- Pure functions with no side effects
- Critical business logic that should be tested as-is

## Fixtures and Factories

**Test Data:**
- Inline test data in table-driven test structs
- No separate fixture files
- Environment variables used as test configuration

**Example from `internal/config/envsubst_test.go`:**
```go
{
	name: "yaml config with env vars",
	input: `mysql:
  host: "${MYSQL_HOST:-localhost}"
  port: 3306
  username: "${MYSQL_USER:-root}"
  password: "${MYSQL_PASSWORD}"`,
	envVars: map[string]string{
		"MYSQL_HOST":     "db.example.com",
		"MYSQL_PASSWORD": "secret",
	},
	expected: `mysql:
  host: "db.example.com"
  port: 3306
  username: "root"
  password: "secret"`,
},
```

**Location:**
- Test fixtures inline in test functions
- No separate `fixtures/` or `testdata/` directories currently used
- Configuration passed via struct fields

## Coverage

**Requirements:**
- No explicit coverage target enforced
- Coverage generation available but not mandated

**View Coverage:**
```bash
go test -cover ./...                    # Basic coverage percentage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out       # HTML coverage report
```

## Test Types

**Unit Tests:**
- Scope: Individual functions and exported methods
- Approach: Table-driven tests with multiple scenarios
- Focus: Logic correctness, error handling, edge cases
- Example: `TestExpandEnvWithDefaults` tests environment variable expansion with 20+ scenarios

**Integration Tests:**
- Scope: Multiple components working together (NOT demonstrated in current test suite)
- Approach: Would require running external services (MySQL, ClickHouse)
- Status: Not implemented in codebase (only one test file present)
- Infrastructure: Would require docker-compose or external databases

**E2E Tests:**
- Framework: Not implemented
- Would require full application stack (MySQL CDC, ClickHouse, state management)
- Could use: testcontainers or manual container orchestration

## Common Patterns

**Async Testing:**
Not demonstrated in current tests (envsubst_test.go is synchronous only).

For goroutine/channel testing (pattern to follow):
```go
// Would use context and select for channel tests
func TestAsyncOperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		// async operation
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("operation timed out")
	}
}
```

**Error Testing:**
```go
// From envsubst_test.go - pattern for error validation
if tt.expectError {
	if err == nil {
		t.Errorf("expected error but got none")
		return
	}
	if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
		t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
	}
	return
}

if err != nil {
	t.Errorf("unexpected error: %v", err)
	return
}
```

**Setup/Teardown:**
```go
// Environment setup before test
for key := range tt.envVars {
	os.Unsetenv(key)  // Clear first
}
for key, value := range tt.envVars {
	os.Setenv(key, value)  // Set for test
}

// Cleanup after test
defer func() {
	for key := range tt.envVars {
		os.Unsetenv(key)
	}
}()
```

## Test Statistics

**Current State:**
- Total test files: 1 (`internal/config/envsubst_test.go`)
- Total test functions: 3
  - `TestExpandEnvWithDefaults()` - 18 main test cases + edge cases
  - `TestExpandEnvWithDefaults_MultipleRequiredErrors()` - 1 test
  - `TestExpandEnvWithDefaults_PartialMatch()` - 3 subtests
- Test lines: ~275 lines (more thorough than source)
- Coverage: Configuration package only

**Missing Test Coverage:**
- `internal/pipeline/` - No tests
- `internal/mysql/` - No tests
- `internal/clickhouse/` - No tests
- `internal/state/` - No tests (except interfaces)
- `internal/schema/` - No tests
- `internal/metrics/` - No tests
- `internal/snapshot/` - No tests
- `cmd/stream-bridge/` - No tests

## Testing Best Practices (Observed)

1. **Table-driven tests**: Comprehensive - 18+ test cases in single function
2. **Descriptive names**: Clear test case names explain what's being tested
3. **Environment isolation**: Setup/teardown for environment variables
4. **Error messages**: Informative `t.Errorf()` messages show expected vs actual
5. **Edge cases**: Multiple edge cases covered (empty strings, missing vars, special characters)
6. **Test organization**: All tests in same package as code, co-located

## Recommendations for Expanding Tests

**Priority 1 (High):**
- Add tests for `internal/state/` - Critical for checkpointing
- Add tests for `internal/pipeline/` - Core event processing
- Add tests for `internal/mysql/` - CDC position tracking

**Priority 2 (Medium):**
- Add integration tests with testcontainers
- Add tests for `internal/clickhouse/` schema operations
- Add tests for `internal/schema/` DDL translation

**Priority 3 (Low):**
- E2E tests with full stack
- Benchmarks for event processing performance
- Chaos testing for failure scenarios

---

*Testing analysis: 2026-01-21*
