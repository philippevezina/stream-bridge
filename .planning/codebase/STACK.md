# Technology Stack

**Analysis Date:** 2026-01-21

## Languages

**Primary:**
- Go 1.24.5 - Core application language for all components

## Runtime

**Environment:**
- Go 1.24.5 runtime

**Build:**
- Standard Go build tools (`go build`, `go test`, `go mod`)
- Module: `github.com/philippevezina/stream-bridge`

## Frameworks & Core Libraries

**Database Drivers:**
- `github.com/ClickHouse/clickhouse-go/v2 v2.37.2` - ClickHouse client for replication sink
- `github.com/go-mysql-org/go-mysql v1.12.0` - MySQL binlog parser and CDC client

**Logging:**
- `go.uber.org/zap v1.27.0` - Structured logging framework
- `gopkg.in/natefinch/lumberjack.v2 v2.2.1` - Log rotation and file management

**Configuration:**
- `github.com/spf13/viper v1.20.1` - Configuration file parsing (YAML) with environment variable support

**Monitoring & Metrics:**
- `github.com/prometheus/client_golang v1.22.0` - Prometheus metrics exposition

**Observability & Error Reporting:**
- `github.com/getsentry/sentry-go v0.41.0` - Sentry error tracking and reporting
- `github.com/newrelic/go-agent/v3 v3.42.0` - NewRelic APM integration
- `github.com/newrelic/go-agent/v3/integrations/logcontext-v2/nrzap v1.2.4` - NewRelic Zap logger integration for log forwarding

**Utilities:**
- `github.com/google/uuid v1.6.0` - UUID generation for event tracking

## Key Dependencies

**Critical - Database Integration:**
- `github.com/ClickHouse/ch-go v0.66.1` - Low-level ClickHouse protocol client (indirect, via clickhouse-go)
- Supports ReplacingMergeTree and ReplicatedReplacingMergeTree engines for schema synchronization

**Critical - MySQL Binlog Processing:**
- `github.com/pingcap/tidb/pkg/parser v0.0.0-20241118164214-4f047be191be` - MySQL SQL parser for DDL translation (via go-mysql dependency chain)

**Infrastructure:**
- `google.golang.org/grpc v1.67.3` - gRPC support (transitive dependency)
- `google.golang.org/protobuf v1.36.5` - Protocol buffers (transitive dependency)

**Compression:**
- `github.com/klauspost/compress v1.18.0` - Brotli/LZ4 compression for network communication

## Configuration

**Environment:**
- Configuration via YAML files (default: `configs/example.yaml`)
- Environment variable substitution supported in config using `${VAR:-default}` syntax
- File location: `internal/config/config.go`

**Config Files:**
- `configs/example.yaml` - Production reference configuration with all options
- `configs/minimal.yaml` - Development/testing minimal configuration
- `configs/local.yaml` - Local development configuration
- `configs/newrelic-test.yaml` - NewRelic integration testing configuration

**Key Configuration Sections:**
- `mysql` - Binlog source connection with SSL/TLS support (5 modes: disabled, preferred, required, verify_ca, verify_identity)
- `clickhouse` - Replication sink connection with cluster support
- `pipeline` - Event batching, worker pools, DDL synchronization timeout
- `monitoring` - Prometheus metrics endpoint (default: `:8080/metrics`)
- `logging` - Structured logging (format: json/console, output: stdout/file with rotation)
- `state` - Checkpoint persistence backend (ClickHouse or file-based, default: ClickHouse)
- `schema` - DDL translation defaults (table engine, nullable preservation, timestamp precision)
- `snapshot` - Initial data sync configuration (chunking, parallel tables, lock strategy)
- `observability` - Error reporting (Sentry) and log exporting (NewRelic)

## Platform Requirements

**Development:**
- Go 1.24.5 or later
- MySQL 5.7+ with binlog enabled (ROW format required)
- ClickHouse 21.1 or later
- YAML configuration file

**Production:**
- Go runtime (compiled binary)
- MySQL 5.7+ (MySQL 8.0+ recommended for backup locks snapshot feature)
- ClickHouse cluster with ReplacingMergeTree support
- Optional: Sentry project (error reporting)
- Optional: NewRelic account (log forwarding)
- Optional: Prometheus scrape endpoint

## Environment Variables

**Configuration Substitution:**
- Any config value supports `${VAR_NAME:-default_value}` syntax
- Examples: `${MYSQL_PASSWORD:-}`, `${SENTRY_DSN:-}`, `${NEW_RELIC_LICENSE_KEY:-}`

**Critical Variables (via config file):**
- `mysql.username`, `mysql.password`, `mysql.host`, `mysql.port`, `mysql.database`
- `clickhouse.addresses`, `clickhouse.username`, `clickhouse.password`
- `observability.error_reporting.sentry.dsn` (for Sentry)
- `observability.log_exporting.newrelic.license_key` (for NewRelic)

## Build & Deployment

**Build Commands:**
```bash
go build ./...                              # Build all packages
go build -o stream-bridge cmd/stream-bridge/main.go  # Build main binary
go build -race ./...                        # Build with race detection
go mod tidy                                 # Clean up dependencies
```

**Run Commands:**
```bash
./stream-bridge -config configs/example.yaml          # Run with config
./stream-bridge -version                              # Show version
./stream-bridge -snapshot                             # Enable initial snapshot
./stream-bridge -force-snapshot                       # Force new snapshot
./stream-bridge -test-sentry                          # Test Sentry integration
./stream-bridge -test-newrelic                        # Test NewRelic integration
```

**Entry Point:** `cmd/stream-bridge/main.go`

---

*Stack analysis: 2026-01-21*
