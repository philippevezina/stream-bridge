# External Integrations

**Analysis Date:** 2026-01-21

## APIs & External Services

**MySQL Binlog CDC:**
- `go-mysql-org/go-mysql` - Streams binary log events from MySQL
  - SDK/Client: `github.com/go-mysql-org/go-mysql v1.12.0`
  - Auth: Username/password in `mysql.username`, `mysql.password`
  - Connection: Host/port in `mysql.host`, `mysql.port`
  - Features: Binlog position tracking, table filtering via regex patterns, SSL/TLS support (5 modes)

## Data Storage

**Databases:**

**MySQL (Source):**
- Type: MySQL 5.7+ (8.0+ recommended)
- Connection: TCP/IP via `go-mysql-org/go-mysql` binlog client
- Credentials: `mysql.username`, `mysql.password`
- Client: Binlog replication protocol (not SQL)
- Features:
  - Binlog format = ROW (required)
  - Table filtering with include/exclude patterns and explicit include/exclude lists
  - GTID support
  - SSL/TLS mutual authentication support

**ClickHouse (Sink):**
- Type: ClickHouse 21.1+
- Addresses: Multiple nodes supported (`clickhouse.addresses` - comma-separated list)
- Connection: TCP/IP native protocol via `github.com/ClickHouse/clickhouse-go/v2`
- Credentials: `clickhouse.username`, `clickhouse.password`
- Client: Native ClickHouse driver with connection pooling
- Features:
  - Bulk INSERT operations
  - ALTER TABLE operations for schema synchronization
  - ReplacingMergeTree and ReplicatedReplacingMergeTree engines
  - Automatic type mapping from MySQL to ClickHouse (integer, string, datetime, decimal, etc.)

**Checkpoint Storage (State Management):**
- Backend: ClickHouse (recommended) or File-based
- Default Storage: ClickHouse table `stream_bridge_checkpoints` in configured database
- Schema: Checkpoint metadata with status (PENDING → PROCESSED → COMMITTED)
- Retention: Configurable retention period (default: 168h = 7 days)
- Interval: Checkpoint creation frequency (default: 30s)

## Authentication & Identity

**Auth Provider:**
- Custom (No OAuth/OIDC)
- Implementation:
  - MySQL: TCP authentication with optional SSL certificate-based mutual TLS
  - ClickHouse: TCP authentication with optional SSL
  - Credentials stored in YAML config with environment variable substitution

**SSL/TLS Configuration:**
- MySQL SSL modes: disabled, preferred, required, verify_ca, verify_identity
- Certificate files: Client cert, client key, CA cert (configurable paths)
- ClickHouse SSL: Simple enable/disable flag

## Monitoring & Observability

**Metrics Collection:**
- Framework: Prometheus
- Endpoint: `http://localhost:8080/metrics` (default)
- Path: Configurable via `monitoring.metrics_path` (default: `/metrics`)
- Port: Configurable via `monitoring.port` (default: `8080`)
- Key metrics:
  - `stream_bridge_events_processed_total` - Total events processed
  - `stream_bridge_events_failed_total` - Failed event count
  - `stream_bridge_replication_lag_seconds` - CDC lag indicator
  - `stream_bridge_checkpoint_age_seconds` - Position persistence health
  - `stream_bridge_connection_status` - Database connectivity
  - `stream_bridge_batches_processed_total` - Batch throughput
  - `stream_bridge_queue_length` - Event queue depth

**Error Tracking:**
- Provider: Sentry (optional, disabled by default)
- SDK: `github.com/getsentry/sentry-go v0.41.0`
- Configuration: `observability.error_reporting.sentry`
- Env Var: `SENTRY_DSN` (read via `${SENTRY_DSN:-}` in config)
- Features:
  - Automatic exception capture with stack traces
  - Error context attachment (operation type, affected tables, positions)
  - Breadcrumb trails for debugging
  - Sample rate control (0.0-1.0)
  - Environment tagging (production, staging, development)
  - Release version tagging
  - Flush timeout on shutdown

**Log Exporting:**
- Provider: NewRelic (optional, disabled by default)
- SDK: `github.com/newrelic/go-agent/v3 v3.42.0` with nrzap integration
- Configuration: `observability.log_exporting.newrelic`
- Env Var: `NEW_RELIC_LICENSE_KEY` (read via `${NEW_RELIC_LICENSE_KEY:-}` in config)
- Features:
  - Automatic log forwarding via Zap core wrapping
  - Application name tagging
  - Log level filtering (min_log_level: debug, info, warn, error)
  - Flush timeout on shutdown

**Health Check:**
- Endpoint: `http://localhost:8080/health` (default)
- Path: Configurable via `monitoring.health_path` (default: `/health`)

## Logging

**Framework:** Uber Zap
- Format: JSON (default) or console
- Output: stdout (default), stderr, or file path
- Rotation: File-based rotation when output is a file
  - Max size: 100 MB (configurable)
  - Max backups: 3 files (configurable)
  - Max age: 7 days (configurable)
  - Compress: gzip compression enabled by default
- Level: debug, info, warn, error (configurable)

**Log Integration with Observability:**
- NewRelic: Auto-forwarding via nrzap core wrapper when enabled
- Sentry: Manual error capture in exception handlers

## CI/CD & Deployment

**Hosting:**
- Self-hosted (binary deployment)
- Container-ready (Go binary can run in any environment)

**CI Pipeline:**
- Not detected - No GitHub Actions, GitLab CI, or other CI config files present

**Build Requirements:**
- Go 1.24.5+ compiler
- `go mod` dependency management

## Environment Configuration

**Required Environment Variables (via config substitution):**
- `MYSQL_PASSWORD` or hardcoded in config
- `CLICKHOUSE_PASSWORD` or hardcoded in config

**Optional Environment Variables (for observability):**
- `SENTRY_DSN` - Sentry project DSN (if error reporting enabled)
- `NEW_RELIC_LICENSE_KEY` - NewRelic license key (if log exporting enabled)
- `SENTRY_ENVIRONMENT` - Environment tag for Sentry (production, staging, development)
- `NEW_RELIC_APP_NAME` - Application name in NewRelic
- `NEW_RELIC_LICENSE_KEY` - NewRelic license key

**Secrets Location:**
- YAML config file (`configs/example.yaml`, `configs/minimal.yaml`, etc.)
- Environment variables via `${VAR:-default}` substitution in config
- No `.env` file support detected (use config file or direct env vars)

## Webhooks & Callbacks

**Incoming:**
- None - Application is a CDC sink, not a webhook receiver

**Outgoing:**
- None - Integrations are pull-based (read from MySQL binlog) or push-based (write to ClickHouse)

## Network & Connectivity

**Inbound Ports:**
- `8080` - Prometheus metrics and health check endpoint (configurable)

**Outbound Connections:**
- MySQL: Port 3306 (configurable) - Binlog replication protocol
- ClickHouse: Port 9000 (default, in addresses) - Native protocol

**Connection Pool Management:**
- ClickHouse: Configurable connection pooling
  - Max open connections: 10 (default)
  - Max idle connections: 5 (default)
  - Max lifetime: 1 hour (default)
  - Dial timeout: 10 seconds (default)

## Testing Utilities

**Observability Testing:**
- Command: `./stream-bridge -test-sentry` - Send test error to Sentry
- Command: `./stream-bridge -test-newrelic` - Send test logs to NewRelic
- Configuration: Uses config file specified via `-config` flag

---

*Integration audit: 2026-01-21*
