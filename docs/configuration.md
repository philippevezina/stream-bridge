# Configuration Reference

This document provides a complete reference for all Stream Bridge configuration options.

## Configuration File

Stream Bridge uses YAML configuration files. Specify the configuration file with the `-config` flag:

```bash
./stream-bridge -config /path/to/config.yaml
```

## MySQL Configuration

Configure the MySQL source database connection.

```yaml
mysql:
  host: "localhost"
  port: 3306
  username: "replicator"
  password: "password"
  database: "mydb"
  server_id: 1001
  flavor: "mysql"
  ssl_mode: "preferred"
  ssl_cert: ""
  ssl_key: ""
  ssl_ca: ""
  event_channel_buffer: 10000
  table_filter:
    include_patterns: []
    exclude_patterns: []
    include_tables: []
    exclude_tables: []
```

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | string | `localhost` | MySQL server hostname or IP address |
| `port` | int | `3306` | MySQL server port (1-65535) |
| `username` | string | **required** | MySQL user for replication |
| `password` | string | **required** | MySQL password |
| `database` | string | **required** | Default database name |
| `server_id` | uint32 | `1001` | Unique server ID for binlog replication |
| `flavor` | string | `mysql` | MySQL flavor: `mysql` or `mariadb` |

### SSL/TLS Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ssl_mode` | string | `preferred` | SSL mode (see below) |
| `ssl_cert` | string | `""` | Path to client certificate file |
| `ssl_key` | string | `""` | Path to client private key file |
| `ssl_ca` | string | `""` | Path to CA certificate file |

**SSL Modes:**

| Mode | Description |
|------|-------------|
| `disabled` | No SSL encryption |
| `preferred` | Use SSL if available, fallback to plaintext |
| `required` | Require SSL, fail if unavailable |
| `verify_ca` | Require SSL and verify server certificate against CA |
| `verify_identity` | Require SSL, verify CA, and verify hostname |

### Buffer Settings

| Option | Type | Default | Range | Description |
|--------|------|---------|-------|-------------|
| `event_channel_buffer` | int | `10000` | 100-1000000 | Size of the CDC event channel buffer |

### Table Filtering

Control which tables are replicated using patterns or explicit lists.

```yaml
mysql:
  table_filter:
    # Regex patterns for inclusion (format: database\.table)
    include_patterns:
      - "mydb\\.users"
      - "mydb\\.orders.*"

    # Regex patterns for exclusion
    exclude_patterns:
      - ".*_temp$"
      - ".*_backup$"

    # Explicit table inclusion (format: database.table)
    include_tables:
      - "mydb.customers"

    # Explicit table exclusion
    exclude_tables:
      - "mydb.sessions"
```

**Filtering Logic:**

1. If `include_patterns` or `include_tables` are specified, only matching tables are included
2. `exclude_patterns` and `exclude_tables` are then applied to remove unwanted tables
3. Exclusions take precedence over inclusions

---

## ClickHouse Configuration

Configure the ClickHouse target database connection.

```yaml
clickhouse:
  addresses:
    - "localhost:9000"
  database: "default"
  username: "default"
  password: ""
  enable_ssl: false
  dial_timeout: "10s"
  max_open_conns: 10
  max_idle_conns: 5
  max_lifetime: "1h"
```

### Connection Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `addresses` | []string | **required** | List of ClickHouse addresses (`host:port`) |
| `database` | string | `default` | Target ClickHouse database |
| `username` | string | **required** | ClickHouse username |
| `password` | string | `""` | ClickHouse password |
| `enable_ssl` | bool | `false` | Enable SSL/TLS for connections |

### Connection Pool Settings

| Option | Type | Default | Range | Description |
|--------|------|---------|-------|-------------|
| `dial_timeout` | duration | `10s` | >0 | Connection timeout |
| `max_open_conns` | int | `10` | 1-1000 | Maximum open connections |
| `max_idle_conns` | int | `5` | 0 to max_open_conns | Maximum idle connections |
| `max_lifetime` | duration | `1h` | >0 | Connection lifetime before recycling |

---

## Pipeline Configuration

Configure event processing and batching behavior.

```yaml
pipeline:
  batch_size: 500
  batch_timeout: "2s"
  max_retries: 3
  retry_delay: "1s"
  worker_count: 4
  buffer_size: 10000
  worker_channel_buffer_size: 1000
  flush_interval: "2s"
  ddl_flush_timeout: "1m"
  worker_channel_timeout: "30s"
```

### Batching Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch_size` | int | `500` | Number of events per batch |
| `batch_timeout` | duration | `2s` | Maximum time to wait before flushing a batch |
| `flush_interval` | duration | `2s` | Force flush interval regardless of batch size |

### Retry Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_retries` | int | `3` | Maximum retry attempts on failure |
| `retry_delay` | duration | `1s` | Delay between retry attempts |

### Worker Settings

| Option | Type | Default | Range | Description |
|--------|------|---------|-------|-------------|
| `worker_count` | int | `4` | 1-100 | Number of worker goroutines |
| `buffer_size` | int | `10000` | 100-10000000 | Main event buffer size |
| `worker_channel_buffer_size` | int | `1000` | 100-1000000 | Per-worker channel buffer size |
| `worker_channel_timeout` | duration | `30s` | >0 | Worker channel operation timeout |

### DDL Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ddl_flush_timeout` | duration | `1m` | Maximum time to wait for active batches before DDL execution |

---

## State Configuration

Configure checkpoint persistence for recovery.

```yaml
state:
  type: "clickhouse"
  clickhouse:
    database: "default"
    table: "stream_bridge_checkpoints"
  checkpoint_interval: "30s"
  retention_period: "168h"
```

### Storage Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | `clickhouse` | Storage backend type (`clickhouse`) |

### ClickHouse Storage Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `clickhouse.database` | string | `default` | Database for checkpoint storage |
| `clickhouse.table` | string | `stream_bridge_checkpoints` | Table name for checkpoints |

### Checkpoint Settings

| Option | Type | Default | Minimum | Description |
|--------|------|---------|---------|-------------|
| `checkpoint_interval` | duration | `30s` | 1s | How often to create checkpoints |
| `retention_period` | duration | `168h` (7 days) | 1h | How long to retain old checkpoints |

---

## Schema Configuration

Configure schema translation behavior.

```yaml
schema:
  default_engine: "ReplacingMergeTree"
  preserve_nullable: true
  timestamp_precision: 3
  allow_destructive_ddl: true
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `default_engine` | string | `ReplacingMergeTree` | Default ClickHouse table engine |
| `preserve_nullable` | bool | `true` | Preserve nullable columns from MySQL |
| `timestamp_precision` | int | `3` | DateTime64 precision (3=ms, 6=us) |
| `allow_destructive_ddl` | bool | `true` | Allow DROP TABLE/DROP COLUMN operations |

**Supported Engines:**

- `ReplacingMergeTree` - For single-node deployments
- `ReplicatedReplacingMergeTree` - For ClickHouse clusters

---

## Snapshot Configuration

Configure initial data snapshot behavior.

```yaml
snapshot:
  enabled: false
  chunk_size: 10000
  parallel_tables: 2
  timeout: "1h"
  resume_on_failure: true
  max_retries: 3
  retry_delay: "5s"
  lock_timeout: "30s"
  lock_strategy: "auto"
  create_missing_tables: true
  skip_data_load: false
```

### General Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Enable initial snapshot on startup |
| `timeout` | duration | `1h` | Overall snapshot timeout |
| `resume_on_failure` | bool | `true` | Resume snapshot after transient failures |

### Data Loading Settings

| Option | Type | Default | Range | Description |
|--------|------|---------|-------|-------------|
| `chunk_size` | int | `10000` | 100-1000000 | Rows per chunk during data loading |
| `parallel_tables` | int | `2` | >0 | Number of tables to snapshot in parallel |
| `skip_data_load` | bool | `false` | Skip data loading (schema and position only) |

### Table Creation Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `create_missing_tables` | bool | `true` | Auto-create missing ClickHouse tables |

### Retry Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_retries` | int | `3` | Maximum retry attempts per operation |
| `retry_delay` | duration | `5s` | Delay between retry attempts |

### Lock Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lock_timeout` | duration | `30s` | Timeout for acquiring MySQL locks |
| `lock_strategy` | string | `auto` | Locking strategy (see below) |

**Lock Strategies:**

| Strategy | MySQL Version | Description |
|----------|--------------|-------------|
| `auto` | Any | Auto-detect best available strategy |
| `flush_tables` | 5.7+ | Use `FLUSH TABLES WITH READ LOCK` |
| `backup_locks` | 8.0+ | Use `LOCK INSTANCE FOR BACKUP` |
| `none` | Any | No locking (not recommended for production) |

---

## Monitoring Configuration

Configure Prometheus metrics and health endpoints.

```yaml
monitoring:
  enabled: true
  port: 8080
  metrics_path: "/metrics"
  health_path: "/health"
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `true` | Enable metrics and health endpoints |
| `port` | int | `8080` | HTTP server port (1-65535) |
| `metrics_path` | string | `/metrics` | Prometheus metrics endpoint path |
| `health_path` | string | `/health` | Health check endpoint path |

---

## Logging Configuration

Configure application logging.

```yaml
logging:
  level: "info"
  format: "json"
  output_path: "stdout"
  max_size: 100
  max_backups: 3
  max_age: 7
  compress: true
  local_time: true
```

### Basic Settings

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `level` | string | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `format` | string | `json` | Log format: `json` or `console` |
| `output_path` | string | `stdout` | Output: `stdout`, `stderr`, or file path |

### File Rotation Settings

These settings apply only when `output_path` is a file path:

| Option | Type | Default | Range | Description |
|--------|------|---------|-------|-------------|
| `max_size` | int | `100` | 1-1000 | Max file size in MB before rotation |
| `max_backups` | int | `3` | 0-100 | Max rotated files to retain |
| `max_age` | int | `7` | 0-365 | Max age of files in days |
| `compress` | bool | `true` | Compress rotated files with gzip |
| `local_time` | bool | `true` | Use local time for filenames (false = UTC) |

---

## Observability Configuration

Configure external error reporting and log shipping.

```yaml
observability:
  error_reporting:
    enabled: false
    provider: "sentry"
    sentry:
      dsn: ""
      environment: "production"
      release: "stream-bridge@1.0.0"
      sample_rate: 1.0
      debug: false
      flush_timeout: "5s"

  log_exporting:
    enabled: false
    provider: "newrelic"
    newrelic:
      license_key: ""
      app_name: "stream-bridge"
      log_forwarding: true
      min_log_level: "info"
      flush_timeout: "5s"
```

### Error Reporting (Sentry)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `error_reporting.enabled` | bool | `false` | Enable error reporting |
| `error_reporting.provider` | string | `sentry` | Provider: `sentry` or `noop` |

**Sentry Settings:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sentry.dsn` | string | `""` | Sentry DSN from project settings |
| `sentry.environment` | string | `production` | Environment identifier |
| `sentry.release` | string | `stream-bridge@1.0.0` | Release version tag |
| `sentry.sample_rate` | float64 | `1.0` | Error sample rate (0.0-1.0) |
| `sentry.debug` | bool | `false` | Enable Sentry debug mode |
| `sentry.flush_timeout` | duration | `5s` | Event flush timeout on shutdown |

### Log Exporting (NewRelic)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `log_exporting.enabled` | bool | `false` | Enable log exporting |
| `log_exporting.provider` | string | `newrelic` | Provider: `newrelic` or `noop` |

**NewRelic Settings:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `newrelic.license_key` | string | `""` | NewRelic license key |
| `newrelic.app_name` | string | `stream-bridge` | Application name in NewRelic |
| `newrelic.log_forwarding` | bool | `true` | Forward logs to NewRelic Logs |
| `newrelic.min_log_level` | string | `info` | Minimum level to export |
| `newrelic.flush_timeout` | duration | `5s` | Log flush timeout on shutdown |

---

## Environment Variables

Configuration values can reference environment variables using the `${VAR:-default}` syntax:

```yaml
mysql:
  password: "${MYSQL_PASSWORD}"

observability:
  error_reporting:
    sentry:
      dsn: "${SENTRY_DSN:-}"
```

---

## Example Configurations

### Minimal Development Configuration

```yaml
mysql:
  host: "localhost"
  port: 3306
  username: "root"
  password: "password"
  database: "test_db"

clickhouse:
  addresses:
    - "localhost:9000"
  database: "default"
  username: "default"

logging:
  level: "debug"
  format: "console"
```

### Production Configuration

See `configs/example.yaml` for a comprehensive production configuration example.
