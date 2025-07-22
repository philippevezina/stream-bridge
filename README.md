# Stream Bridge - MySQL to ClickHouse Replicator

A robust, production-ready replication system that captures changes from MySQL databases using Change Data Capture (CDC) and applies them to ClickHouse in real-time.

## Features

- **Real-time CDC**: Captures MySQL binary log changes with minimal latency
- **Position Persistence**: Automatic checkpoint creation and recovery for reliable restarts
- **Schema Synchronization**: Automatic translation and synchronization of DDL changes
- **Batch Processing**: Optimized bulk operations for high throughput
- **Error Handling**: Comprehensive retry logic and error recovery
- **Monitoring**: Prometheus metrics and health checks with checkpoint tracking
- **Flexible Filtering**: Regex and explicit table filtering
- **Initial Snapshots**: Consistent initial data load with multiple locking strategies
- **Production Ready**: Structured logging, graceful shutdown, and configuration management

## Quick Start

### Prerequisites

- Go 1.21 or later
- MySQL 8.0.1+ with binary logging enabled in ROW format
- ClickHouse 20.8+

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd stream-bridge

# Build the application
go build -o stream-bridge cmd/stream-bridge/main.go
```

### Database Setup

#### MySQL Server Configuration

The MySQL server must be **MySQL 8.0.1 or later** with the following settings:

```sql
-- Check current settings
SHOW VARIABLES LIKE 'log_bin';            -- Must be ON
SHOW VARIABLES LIKE 'binlog_format';      -- Must be ROW
SHOW VARIABLES LIKE 'binlog_row_metadata'; -- Must be FULL
```

Required settings (add to `my.cnf`):

```ini
[mysqld]
log_bin = ON
binlog_format = ROW
binlog_row_metadata = FULL
```

The `binlog_row_metadata=FULL` setting is **required** for proper column name extraction from binlog events.

#### MySQL User Setup

Create a MySQL user with replication privileges:

```sql
CREATE USER 'stream_bridge'@'%' IDENTIFIED BY 'your_password';

-- CDC privileges (always required)
GRANT REPLICATION CLIENT ON *.* TO 'stream_bridge'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'stream_bridge'@'%';
GRANT SELECT ON your_database.* TO 'stream_bridge'@'%';

-- Snapshot privileges (if using snapshots)
GRANT PROCESS ON *.* TO 'stream_bridge'@'%';
GRANT SELECT ON information_schema.* TO 'stream_bridge'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'stream_bridge'@'%';  -- MySQL 8.0+

FLUSH PRIVILEGES;
```

See [docs/mysql-privileges.md](docs/mysql-privileges.md) for detailed privilege documentation.

#### ClickHouse User Setup

Create a ClickHouse user with appropriate privileges:

```sql
CREATE USER 'stream_bridge' IDENTIFIED WITH plaintext_password BY 'your_password';

GRANT SELECT, INSERT, CREATE, ALTER ON your_database.* TO 'stream_bridge';
GRANT SELECT ON system.tables TO 'stream_bridge';
GRANT SELECT ON system.columns TO 'stream_bridge';
```

See [docs/clickhouse-privileges.md](docs/clickhouse-privileges.md) for detailed privilege documentation.

### Configuration

Create a configuration file based on the examples in `configs/`:

```bash
# Copy and edit the minimal configuration
cp configs/minimal.yaml config.yaml
```

Key configuration sections:

- `mysql`: Source database connection and filtering
- `clickhouse`: Target database connection settings
- `pipeline`: Batch processing and performance tuning
- `state`: Checkpoint storage and persistence settings
- `schema`: Schema translation and DDL handling
- `snapshot`: Initial data snapshot settings
- `monitoring`: Metrics and health check endpoints
- `logging`: Log level and output format

See [docs/configuration.md](docs/configuration.md) for complete configuration reference.

### Running

```bash
# Start with default configuration
./stream-bridge -config config.yaml

# Show version
./stream-bridge -version
```

## Documentation

- [Architecture](docs/architecture.md) - System design and event flow
- [Configuration Reference](docs/configuration.md) - Complete configuration options
- [MySQL Privileges](docs/mysql-privileges.md) - Required MySQL user privileges
- [ClickHouse Privileges](docs/clickhouse-privileges.md) - Required ClickHouse user privileges

## Architecture

The replicator consists of several key components:

1. **MySQL CDC Engine** (`internal/mysql/`): Connects to MySQL binlog stream and parses events
2. **ClickHouse Client** (`internal/clickhouse/`): Handles target database operations
3. **Event Pipeline** (`internal/pipeline/`): Processes and batches events for optimal performance
4. **State Management** (`internal/state/`): Checkpoint persistence and position recovery
5. **Configuration System** (`internal/config/`): Manages application settings with validation
6. **Metrics & Monitoring** (`internal/metrics/`): Prometheus metrics and health endpoints

### Event Flow

```
MySQL Binlog → CDC Engine → Event Router → Batcher/DDL Handler → Workers → ClickHouse
                                                    │
                                            Checkpoint Manager
```

See [Architecture Documentation](docs/architecture.md) for detailed event flow and internal design.

## Monitoring

The application exposes metrics and health endpoints:

- **Metrics**: `http://localhost:8080/metrics` (Prometheus format)
- **Health Check**: `http://localhost:8080/health`

Key metrics include:

- `stream_bridge_events_processed_total`: Total events processed
- `stream_bridge_replication_lag_seconds`: Current replication lag
- `stream_bridge_connection_status`: Database connection status
- `stream_bridge_checkpoints_created_total`: Total checkpoints created
- `stream_bridge_checkpoint_age_seconds`: Age of the last checkpoint

## Development

### Project Structure

```
stream-bridge/
├── cmd/stream-bridge/          # Main application
├── internal/
│   ├── mysql/                  # MySQL CDC implementation
│   ├── clickhouse/             # ClickHouse client
│   ├── schema/                 # Schema management
│   ├── pipeline/               # Event processing
│   ├── config/                 # Configuration
│   ├── metrics/                # Monitoring
│   └── common/                 # Shared utilities
├── configs/                    # Example configurations
├── docs/                       # Documentation
└── tests/                      # Tests
```

### Building

```bash
# Build all packages
go build ./...

# Run tests
go test ./...

# Build with race detection
go build -race ./...
```

## Contributing

1. Follow Go best practices and conventions
2. Add tests for new functionality
3. Update documentation for configuration changes
4. Ensure all builds pass: `go build ./...`
