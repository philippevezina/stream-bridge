# ClickHouse Privileges

This document describes the required ClickHouse user privileges for Stream Bridge to operate correctly.

## Required Privileges

Stream Bridge needs the following privileges on your ClickHouse server:

### Data Operations

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `SELECT` | Target database | Read table data for validation |
| `INSERT` | Target database | Write replicated data from MySQL |

### Schema Operations (DDL Synchronization)

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `CREATE` | Target database | Create tables from MySQL DDL or during snapshot |
| `ALTER` | Target database | Modify table schemas (ADD/DROP/MODIFY COLUMN) |
| `DROP` | Target database | Drop tables (only if `schema.allow_destructive_ddl: true`) |

### System Table Access

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `SELECT` | `system.tables` | Discover existing tables |
| `SELECT` | `system.columns` | Query column metadata |

### State Storage

Stream Bridge automatically creates metadata tables for checkpoint storage. The user needs `CREATE` privilege to create these tables on first run.

Default tables created:
- `stream_bridge_checkpoints` - Binlog position checkpoints
- `stream_bridge_checkpoints_snapshots` - Snapshot progress tracking

## User Setup Examples

### Basic Setup

```sql
-- Create the user
CREATE USER 'stream_bridge' IDENTIFIED WITH plaintext_password BY 'your_secure_password';

-- Grant privileges on target database
GRANT SELECT, INSERT, CREATE, ALTER ON your_database.* TO 'stream_bridge';

-- Grant system table access for schema discovery
GRANT SELECT ON system.tables TO 'stream_bridge';
GRANT SELECT ON system.columns TO 'stream_bridge';
```

### With Destructive DDL Support

If you want Stream Bridge to replicate `DROP TABLE` and `DROP COLUMN` operations from MySQL:

```sql
CREATE USER 'stream_bridge' IDENTIFIED WITH plaintext_password BY 'your_secure_password';

-- Include DROP privilege
GRANT SELECT, INSERT, CREATE, ALTER, DROP ON your_database.* TO 'stream_bridge';

-- System table access
GRANT SELECT ON system.tables TO 'stream_bridge';
GRANT SELECT ON system.columns TO 'stream_bridge';
```

### Separate State Database

If you want to store checkpoints in a separate database:

```sql
CREATE USER 'stream_bridge' IDENTIFIED WITH plaintext_password BY 'your_secure_password';

-- Target database privileges
GRANT SELECT, INSERT, CREATE, ALTER ON analytics.* TO 'stream_bridge';

-- State database privileges
GRANT SELECT, INSERT, CREATE, ALTER ON stream_bridge_state.* TO 'stream_bridge';

-- System table access
GRANT SELECT ON system.tables TO 'stream_bridge';
GRANT SELECT ON system.columns TO 'stream_bridge';
```

Then configure in your YAML:

```yaml
clickhouse:
  database: "analytics"

state:
  type: "clickhouse"
  clickhouse:
    database: "stream_bridge_state"
    table: "checkpoints"
```

### Multiple Target Databases

If replicating to multiple ClickHouse databases:

```sql
CREATE USER 'stream_bridge' IDENTIFIED WITH plaintext_password BY 'your_secure_password';

-- Grant on each database
GRANT SELECT, INSERT, CREATE, ALTER ON database1.* TO 'stream_bridge';
GRANT SELECT, INSERT, CREATE, ALTER ON database2.* TO 'stream_bridge';

-- System table access
GRANT SELECT ON system.tables TO 'stream_bridge';
GRANT SELECT ON system.columns TO 'stream_bridge';
```

## Authentication Methods

ClickHouse supports various authentication methods:

### Plaintext Password (Development)

```sql
CREATE USER 'stream_bridge' IDENTIFIED WITH plaintext_password BY 'password';
```

### SHA256 Password (Recommended for Production)

```sql
CREATE USER 'stream_bridge' IDENTIFIED WITH sha256_password BY 'your_secure_password';
```

### Double SHA1 (MySQL Compatible)

```sql
CREATE USER 'stream_bridge' IDENTIFIED WITH double_sha1_password BY 'your_secure_password';
```

## Verifying Privileges

Check granted privileges:

```sql
SHOW GRANTS FOR 'stream_bridge';
```

Test connectivity and permissions:

```sql
-- Test SELECT on system tables
SELECT name FROM system.tables WHERE database = 'your_database' LIMIT 1;

-- Test INSERT capability
INSERT INTO your_database.test_table VALUES (1, 'test');

-- Test CREATE capability
CREATE TABLE your_database.test_permissions (id UInt32) ENGINE = MergeTree() ORDER BY id;
DROP TABLE your_database.test_permissions;
```

## State Storage Tables

Stream Bridge creates these tables automatically for checkpoint management:

### Checkpoint Table

Stores binlog positions for recovery:

```sql
CREATE TABLE IF NOT EXISTS {database}.{table} (
    id String,
    position_file String,
    position_offset UInt32,
    position_gtid String,
    last_binlog_timestamp DateTime64(3),
    created_at DateTime64(3)
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (id, created_at)
TTL toDateTime(created_at) + toIntervalSecond({retention_seconds}) DELETE
```

### Snapshot Progress Table

Tracks snapshot execution state:

```sql
CREATE TABLE IF NOT EXISTS {database}.{table}_snapshots (
    id String,
    status String,
    start_time DateTime,
    end_time Nullable(DateTime),
    total_tables Int32,
    completed_tables Int32,
    failed_tables Int32,
    tables_json String,
    error_message String,
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
```

## Schema Translation

Stream Bridge translates MySQL schemas to ClickHouse. Two special columns are added to all replicated tables:

| Column | Type | Purpose |
|--------|------|---------|
| `_version` | `UInt64` | Used by ReplacingMergeTree for deduplication |
| `_is_deleted` | `UInt8` | Soft delete marker (0 = active, 1 = deleted) |

### Supported Table Engines

Only these ClickHouse table engines are supported:

- `ReplacingMergeTree` - Default, for single-node deployments
- `ReplicatedReplacingMergeTree` - For ClickHouse clusters

Configure via `schema.default_engine` in your configuration file.

## Network Requirements

- **Port**: 9000 (native TCP protocol) or 9440 (native TCP with TLS)
- **SSL/TLS**: Configure with `clickhouse.enable_ssl: true`
- **Connection Pool**: Configured via `max_open_conns`, `max_idle_conns`, `max_lifetime`

## Security Best Practices

1. **Use strong passwords**: Generate a unique, complex password for the replication user
2. **Use SHA256 authentication**: Prefer `sha256_password` over `plaintext_password`
3. **Enable SSL/TLS**: Set `clickhouse.enable_ssl: true` for encrypted connections
4. **Minimum privileges**: Only grant `DROP` if you actually need destructive DDL replication
5. **Separate databases**: Consider using a separate database for state storage
6. **Network restrictions**: Configure ClickHouse to only accept connections from Stream Bridge hosts

## Troubleshooting

### "Code: 497. Access denied" errors

The user lacks required privileges. Check grants:

```sql
SHOW GRANTS FOR 'stream_bridge';
```

### "Table doesn't exist" during startup

The state tables haven't been created. Ensure the user has `CREATE` privilege on the state database.

### "Cannot insert into readonly table"

Check that the user has `INSERT` privilege on the target database.

### Schema sync failures

Ensure the user has `ALTER` privilege for schema modification operations, and `DROP` if you're using destructive DDL operations.
