# MySQL Privileges

This document describes the required MySQL user privileges for Stream Bridge to operate correctly.

## Server Requirements

Before configuring user privileges, ensure your MySQL server meets these requirements:

- **MySQL Version**: 8.0.1 or later
- **Binary Logging**: Enabled with ROW format
- **Binlog Row Metadata**: Set to FULL (required for column name extraction)

```sql
-- Verify server settings
SHOW VARIABLES LIKE 'log_bin';           -- Must be ON
SHOW VARIABLES LIKE 'binlog_format';     -- Must be ROW
SHOW VARIABLES LIKE 'binlog_row_metadata'; -- Must be FULL
```

If these settings need to be changed, add to your MySQL configuration (`my.cnf`):

```ini
[mysqld]
log_bin = ON
binlog_format = ROW
binlog_row_metadata = FULL
```

## Required Privileges

Stream Bridge requires different privilege sets depending on which features you use.

### CDC (Change Data Capture) - Always Required

These privileges are required for binlog-based replication:

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `REPLICATION CLIENT` | Global (`*.*`) | Execute `SHOW MASTER STATUS` and `SHOW BINARY LOGS` |
| `REPLICATION SLAVE` | Global (`*.*`) | Read the binary log stream |
| `SELECT` | Target database(s) | Read table data and metadata |

### Snapshot - Required if `snapshot.enabled: true`

Additional privileges for initial data snapshot:

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `PROCESS` | Global (`*.*`) | Monitor query execution |
| `SELECT` | `information_schema.*` | Query column metadata |

#### Lock Strategy Privileges

Depending on your `snapshot.lock_strategy` setting:

**For `flush_tables` strategy (MySQL 5.7+):**

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `RELOAD` | Global (`*.*`) | Execute `FLUSH TABLES WITH READ LOCK` |

**For `backup_locks` strategy (MySQL 8.0+, recommended):**

| Privilege | Scope | Purpose |
|-----------|-------|---------|
| `BACKUP_ADMIN` | Global (`*.*`) | Execute `LOCK INSTANCE FOR BACKUP` |

**For `auto` strategy (default):**

Grant both `RELOAD` and `BACKUP_ADMIN`. The system will automatically use `backup_locks` if available.

## User Setup Examples

### Minimal Setup (CDC Only)

Use this if you're not using snapshots and will manage initial data load manually:

```sql
CREATE USER 'stream_bridge'@'%' IDENTIFIED BY 'your_secure_password';

-- Replication privileges
GRANT REPLICATION CLIENT ON *.* TO 'stream_bridge'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'stream_bridge'@'%';

-- Data access for your database(s)
GRANT SELECT ON your_database.* TO 'stream_bridge'@'%';

FLUSH PRIVILEGES;
```

### Recommended Setup (CDC + Snapshot with Backup Locks)

For MySQL 8.0+ with full snapshot support using the least disruptive locking:

```sql
CREATE USER 'stream_bridge'@'%' IDENTIFIED BY 'your_secure_password';

-- Replication privileges
GRANT REPLICATION CLIENT ON *.* TO 'stream_bridge'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'stream_bridge'@'%';

-- Data access
GRANT SELECT ON your_database.* TO 'stream_bridge'@'%';
GRANT SELECT ON information_schema.* TO 'stream_bridge'@'%';

-- Snapshot privileges
GRANT PROCESS ON *.* TO 'stream_bridge'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'stream_bridge'@'%';

FLUSH PRIVILEGES;
```

### Full Setup (All Features)

For maximum compatibility across MySQL versions:

```sql
CREATE USER 'stream_bridge'@'%' IDENTIFIED BY 'your_secure_password';

-- Replication privileges
GRANT REPLICATION CLIENT ON *.* TO 'stream_bridge'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'stream_bridge'@'%';

-- Data access
GRANT SELECT ON your_database.* TO 'stream_bridge'@'%';
GRANT SELECT ON information_schema.* TO 'stream_bridge'@'%';

-- Snapshot privileges
GRANT PROCESS ON *.* TO 'stream_bridge'@'%';
GRANT RELOAD ON *.* TO 'stream_bridge'@'%';
GRANT BACKUP_ADMIN ON *.* TO 'stream_bridge'@'%';

FLUSH PRIVILEGES;
```

### Multiple Databases

If replicating multiple databases:

```sql
-- Option 1: Grant SELECT on each database explicitly
GRANT SELECT ON database1.* TO 'stream_bridge'@'%';
GRANT SELECT ON database2.* TO 'stream_bridge'@'%';
GRANT SELECT ON database3.* TO 'stream_bridge'@'%';

-- Option 2: Grant SELECT globally (broader access)
GRANT SELECT ON *.* TO 'stream_bridge'@'%';
```

## Verifying Privileges

After creating the user, verify the grants:

```sql
SHOW GRANTS FOR 'stream_bridge'@'%';
```

Test the replication connection:

```sql
-- These should work with REPLICATION CLIENT
SHOW MASTER STATUS;
SHOW BINARY LOGS;
```

## Lock Strategy Comparison

| Strategy | MySQL Version | Privilege | Behavior | Impact |
|----------|--------------|-----------|----------|--------|
| `flush_tables` | 5.7+ | `RELOAD` | Global read lock | Blocks all writes during snapshot position capture |
| `backup_locks` | 8.0+ | `BACKUP_ADMIN` | Instance backup lock | Allows DML, blocks DDL during snapshot |
| `none` | Any | None | No locking | Inconsistent snapshot (not recommended for production) |
| `auto` | Any | Both | Auto-detect best | Uses `backup_locks` if available |

**Recommendation**: Use `backup_locks` (or `auto`) on MySQL 8.0+ for minimal disruption to your application.

## Security Best Practices

1. **Use strong passwords**: The replication user should have a strong, unique password
2. **Restrict host access**: Replace `'%'` with specific IP addresses or hostnames when possible
3. **Use SSL/TLS**: Configure `mysql.ssl_mode` in Stream Bridge for encrypted connections
4. **Minimum privileges**: Only grant the privileges you need based on your configuration
5. **Separate user**: Don't use the same MySQL user for application access and replication

## Troubleshooting

### "Access denied for user" errors

Verify the user has the required privileges:

```sql
SHOW GRANTS FOR 'stream_bridge'@'%';
```

### "binlog_row_metadata is MINIMAL" warnings

The `binlog_row_metadata` must be set to `FULL`:

```sql
SET GLOBAL binlog_row_metadata = 'FULL';
```

Or add to `my.cnf` and restart MySQL.

### "LOCK INSTANCE FOR BACKUP" not supported

Your MySQL version doesn't support backup locks. Either:
- Upgrade to MySQL 8.0+
- Use `snapshot.lock_strategy: flush_tables`
- Grant `RELOAD` privilege instead of `BACKUP_ADMIN`
