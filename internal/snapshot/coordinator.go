package snapshot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/mysql/connector"
)

// LockStrategy represents the locking strategy to use for consistency
type LockStrategy int

const (
	LockStrategyFlushTables LockStrategy = iota
	LockStrategyBackupLocks
	LockStrategyNone
)

type Coordinator struct {
	cfg          *config.MySQLConfig
	snapshotCfg  *config.SnapshotConfig
	logger       *zap.Logger
	connector    *connector.Connector
	conn         *client.Conn
	lockStrategy LockStrategy
}

func NewCoordinator(cfg *config.MySQLConfig, snapshotCfg *config.SnapshotConfig, logger *zap.Logger) *Coordinator {
	return &Coordinator{
		cfg:         cfg,
		snapshotCfg: snapshotCfg,
		logger:      logger,
		connector:   connector.New(cfg, logger),
	}
}

func (c *Coordinator) Start(ctx context.Context) error {
	if err := c.connect(); err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	// Determine lock strategy based on MySQL 8.0+ capabilities
	if err := c.detectLockStrategy(); err != nil {
		return fmt.Errorf("failed to determine lock strategy: %w", err)
	}

	c.logger.Info("Consistency coordinator started",
		zap.String("lock_strategy", c.getLockStrategyName()))
	return nil
}

func (c *Coordinator) Stop() error {
	if c.conn != nil {
		c.conn.Close()
	}
	c.logger.Info("Consistency coordinator stopped")
	return nil
}

func (c *Coordinator) detectLockStrategy() error {
	// Determine lock strategy based on configuration and MySQL 8.0+ capability
	strategy, err := c.determineLockStrategy()
	if err != nil {
		return fmt.Errorf("failed to determine lock strategy: %w", err)
	}
	c.lockStrategy = strategy

	return nil
}

func (c *Coordinator) determineLockStrategy() (LockStrategy, error) {
	c.logger.Info("Determining lock strategy",
		zap.String("configured_strategy", c.snapshotCfg.LockStrategy))

	switch c.snapshotCfg.LockStrategy {
	case "none", "no_lock":
		c.logger.Warn("Using no_lock strategy - snapshot position may be inconsistent")
		return LockStrategyNone, nil
	case "flush_tables":
		c.logger.Info("Using forced flush_tables strategy")
		return LockStrategyFlushTables, nil
	case "backup_locks":
		c.logger.Info("Attempting to use forced backup_locks strategy")
		c.logger.Debug("Testing MySQL 8.0+ backup locks support...")
		if supported, err := c.supportsBackupLocks(); err != nil || !supported {
			c.logger.Warn("Backup locks not supported, falling back to flush tables",
				zap.Error(err), zap.Bool("supported", supported))
			return LockStrategyFlushTables, nil
		}
		c.logger.Info("MySQL 8.0+ backup locks supported and will be used")
		return LockStrategyBackupLocks, nil
	case "auto":
		c.logger.Info("Using auto detection strategy")
		c.logger.Debug("Testing MySQL 8.0+ backup locks support...")
		if supported, err := c.supportsBackupLocks(); err == nil && supported {
			c.logger.Info("MySQL 8.0+ backup locks supported, using backup_locks strategy")
			return LockStrategyBackupLocks, nil
		} else {
			c.logger.Info("MySQL 8.0+ backup locks not supported, using flush_tables strategy",
				zap.Error(err), zap.Bool("supported", supported))
		}
		return LockStrategyFlushTables, nil
	default:
		c.logger.Error("Unknown lock strategy configured", zap.String("strategy", c.snapshotCfg.LockStrategy))
		return LockStrategyFlushTables, fmt.Errorf("unknown lock strategy: %s", c.snapshotCfg.LockStrategy)
	}
}

func (c *Coordinator) supportsBackupLocks() (bool, error) {
	c.logger.Debug("Testing if MySQL 8.0+ backup locks are supported...")

	// Test 1: Check if LOCK INSTANCE FOR BACKUP is supported
	_, err := c.conn.Execute("LOCK INSTANCE FOR BACKUP")
	if err != nil {
		c.logger.Debug("LOCK INSTANCE FOR BACKUP failed", zap.Error(err))
		return false, err
	}
	c.logger.Debug("LOCK INSTANCE FOR BACKUP succeeded")

	// Test 2: Check if performance_schema.log_status is accessible
	_, err = c.conn.Execute("SELECT * FROM performance_schema.log_status LIMIT 0")
	if err != nil {
		c.logger.Debug("performance_schema.log_status access failed", zap.Error(err))
		// Unlock instance before returning
		c.conn.Execute("UNLOCK INSTANCE")
		return false, err
	}
	c.logger.Debug("performance_schema.log_status access succeeded")

	// Clean up test lock
	_, unlockErr := c.conn.Execute("UNLOCK INSTANCE")
	if unlockErr != nil {
		c.logger.Warn("Failed to unlock instance after backup lock test", zap.Error(unlockErr))
	} else {
		c.logger.Debug("Successfully unlocked after backup lock test")
	}

	c.logger.Info("MySQL 8.0+ backup locks are fully supported")
	return true, nil
}

func (c *Coordinator) getLockStrategyName() string {
	switch c.lockStrategy {
	case LockStrategyBackupLocks:
		return "mysql8_instance_backup_lock"
	case LockStrategyNone:
		return "none"
	default:
		return "flush_tables"
	}
}

func (c *Coordinator) CaptureConsistentPosition(ctx context.Context) (*common.BinlogPosition, error) {
	c.logger.Info("Capturing consistent binlog position",
		zap.String("lock_strategy", c.getLockStrategyName()))

	if c.conn == nil {
		if err := c.connect(); err != nil {
			return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
		}
	}

	var position *common.BinlogPosition
	var err error

	switch c.lockStrategy {
	case LockStrategyBackupLocks:
		position, err = c.captureWithBackupLocks()
	case LockStrategyNone:
		position, err = c.captureWithNoLock()
	default:
		position, err = c.captureWithFlushTables()
	}

	if err != nil {
		return nil, err
	}

	c.logger.Info("Consistent position captured",
		zap.String("file", position.File),
		zap.Uint32("offset", position.Offset),
		zap.String("gtid", position.GTID),
		zap.String("strategy", c.getLockStrategyName()))

	return position, nil
}

func (c *Coordinator) captureWithBackupLocks() (*common.BinlogPosition, error) {
	c.logger.Debug("Using MySQL 8.0+ LOCK INSTANCE FOR BACKUP for consistent position capture")

	// Step 1: Lock instance for backup (prevents file operations, allows DML)
	if err := c.lockInstanceForBackup(); err != nil {
		return nil, fmt.Errorf("failed to lock instance for backup: %w", err)
	}

	// Ensure unlock always happens, even on error paths
	defer func() {
		if unlockErr := c.unlockInstance(); unlockErr != nil {
			c.logger.Error("CRITICAL: Failed to unlock MySQL instance - manual intervention may be required",
				zap.Error(unlockErr))
		}
	}()

	// Step 2: Capture consistent position from performance_schema.log_status
	position, err := c.getPositionFromLogStatus()
	if err != nil {
		return nil, fmt.Errorf("failed to get position from log_status: %w", err)
	}

	c.logger.Debug("Consistent position captured with MySQL 8.0+ backup locks")

	return position, nil
}

func (c *Coordinator) captureWithFlushTables() (*common.BinlogPosition, error) {
	c.logger.Debug("Using FLUSH TABLES WITH READ LOCK for consistent position capture")

	// Lock all tables
	if err := c.flushTablesWithReadLock(); err != nil {
		return nil, fmt.Errorf("failed to flush tables with read lock: %w", err)
	}

	// Ensure unlock always happens, even on error paths
	defer func() {
		if unlockErr := c.unlockTables(); unlockErr != nil {
			c.logger.Error("CRITICAL: Failed to unlock MySQL tables - manual intervention may be required",
				zap.Error(unlockErr))
		}
	}()

	// Capture the current binlog position
	position, err := c.getCurrentBinlogPosition()
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog position: %w", err)
	}

	return position, nil
}

func (c *Coordinator) captureWithNoLock() (*common.BinlogPosition, error) {
	c.logger.Warn("Capturing binlog position WITHOUT locking - position may be inconsistent with snapshot data")

	// Simply capture the current binlog position without any locking
	position, err := c.getCurrentBinlogPosition()
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog position: %w", err)
	}

	return position, nil
}

func (c *Coordinator) lockInstanceForBackup() error {
	c.logger.Debug("Acquiring instance backup lock")

	query := "LOCK INSTANCE FOR BACKUP"
	if _, err := c.conn.Execute(query); err != nil {
		return fmt.Errorf("failed to execute LOCK INSTANCE FOR BACKUP: %w", err)
	}

	c.logger.Debug("Instance backup lock acquired")
	return nil
}

func (c *Coordinator) unlockInstance() error {
	c.logger.Debug("Releasing instance backup lock")

	query := "UNLOCK INSTANCE"
	if _, err := c.conn.Execute(query); err != nil {
		return fmt.Errorf("failed to execute UNLOCK INSTANCE: %w", err)
	}

	c.logger.Debug("Instance backup lock released")
	return nil
}

func (c *Coordinator) getPositionFromLogStatus() (*common.BinlogPosition, error) {
	c.logger.Debug("Getting consistent position from performance_schema.log_status")

	// Query the log_status table for consistent binlog position and GTID information
	query := "SELECT * FROM performance_schema.log_status"
	result, err := c.conn.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query performance_schema.log_status: %w", err)
	}

	if result.RowNumber() == 0 {
		return nil, fmt.Errorf("no data returned from performance_schema.log_status")
	}

	// Parse the log_status result
	// Expected columns: SERVER_UUID, LOCAL, REPLICATION, STORAGE_ENGINES
	if result.ColumnNumber() < 4 {
		return nil, fmt.Errorf("unexpected number of columns in log_status result: %d", result.ColumnNumber())
	}

	// Get LOCAL column data first (index 1) which contains binary log position
	localData, err := result.GetString(0, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to get local data from log_status: %w", err)
	}

	c.logger.Debug("Raw local data from log_status", zap.String("data", localData))

	// Try to parse binary log position from LOCAL column
	position, err := c.parseLogStatusLocal(localData)
	if err != nil {
		c.logger.Warn("Failed to parse LOCAL column, trying REPLICATION column as fallback", zap.Error(err))

		// Fallback to REPLICATION column
		replicationData, err := result.GetString(0, 2)
		if err != nil {
			return nil, fmt.Errorf("failed to get replication data from log_status: %w", err)
		}

		c.logger.Debug("Raw replication data from log_status", zap.String("data", replicationData))

		position, err = c.parseLogStatusReplication(replicationData)
		if err != nil {
			return nil, fmt.Errorf("failed to parse both LOCAL and REPLICATION data: %w", err)
		}
	}

	c.logger.Info("Position extracted from log_status",
		zap.String("file", position.File),
		zap.Uint32("offset", position.Offset),
		zap.String("gtid", position.GTID))

	return position, nil
}

func (c *Coordinator) parseLogStatusLocal(localData string) (*common.BinlogPosition, error) {
	// Define the structure for parsing the LOCAL JSON data
	// LOCAL column contains binary log state information
	type LogStatusLocal struct {
		BinaryLogFile     string `json:"binary_log_file"`
		BinaryLogPosition uint32 `json:"binary_log_position"`
		GtidExecuted      string `json:"gtid_executed,omitempty"`
	}

	var local LogStatusLocal
	if err := json.Unmarshal([]byte(localData), &local); err != nil {
		return nil, fmt.Errorf("failed to parse local JSON: %w", err)
	}

	if local.BinaryLogFile == "" {
		return nil, fmt.Errorf("no binary log file found in local data")
	}

	c.logger.Debug("Found binary log position from LOCAL column",
		zap.String("binary_log_file", local.BinaryLogFile),
		zap.Uint32("binary_log_position", local.BinaryLogPosition),
		zap.String("gtid_executed", local.GtidExecuted))

	position := &common.BinlogPosition{
		File:   local.BinaryLogFile,
		Offset: local.BinaryLogPosition,
	}

	// Add GTID if available
	if local.GtidExecuted != "" {
		position.GTID = local.GtidExecuted
	}

	return position, nil
}

func (c *Coordinator) parseLogStatusReplication(replicationData string) (*common.BinlogPosition, error) {
	// Define the structure for parsing the replication JSON data
	// MySQL 8.4 returns a direct array, not an object with "channels" key
	type ReplicationChannel struct {
		ChannelName  string `json:"channel_name"`
		RelayLogFile string `json:"relay_log_file"`
		RelayLogPos  uint32 `json:"relay_log_pos"`
	}

	var channels []ReplicationChannel
	if err := json.Unmarshal([]byte(replicationData), &channels); err != nil {
		c.logger.Error("Failed to parse replication JSON",
			zap.Error(err),
			zap.String("raw_data", replicationData))
		return nil, fmt.Errorf("failed to parse replication JSON: %w", err)
	}

	c.logger.Debug("Parsed replication channels",
		zap.Int("channels_count", len(channels)),
		zap.Any("channels", channels))

	// Find the main channel (empty channel name indicates the main replication channel)
	var relayLogFile string
	var relayLogPos uint32
	mainChannelFound := false

	for i, channel := range channels {
		c.logger.Debug("Examining replication channel",
			zap.Int("index", i),
			zap.String("channel_name", channel.ChannelName),
			zap.String("relay_log_file", channel.RelayLogFile),
			zap.Uint32("relay_log_pos", channel.RelayLogPos))

		if channel.ChannelName == "" {
			relayLogFile = channel.RelayLogFile
			relayLogPos = channel.RelayLogPos
			mainChannelFound = true
			c.logger.Debug("Found main replication channel", zap.Int("at_index", i))
			break
		}
	}

	if !mainChannelFound {
		c.logger.Warn("No main replication channel found",
			zap.Int("total_channels", len(channels)),
			zap.String("suggestion", "This may be a source server without replication, or the channel structure has changed"))

		// Log all channel names for debugging
		channelNames := make([]string, len(channels))
		for i, ch := range channels {
			channelNames[i] = fmt.Sprintf("'%s'", ch.ChannelName)
		}
		c.logger.Debug("Available channel names", zap.Strings("channel_names", channelNames))

		return nil, fmt.Errorf("no main replication channel found in replication data - found %d channels, none with empty name", len(channels))
	}

	// Note: relay log position might not be what we want for CDC
	// This function may need to be updated to use LOCAL column instead
	c.logger.Debug("Found main replication channel",
		zap.String("relay_log_file", relayLogFile),
		zap.Uint32("relay_log_pos", relayLogPos))

	position := &common.BinlogPosition{
		File:   relayLogFile,
		Offset: relayLogPos,
	}

	// TODO: Extract GTID from a separate source since it's not in replication channels

	return position, nil
}

func (c *Coordinator) flushTablesWithReadLock() error {
	c.logger.Debug("Acquiring global read lock")

	// This will block until all currently executing statements finish
	// and prevent new statements from starting
	query := "FLUSH TABLES WITH READ LOCK"

	if _, err := c.conn.Execute(query); err != nil {
		return fmt.Errorf("failed to execute FLUSH TABLES WITH READ LOCK: %w", err)
	}

	c.logger.Debug("Global read lock acquired")
	return nil
}

func (c *Coordinator) unlockTables() error {
	c.logger.Debug("Releasing global read lock")

	query := "UNLOCK TABLES"
	if _, err := c.conn.Execute(query); err != nil {
		return fmt.Errorf("failed to execute UNLOCK TABLES: %w", err)
	}

	c.logger.Debug("Global read lock released")
	return nil
}

func (c *Coordinator) getCurrentBinlogPosition() (*common.BinlogPosition, error) {
	// Get the current master status
	result, err := c.conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		return nil, fmt.Errorf("failed to execute SHOW MASTER STATUS: %w", err)
	}

	if result.RowNumber() == 0 {
		return nil, fmt.Errorf("no master status found - is binary logging enabled?")
	}

	// Extract binlog file and position
	file, err := result.GetString(0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog file: %w", err)
	}

	position, err := result.GetUint(0, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog position: %w", err)
	}

	binlogPos := &common.BinlogPosition{
		File:   file,
		Offset: uint32(position),
	}

	// Try to get GTID if available (MySQL 5.6+)
	if result.ColumnNumber() >= 5 {
		gtid, err := result.GetString(0, 4)
		if err == nil && gtid != "" {
			binlogPos.GTID = gtid
		}
	}

	return binlogPos, nil
}

func (c *Coordinator) connect() error {
	conn, err := c.connector.Connect(c.cfg.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	c.conn = conn
	return nil
}
