package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/security"
)

type ClickHouseStorage struct {
	client    *clickhouse.Client
	logger    *zap.Logger
	config    Config
	tableName string
}

func NewClickHouseStorage(client *clickhouse.Client, logger *zap.Logger, config Config) *ClickHouseStorage {
	tableName := config.ClickHouse.Table
	if tableName == "" {
		tableName = "stream_bridge_checkpoints"
	}

	return &ClickHouseStorage{
		client:    client,
		logger:    logger,
		config:    config,
		tableName: tableName,
	}
}

func (s *ClickHouseStorage) Initialize(ctx context.Context) error {
	if err := s.createCheckpointTable(ctx); err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	if err := s.createSnapshotTable(ctx); err != nil {
		return fmt.Errorf("failed to create snapshot table: %w", err)
	}

	s.logger.Info("ClickHouse state storage initialized",
		zap.String("table", s.tableName))

	return nil
}

func (s *ClickHouseStorage) Close() error {
	return nil
}

// getFullTableName returns the fully qualified table name (database.table) with proper escaping
func (s *ClickHouseStorage) getFullTableName() string {
	database := s.config.ClickHouse.Database
	if database == "" {
		database = "default"
	}

	// SECURITY: Validate and escape identifiers
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		s.logger.Error("Invalid database name in config", zap.Error(err))
		return ""
	}
	if err := security.ValidateIdentifier(s.tableName, "table name"); err != nil {
		s.logger.Error("Invalid table name in config", zap.Error(err))
		return ""
	}

	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(s.tableName)
	return fmt.Sprintf("%s.%s", escapedDB, escapedTable)
}

// getFullSnapshotTableName returns the fully qualified snapshot table name (database.table_snapshots) with proper escaping
func (s *ClickHouseStorage) getFullSnapshotTableName() string {
	database := s.config.ClickHouse.Database
	if database == "" {
		database = "default"
	}

	// SECURITY: Validate and escape identifiers
	// Snapshot table name is base table name + "_snapshots" suffix
	snapshotTableName := s.tableName + "_snapshots"

	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		s.logger.Error("Invalid database name in config", zap.Error(err))
		return ""
	}
	if err := security.ValidateIdentifier(snapshotTableName, "snapshot table name"); err != nil {
		s.logger.Error("Invalid snapshot table name", zap.Error(err))
		return ""
	}

	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(snapshotTableName)
	return fmt.Sprintf("%s.%s", escapedDB, escapedTable)
}

func (s *ClickHouseStorage) SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, position_file, position_offset, position_gtid,
			last_binlog_timestamp, created_at
		) VALUES (?, ?, ?, ?, ?, ?)
	`, s.getFullTableName())

	err := s.executeQuery(ctx, query,
		checkpoint.ID,
		checkpoint.Position.File,
		checkpoint.Position.Offset,
		checkpoint.Position.GTID,
		checkpoint.LastBinlogTimestamp,
		checkpoint.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	s.logger.Debug("Checkpoint saved",
		zap.String("id", checkpoint.ID),
		zap.String("position", fmt.Sprintf("%s:%d", checkpoint.Position.File, checkpoint.Position.Offset)))

	return nil
}

func (s *ClickHouseStorage) GetLatestCheckpoint(ctx context.Context) (*Checkpoint, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, position_file, position_offset, position_gtid,
			last_binlog_timestamp, created_at
		FROM %s 
		ORDER BY created_at DESC 
		LIMIT 1
	`, s.getFullTableName())

	row, err := s.queryRow(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest checkpoint: %w", err)
	}
	checkpoint, err := s.scanCheckpoint(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	s.logger.Debug("Retrieved latest checkpoint",
		zap.String("id", checkpoint.ID),
		zap.String("position", fmt.Sprintf("%s:%d", checkpoint.Position.File, checkpoint.Position.Offset)))

	return checkpoint, nil
}

func (s *ClickHouseStorage) GetCheckpoint(ctx context.Context, id string) (*Checkpoint, error) {
	query := fmt.Sprintf(`
		SELECT 
			id, position_file, position_offset, position_gtid,
			last_binlog_timestamp, created_at
		FROM %s 
		WHERE id = ?
		LIMIT 1
	`, s.getFullTableName())

	row, err := s.queryRow(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint: %w", err)
	}
	checkpoint, err := s.scanCheckpoint(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("checkpoint not found: %s", id)
		}
		return nil, fmt.Errorf("failed to get checkpoint: %w", err)
	}

	return checkpoint, nil
}

func (s *ClickHouseStorage) ListCheckpoints(ctx context.Context, limit int) ([]*Checkpoint, error) {
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT 
			id, position_file, position_offset, position_gtid,
			last_binlog_timestamp, created_at
		FROM %s 
		ORDER BY created_at DESC 
		LIMIT %d
	`, s.getFullTableName(), limit)

	rows, err := s.query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}
	defer rows.Close()

	var checkpoints []*Checkpoint
	for rows.Next() {
		checkpoint, err := s.scanCheckpoint(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan checkpoint: %w", err)
		}
		checkpoints = append(checkpoints, checkpoint)
	}

	return checkpoints, nil
}

func (s *ClickHouseStorage) HealthCheck(ctx context.Context) error {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s LIMIT 1", s.getFullTableName())

	row, err := s.queryRow(ctx, query)
	if err != nil {
		return fmt.Errorf("checkpoint table health check failed: %w", err)
	}
	var count int64
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("checkpoint table health check failed: %w", err)
	}

	return nil
}

func (s *ClickHouseStorage) createCheckpointTable(ctx context.Context) error {
	database := s.config.ClickHouse.Database
	if database == "" {
		database = "default"
	}

	// SECURITY: Validate identifiers before using in SQL
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return fmt.Errorf("invalid database name in config: %w", err)
	}
	if err := security.ValidateIdentifier(s.tableName, "table name"); err != nil {
		return fmt.Errorf("invalid table name in config: %w", err)
	}

	// Convert retention period to seconds for TTL
	retentionSeconds := int64(s.config.RetentionPeriod.Seconds())

	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(s.tableName)

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id String,
			position_file String,
			position_offset UInt32,
			position_gtid String,
			last_binlog_timestamp DateTime64(3),
			created_at DateTime64(3)
		) ENGINE = ReplacingMergeTree(created_at)
		ORDER BY (id, created_at)
		TTL toDateTime(created_at) + toIntervalSecond(%d) DELETE
	`, escapedDB, escapedTable, retentionSeconds)

	err := s.executeQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint table: %w", err)
	}

	s.logger.Info("Checkpoint table created",
		zap.String("database", database),
		zap.String("table", s.tableName))

	return nil
}

func (s *ClickHouseStorage) executeQuery(ctx context.Context, query string, args ...any) error {
	db := s.client.GetDB()
	if db == nil {
		return fmt.Errorf("ClickHouse connection not available")
	}

	_, err := db.ExecContext(ctx, query, args...)
	return err
}

func (s *ClickHouseStorage) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db := s.client.GetDB()
	if db == nil {
		return nil, fmt.Errorf("ClickHouse connection not available")
	}

	return db.QueryContext(ctx, query, args...)
}

func (s *ClickHouseStorage) queryRow(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	db := s.client.GetDB()
	if db == nil {
		return nil, fmt.Errorf("ClickHouse connection not available")
	}
	return db.QueryRowContext(ctx, query, args...), nil
}

func (s *ClickHouseStorage) scanCheckpoint(scanner interface {
	Scan(dest ...any) error
}) (*Checkpoint, error) {
	var (
		id                  string
		positionFile        string
		positionOffset      uint32
		positionGTID        string
		lastBinlogTimestamp time.Time
		createdAt           time.Time
	)

	err := scanner.Scan(
		&id, &positionFile, &positionOffset, &positionGTID,
		&lastBinlogTimestamp, &createdAt,
	)
	if err != nil {
		return nil, err
	}

	return &Checkpoint{
		ID: id,
		Position: common.BinlogPosition{
			File:   positionFile,
			Offset: positionOffset,
			GTID:   positionGTID,
		},
		LastBinlogTimestamp: lastBinlogTimestamp,
		CreatedAt:           createdAt,
	}, nil
}

// Snapshot-related methods

func (s *ClickHouseStorage) SaveSnapshotProgress(ctx context.Context, progress *SnapshotProgress) error {
	// Create snapshot table if it doesn't exist
	if err := s.createSnapshotTable(ctx); err != nil {
		return fmt.Errorf("failed to create snapshot table: %w", err)
	}

	tablesJSON, err := json.Marshal(progress.Tables)
	if err != nil {
		return fmt.Errorf("failed to marshal tables: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, status, start_time, end_time,
			total_tables, completed_tables, failed_tables, tables_json, error_message,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, s.getFullSnapshotTableName())

	// Handle EndTime pointer properly for ClickHouse Nullable(DateTime)
	var endTimeForDB any
	if progress.EndTime != nil {
		endTimeForDB = *progress.EndTime
	} else {
		endTimeForDB = nil
	}

	err = s.executeQuery(ctx, query,
		progress.ID,
		progress.Status,
		progress.StartTime,
		endTimeForDB,
		progress.TotalTables,
		progress.CompletedTables,
		progress.FailedTables,
		string(tablesJSON),
		progress.Error,
		progress.CreatedAt,
		progress.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to save snapshot progress: %w", err)
	}

	s.logger.Debug("Snapshot progress saved",
		zap.String("id", progress.ID),
		zap.String("status", progress.Status))

	return nil
}

func (s *ClickHouseStorage) GetSnapshotProgress(ctx context.Context, id string) (*SnapshotProgress, error) {
	db := s.client.GetDB()
	if db == nil {
		return nil, fmt.Errorf("ClickHouse connection not available")
	}

	query := fmt.Sprintf(`
		SELECT
			id, status, start_time, end_time,
			total_tables, completed_tables, failed_tables, tables_json, error_message,
			created_at, updated_at
		FROM %s
		WHERE id = ?
		LIMIT 1
	`, s.getFullSnapshotTableName())

	row := db.QueryRowContext(ctx, query, id)
	return s.scanSnapshotProgress(row)
}

func (s *ClickHouseStorage) GetLatestSnapshotProgress(ctx context.Context) (*SnapshotProgress, error) {
	// Ensure snapshot table exists before querying
	if err := s.createSnapshotTable(ctx); err != nil {
		return nil, fmt.Errorf("failed to create snapshot table: %w", err)
	}

	db := s.client.GetDB()
	if db == nil {
		return nil, fmt.Errorf("ClickHouse connection not available")
	}

	query := fmt.Sprintf(`
		SELECT
			id, status, start_time, end_time,
			total_tables, completed_tables, failed_tables, tables_json, error_message,
			created_at, updated_at
		FROM %s
		ORDER BY created_at DESC
		LIMIT 1
	`, s.getFullSnapshotTableName())

	row := db.QueryRowContext(ctx, query)
	progress, err := s.scanSnapshotProgress(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest snapshot progress: %w", err)
	}
	return progress, nil
}

func (s *ClickHouseStorage) ListSnapshotProgress(ctx context.Context, limit int) ([]*SnapshotProgress, error) {
	db := s.client.GetDB()
	if db == nil {
		return nil, fmt.Errorf("ClickHouse connection not available")
	}

	query := fmt.Sprintf(`
		SELECT
			id, status, start_time, end_time,
			total_tables, completed_tables, failed_tables, tables_json, error_message,
			created_at, updated_at
		FROM %s
		ORDER BY created_at DESC
		LIMIT ?
	`, s.getFullSnapshotTableName())

	rows, err := db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshot progress: %w", err)
	}
	defer rows.Close()

	var snapshots []*SnapshotProgress
	for rows.Next() {
		snapshot, err := s.scanSnapshotProgress(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot progress: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}

	return snapshots, nil
}

func (s *ClickHouseStorage) UpdateSnapshotStatus(ctx context.Context, id string, status string, errorMsg string) error {
	query := fmt.Sprintf(`
		ALTER TABLE %s 
		UPDATE status = ?, error_message = ?, updated_at = ?
		WHERE id = ?
	`, s.getFullSnapshotTableName())

	err := s.executeQuery(ctx, query, status, errorMsg, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update snapshot status: %w", err)
	}

	s.logger.Debug("Snapshot status updated",
		zap.String("id", id),
		zap.String("status", status))

	return nil
}

func (s *ClickHouseStorage) createSnapshotTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
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
	`, s.getFullSnapshotTableName())

	err := s.executeQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create snapshot table: %w", err)
	}

	s.logger.Debug("Snapshot table created/verified",
		zap.String("table", s.tableName+"_snapshots"))

	return nil
}

func (s *ClickHouseStorage) scanSnapshotProgress(scanner any) (*SnapshotProgress, error) {
	var (
		id              string
		status          string
		startTime       time.Time
		endTime         *time.Time
		totalTables     int32
		completedTables int32
		failedTables    int32
		tablesJSON      string
		errorMessage    string
		createdAt       time.Time
		updatedAt       time.Time
	)

	var err error
	switch s := scanner.(type) {
	case *sql.Row:
		err = s.Scan(
			&id, &status, &startTime, &endTime,
			&totalTables, &completedTables, &failedTables, &tablesJSON, &errorMessage,
			&createdAt, &updatedAt,
		)
	case *sql.Rows:
		err = s.Scan(
			&id, &status, &startTime, &endTime,
			&totalTables, &completedTables, &failedTables, &tablesJSON, &errorMessage,
			&createdAt, &updatedAt,
		)
	default:
		return nil, fmt.Errorf("unsupported scanner type")
	}

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	var tables map[string]*TableSnapshot
	if tablesJSON != "" {
		if err := json.Unmarshal([]byte(tablesJSON), &tables); err != nil {
			s.logger.Warn("Failed to unmarshal snapshot tables", zap.Error(err))
			tables = make(map[string]*TableSnapshot)
		}
	} else {
		tables = make(map[string]*TableSnapshot)
	}

	return &SnapshotProgress{
		ID:              id,
		Status:          status,
		StartTime:       startTime,
		EndTime:         endTime,
		TotalTables:     int(totalTables),
		CompletedTables: int(completedTables),
		FailedTables:    int(failedTables),
		Tables:          tables,
		Error:           errorMessage,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
	}, nil
}
