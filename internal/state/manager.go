package state

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
)

type Manager struct {
	storage               StateStorage
	logger                *zap.Logger
	config                config.StateConfig
	currentCheckpoint     *Checkpoint
	mu                    sync.RWMutex
	lastSaveTime          time.Time
	eventsSinceCheckpoint int64
}

func NewManager(cfg config.StateConfig, clickhouseClient *clickhouse.Client, logger *zap.Logger) (*Manager, error) {
	var storage StateStorage

	switch cfg.Type {
	case "clickhouse":
		stateConfig := Config{
			Type:               cfg.Type,
			CheckpointInterval: cfg.CheckpointInterval,
			RetentionPeriod:    cfg.RetentionPeriod,
			ClickHouse: ClickHouseConfig{
				Database: cfg.ClickHouse.Database,
				Table:    cfg.ClickHouse.Table,
			},
		}
		storage = NewClickHouseStorage(clickhouseClient, logger, stateConfig)
	case "file":
		return nil, fmt.Errorf("file storage not implemented yet")
	default:
		return nil, fmt.Errorf("unsupported state storage type: %s", cfg.Type)
	}

	return &Manager{
		storage: storage,
		logger:  logger,
		config:  cfg,
	}, nil
}

func (m *Manager) Initialize(ctx context.Context) error {
	if err := m.storage.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize state storage: %w", err)
	}

	checkpoint, err := m.storage.GetLatestCheckpoint(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest checkpoint: %w", err)
	}

	if checkpoint != nil {
		m.mu.Lock()
		m.currentCheckpoint = checkpoint
		m.mu.Unlock()

		m.logger.Info("Restored from checkpoint",
			zap.String("checkpoint_id", checkpoint.ID),
			zap.String("position", fmt.Sprintf("%s:%d", checkpoint.Position.File, checkpoint.Position.Offset)),
			zap.Time("last_binlog_timestamp", checkpoint.LastBinlogTimestamp),
			zap.Time("created_at", checkpoint.CreatedAt))
	} else {
		m.logger.Info("No previous checkpoint found, starting fresh")
	}

	return nil
}

func (m *Manager) Close() error {
	return m.storage.Close()
}

func (m *Manager) GetLastPosition() *common.BinlogPosition {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.currentCheckpoint == nil {
		return nil
	}

	return &m.currentCheckpoint.Position
}

func (m *Manager) ShouldCreateCheckpoint() bool {
	m.mu.RLock()
	timeSinceLastSave := time.Since(m.lastSaveTime)
	m.mu.RUnlock()

	// Use atomic load to read the event count for consistency
	eventsSinceCheckpoint := atomic.LoadInt64(&m.eventsSinceCheckpoint)
	return timeSinceLastSave >= m.config.CheckpointInterval || eventsSinceCheckpoint >= 1000
}

func (m *Manager) CreateCheckpoint(ctx context.Context, position common.BinlogPosition, lastBinlogTimestamp time.Time) error {
	checkpointID := uuid.New().String()
	now := time.Now()

	checkpoint := &Checkpoint{
		ID:                  checkpointID,
		Position:            position,
		LastBinlogTimestamp: lastBinlogTimestamp,
		CreatedAt:           now,
	}

	if err := m.storage.SaveCheckpoint(ctx, checkpoint); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	m.mu.Lock()
	m.currentCheckpoint = checkpoint
	m.lastSaveTime = now
	m.mu.Unlock()

	// Reset the atomic counter after successful checkpoint creation
	atomic.StoreInt64(&m.eventsSinceCheckpoint, 0)

	m.logger.Info("Checkpoint created",
		zap.String("checkpoint_id", checkpointID),
		zap.String("position", fmt.Sprintf("%s:%d", position.File, position.Offset)),
		zap.Time("last_binlog_timestamp", lastBinlogTimestamp))

	return nil
}

func (m *Manager) IncrementEventCount() {
	// Use atomic increment for better performance in high-throughput scenarios
	atomic.AddInt64(&m.eventsSinceCheckpoint, 1)
}

func (m *Manager) GetCurrentCheckpoint() *Checkpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.currentCheckpoint == nil {
		return nil
	}

	checkpoint := *m.currentCheckpoint
	return &checkpoint
}

func (m *Manager) ListRecentCheckpoints(ctx context.Context, limit int) ([]*Checkpoint, error) {
	return m.storage.ListCheckpoints(ctx, limit)
}

func (m *Manager) HealthCheck(ctx context.Context) error {
	return m.storage.HealthCheck(ctx)
}

// Snapshot-related methods

func (m *Manager) SaveSnapshotProgress(ctx context.Context, progress *SnapshotProgress) error {
	return m.storage.SaveSnapshotProgress(ctx, progress)
}

func (m *Manager) GetSnapshotProgress(ctx context.Context, id string) (*SnapshotProgress, error) {
	return m.storage.GetSnapshotProgress(ctx, id)
}

func (m *Manager) GetLatestSnapshotProgress(ctx context.Context) (*SnapshotProgress, error) {
	return m.storage.GetLatestSnapshotProgress(ctx)
}

func (m *Manager) ListSnapshotProgress(ctx context.Context, limit int) ([]*SnapshotProgress, error) {
	return m.storage.ListSnapshotProgress(ctx, limit)
}

func (m *Manager) UpdateSnapshotStatus(ctx context.Context, id string, status string, errorMsg string) error {
	return m.storage.UpdateSnapshotStatus(ctx, id, status, errorMsg)
}
