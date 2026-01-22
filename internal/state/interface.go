package state

import (
	"context"
	"sync"
	"time"

	"github.com/philippevezina/stream-bridge/internal/common"
)

type Checkpoint struct {
	ID                  string                `json:"id"`
	Position            common.BinlogPosition `json:"position"`
	LastBinlogTimestamp time.Time             `json:"last_binlog_timestamp"`
	CreatedAt           time.Time             `json:"created_at"`
}

type SnapshotProgress struct {
	ID              string                    `json:"id"`
	Status          string                    `json:"status"`
	StartTime       time.Time                 `json:"start_time"`
	EndTime         *time.Time                `json:"end_time,omitempty"`
	TotalTables     int                       `json:"total_tables"`
	CompletedTables int                       `json:"completed_tables"`
	FailedTables    int                       `json:"failed_tables"`
	Tables          map[string]*TableSnapshot `json:"tables"`
	Error           string                    `json:"error,omitempty"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

type TableSnapshot struct {
	Mu              sync.RWMutex           `json:"-"` // Protects concurrent field access (must be held when reading/writing fields)
	Database        string                 `json:"database"`
	Table           string                 `json:"table"`
	Status          string                 `json:"status"`
	TotalRows       int64                  `json:"total_rows"`
	ProcessedRows   int64                  `json:"processed_rows"`
	ChunkSize       int                    `json:"chunk_size"`
	DataLoadEnabled bool                   `json:"data_load_enabled"`
	StartTime       time.Time              `json:"start_time"`
	EndTime         *time.Time             `json:"end_time,omitempty"`
	Error           string                 `json:"error,omitempty"`
	Position        *common.BinlogPosition `json:"position,omitempty"`
}

type StateStorage interface {
	Initialize(ctx context.Context) error
	Close() error
	SaveCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
	GetLatestCheckpoint(ctx context.Context) (*Checkpoint, error)
	GetCheckpoint(ctx context.Context, id string) (*Checkpoint, error)
	ListCheckpoints(ctx context.Context, limit int) ([]*Checkpoint, error)
	HealthCheck(ctx context.Context) error

	// Snapshot related methods
	SaveSnapshotProgress(ctx context.Context, progress *SnapshotProgress) error
	GetSnapshotProgress(ctx context.Context, id string) (*SnapshotProgress, error)
	GetLatestSnapshotProgress(ctx context.Context) (*SnapshotProgress, error)
	ListSnapshotProgress(ctx context.Context, limit int) ([]*SnapshotProgress, error)
	UpdateSnapshotStatus(ctx context.Context, id string, status string, errorMsg string) error
}

type Config struct {
	Type               string           `mapstructure:"type"`
	ClickHouse         ClickHouseConfig `mapstructure:"clickhouse"`
	CheckpointInterval time.Duration    `mapstructure:"checkpoint_interval"`
	RetentionPeriod    time.Duration    `mapstructure:"retention_period"`
}

type ClickHouseConfig struct {
	Database string `mapstructure:"database"`
	Table    string `mapstructure:"table"`
}
