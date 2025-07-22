package common

import (
	"time"
)

type EventType string

const (
	EventTypeInsert EventType = "INSERT"
	EventTypeUpdate EventType = "UPDATE"
	EventTypeDelete EventType = "DELETE"
	EventTypeDDL    EventType = "DDL"
)

type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Database  string                 `json:"database"`
	Table     string                 `json:"table"`
	Timestamp time.Time              `json:"timestamp"`
	Position  BinlogPosition         `json:"position"`
	Data      map[string]interface{} `json:"data,omitempty"`
	OldData   map[string]interface{} `json:"old_data,omitempty"`
	SQL       string                 `json:"sql,omitempty"`
}

type BinlogPosition struct {
	File   string `json:"file"`
	Offset uint32 `json:"offset"`
	GTID   string `json:"gtid,omitempty"`
}

type TableInfo struct {
	Database    string            `json:"database"`
	Name        string            `json:"name"`
	Columns     map[string]Column `json:"columns"`
	ColumnOrder []string          `json:"column_order"`
	Engine      string            `json:"engine,omitempty"`
}

type Column struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	DefaultValue  string `json:"default_value,omitempty"`
	IsPrimaryKey  bool   `json:"is_primary_key"`
	AutoIncrement bool   `json:"auto_increment"`
}

type Metrics struct {
	EventsProcessed  int64          `json:"events_processed"`
	EventsSuccessful int64          `json:"events_successful"`
	EventsFailed     int64          `json:"events_failed"`
	ReplicationLag   time.Duration  `json:"replication_lag"`
	LastEventTime    time.Time      `json:"last_event_time"`
	CurrentPosition  BinlogPosition `json:"current_position"`
	ProcessingRate   float64        `json:"processing_rate"`
	BatchSize        int            `json:"batch_size"`
	QueueLength      int            `json:"queue_length"`
}

type HealthStatus struct {
	Status              string        `json:"status"`
	MySQLConnected      bool          `json:"mysql_connected"`
	ClickHouseConnected bool          `json:"clickhouse_connected"`
	ReplicationRunning  bool          `json:"replication_running"`
	LastError           string        `json:"last_error,omitempty"`
	Uptime              time.Duration `json:"uptime"`
	Version             string        `json:"version"`
}
