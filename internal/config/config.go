package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/spf13/viper"
)

// MySQL SSL modes
const (
	SSLModeDisabled       = "disabled"
	SSLModePreferred      = "preferred"
	SSLModeRequired       = "required"
	SSLModeVerifyCA       = "verify_ca"
	SSLModeVerifyIdentity = "verify_identity"
)

type Config struct {
	MySQL         MySQLConfig         `mapstructure:"mysql"`
	ClickHouse    ClickHouseConfig    `mapstructure:"clickhouse"`
	Pipeline      PipelineConfig      `mapstructure:"pipeline"`
	Monitoring    MonitoringConfig    `mapstructure:"monitoring"`
	Logging       LoggingConfig       `mapstructure:"logging"`
	State         StateConfig         `mapstructure:"state"`
	Schema        SchemaConfig        `mapstructure:"schema"`
	Snapshot      SnapshotConfig      `mapstructure:"snapshot"`
	Observability ObservabilityConfig `mapstructure:"observability"`
}

type MySQLConfig struct {
	Host               string            `mapstructure:"host"`
	Port               int               `mapstructure:"port"`
	Username           string            `mapstructure:"username"`
	Password           string            `mapstructure:"password"`
	Database           string            `mapstructure:"database"`
	ServerID           uint32            `mapstructure:"server_id"`
	Flavor             string            `mapstructure:"flavor"`
	SSLMode            string            `mapstructure:"ssl_mode"`
	SSLCert            string            `mapstructure:"ssl_cert"`
	SSLKey             string            `mapstructure:"ssl_key"`
	SSLCa              string            `mapstructure:"ssl_ca"`
	EventChannelBuffer int               `mapstructure:"event_channel_buffer"`
	TableFilter        TableFilterConfig `mapstructure:"table_filter"`
}

type ClickHouseConfig struct {
	Addresses    []string      `mapstructure:"addresses"`
	Database     string        `mapstructure:"database"`
	Username     string        `mapstructure:"username"`
	Password     string        `mapstructure:"password"`
	EnableSSL    bool          `mapstructure:"enable_ssl"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	MaxOpenConns int           `mapstructure:"max_open_conns"`
	MaxIdleConns int           `mapstructure:"max_idle_conns"`
	MaxLifetime  time.Duration `mapstructure:"max_lifetime"`
}

type TableFilterConfig struct {
	IncludePatterns []string `mapstructure:"include_patterns"`
	ExcludePatterns []string `mapstructure:"exclude_patterns"`
	IncludeTables   []string `mapstructure:"include_tables"`
	ExcludeTables   []string `mapstructure:"exclude_tables"`
}

type PipelineConfig struct {
	BatchSize               int           `mapstructure:"batch_size"`
	BatchTimeout            time.Duration `mapstructure:"batch_timeout"`
	MaxRetries              int           `mapstructure:"max_retries"`
	RetryDelay              time.Duration `mapstructure:"retry_delay"`
	WorkerCount             int           `mapstructure:"worker_count"`
	BufferSize              int           `mapstructure:"buffer_size"`
	WorkerChannelBufferSize int           `mapstructure:"worker_channel_buffer_size"`
	FlushInterval           time.Duration `mapstructure:"flush_interval"`
	DDLFlushTimeout         time.Duration `mapstructure:"ddl_flush_timeout"`
	WorkerChannelTimeout    time.Duration `mapstructure:"worker_channel_timeout"`
}

type MonitoringConfig struct {
	Enabled     bool   `mapstructure:"enabled"`
	Port        int    `mapstructure:"port"`
	MetricsPath string `mapstructure:"metrics_path"`
	HealthPath  string `mapstructure:"health_path"`
}

type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"`
	OutputPath string `mapstructure:"output_path"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
	LocalTime  bool   `mapstructure:"local_time"`
}

type StateConfig struct {
	Type               string                `mapstructure:"type"`
	ClickHouse         StateClickHouseConfig `mapstructure:"clickhouse"`
	CheckpointInterval time.Duration         `mapstructure:"checkpoint_interval"`
	RetentionPeriod    time.Duration         `mapstructure:"retention_period"`
}

type StateClickHouseConfig struct {
	Database string `mapstructure:"database"`
	Table    string `mapstructure:"table"`
}

type SchemaConfig struct {
	DefaultEngine       string `mapstructure:"default_engine"`
	PreserveNullable    bool   `mapstructure:"preserve_nullable"`
	TimestampPrecision  int    `mapstructure:"timestamp_precision"`
	AllowDestructiveDDL bool   `mapstructure:"allow_destructive_ddl"` // Allow DROP TABLE/DROP COLUMN operations
}

type SnapshotConfig struct {
	Enabled             bool          `mapstructure:"enabled"`
	ChunkSize           int           `mapstructure:"chunk_size"`
	ParallelTables      int           `mapstructure:"parallel_tables"`
	Timeout             time.Duration `mapstructure:"timeout"`
	ResumeOnFailure     bool          `mapstructure:"resume_on_failure"`
	MaxRetries          int           `mapstructure:"max_retries"`
	RetryDelay          time.Duration `mapstructure:"retry_delay"`
	LockTimeout         time.Duration `mapstructure:"lock_timeout"`
	LockStrategy        string        `mapstructure:"lock_strategy"`
	CreateMissingTables bool          `mapstructure:"create_missing_tables"`
	SkipDataLoad        bool          `mapstructure:"skip_data_load"`
}

type ObservabilityConfig struct {
	ErrorReporting ErrorReportingConfig `mapstructure:"error_reporting"`
	LogExporting   LogExportingConfig   `mapstructure:"log_exporting"`
}

type ErrorReportingConfig struct {
	Enabled  bool         `mapstructure:"enabled"`
	Provider string       `mapstructure:"provider"` // sentry, noop
	Sentry   SentryConfig `mapstructure:"sentry"`
}

type SentryConfig struct {
	DSN          string        `mapstructure:"dsn"`
	Environment  string        `mapstructure:"environment"`
	Release      string        `mapstructure:"release"`
	SampleRate   float64       `mapstructure:"sample_rate"`
	Debug        bool          `mapstructure:"debug"`
	FlushTimeout time.Duration `mapstructure:"flush_timeout"`
}

type LogExportingConfig struct {
	Enabled  bool           `mapstructure:"enabled"`
	Provider string         `mapstructure:"provider"` // newrelic, noop
	NewRelic NewRelicConfig `mapstructure:"newrelic"`
}

type NewRelicConfig struct {
	LicenseKey    string        `mapstructure:"license_key"`
	AppName       string        `mapstructure:"app_name"`
	LogForwarding bool          `mapstructure:"log_forwarding"`
	MinLogLevel   string        `mapstructure:"min_log_level"`
	FlushTimeout  time.Duration `mapstructure:"flush_timeout"`
}

func Load(configPath string) (*Config, error) {
	v := viper.New()

	v.SetConfigType("yaml")

	setDefaults(v)

	// Read config file as raw bytes
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	expandedData, err := expandEnvWithDefaults(string(data))
	if err != nil {
		return nil, fmt.Errorf("failed to expand environment variables: %w", err)
	}

	// Parse the expanded configuration
	if err := v.ReadConfig(bytes.NewReader([]byte(expandedData))); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("mysql.host", "localhost")
	v.SetDefault("mysql.port", 3306)
	v.SetDefault("mysql.server_id", 1001)
	v.SetDefault("mysql.flavor", "mysql")
	v.SetDefault("mysql.ssl_mode", SSLModePreferred)
	v.SetDefault("mysql.event_channel_buffer", 10000)

	v.SetDefault("clickhouse.database", "default")
	v.SetDefault("clickhouse.enable_ssl", false)
	v.SetDefault("clickhouse.dial_timeout", "10s")
	v.SetDefault("clickhouse.max_open_conns", 10)
	v.SetDefault("clickhouse.max_idle_conns", 5)
	v.SetDefault("clickhouse.max_lifetime", "1h")

	v.SetDefault("pipeline.batch_size", 500)
	v.SetDefault("pipeline.batch_timeout", "2s")
	v.SetDefault("pipeline.max_retries", 3)
	v.SetDefault("pipeline.retry_delay", "1s")
	v.SetDefault("pipeline.worker_count", 4)
	v.SetDefault("pipeline.buffer_size", 10000)
	v.SetDefault("pipeline.worker_channel_buffer_size", 1000)
	v.SetDefault("pipeline.flush_interval", "2s")
	v.SetDefault("pipeline.ddl_flush_timeout", "1m")
	v.SetDefault("pipeline.worker_channel_timeout", "30s")

	v.SetDefault("monitoring.enabled", true)
	v.SetDefault("monitoring.port", 8080)
	v.SetDefault("monitoring.metrics_path", "/metrics")
	v.SetDefault("monitoring.health_path", "/health")

	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.output_path", "stdout")
	v.SetDefault("logging.max_size", 100)
	v.SetDefault("logging.max_backups", 3)
	v.SetDefault("logging.max_age", 7)
	v.SetDefault("logging.compress", true)
	v.SetDefault("logging.local_time", true)

	v.SetDefault("state.type", "clickhouse")
	v.SetDefault("state.clickhouse.database", "default")
	v.SetDefault("state.clickhouse.table", "stream_bridge_checkpoints")
	v.SetDefault("state.checkpoint_interval", "30s")
	v.SetDefault("state.retention_period", "168h")

	v.SetDefault("schema.default_engine", "ReplacingMergeTree")
	v.SetDefault("schema.preserve_nullable", true)
	v.SetDefault("schema.timestamp_precision", 3)
	v.SetDefault("schema.allow_destructive_ddl", true) // Allow destructive DDL by default (user can disable)

	v.SetDefault("snapshot.enabled", false)
	v.SetDefault("snapshot.chunk_size", 10000)
	v.SetDefault("snapshot.parallel_tables", 2)
	v.SetDefault("snapshot.timeout", "1h")
	v.SetDefault("snapshot.resume_on_failure", true)
	v.SetDefault("snapshot.max_retries", 3)
	v.SetDefault("snapshot.retry_delay", "5s")
	v.SetDefault("snapshot.lock_timeout", "30s")
	v.SetDefault("snapshot.lock_strategy", "auto")
	v.SetDefault("snapshot.create_missing_tables", true)
	v.SetDefault("snapshot.skip_data_load", false)

	// Observability defaults
	v.SetDefault("observability.error_reporting.enabled", false)
	v.SetDefault("observability.error_reporting.provider", "sentry")
	v.SetDefault("observability.error_reporting.sentry.sample_rate", 1.0)
	v.SetDefault("observability.error_reporting.sentry.flush_timeout", "5s")

	v.SetDefault("observability.log_exporting.enabled", false)
	v.SetDefault("observability.log_exporting.provider", "newrelic")
	v.SetDefault("observability.log_exporting.newrelic.log_forwarding", true)
	v.SetDefault("observability.log_exporting.newrelic.min_log_level", "info")
	v.SetDefault("observability.log_exporting.newrelic.flush_timeout", "5s")
}

func validate(cfg *Config) error {
	if cfg.MySQL.Host == "" {
		return fmt.Errorf("mysql.host is required")
	}
	if cfg.MySQL.Username == "" {
		return fmt.Errorf("mysql.username is required")
	}
	if cfg.MySQL.Password == "" {
		return fmt.Errorf("mysql.password is required")
	}

	// MySQL port validation
	if err := validatePort(cfg.MySQL.Port, "mysql.port"); err != nil {
		return err
	}

	// MySQL event channel buffer validation
	if err := validateRange(cfg.MySQL.EventChannelBuffer, 100, 1000000, "mysql.event_channel_buffer"); err != nil {
		return err
	}

	// SSL mode validation
	validSSLModes := map[string]bool{
		SSLModeDisabled:       true,
		SSLModePreferred:      true,
		SSLModeRequired:       true,
		SSLModeVerifyCA:       true,
		SSLModeVerifyIdentity: true,
	}
	if !validSSLModes[cfg.MySQL.SSLMode] {
		return fmt.Errorf("mysql.ssl_mode must be one of: disabled, preferred, required, verify_ca, verify_identity")
	}

	// Certificate validation
	if cfg.MySQL.SSLCert != "" && cfg.MySQL.SSLKey == "" {
		return fmt.Errorf("mysql.ssl_key is required when mysql.ssl_cert is specified")
	}
	if cfg.MySQL.SSLKey != "" && cfg.MySQL.SSLCert == "" {
		return fmt.Errorf("mysql.ssl_cert is required when mysql.ssl_key is specified")
	}

	// CA certificate requirements for verification modes
	if (cfg.MySQL.SSLMode == SSLModeVerifyCA || cfg.MySQL.SSLMode == SSLModeVerifyIdentity) && cfg.MySQL.SSLCa == "" {
		return fmt.Errorf("mysql.ssl_ca is required when ssl_mode is %s", cfg.MySQL.SSLMode)
	}

	if len(cfg.ClickHouse.Addresses) == 0 {
		return fmt.Errorf("clickhouse.addresses is required")
	}
	if cfg.ClickHouse.Username == "" {
		return fmt.Errorf("clickhouse.username is required")
	}

	// ClickHouse connection pool validation
	if err := validateRange(cfg.ClickHouse.MaxOpenConns, 1, 1000, "clickhouse.max_open_conns"); err != nil {
		return err
	}
	if err := validateRange(cfg.ClickHouse.MaxIdleConns, 0, cfg.ClickHouse.MaxOpenConns, "clickhouse.max_idle_conns"); err != nil {
		return err
	}
	if cfg.ClickHouse.MaxIdleConns > cfg.ClickHouse.MaxOpenConns {
		return fmt.Errorf("clickhouse.max_idle_conns (%d) cannot exceed clickhouse.max_open_conns (%d)",
			cfg.ClickHouse.MaxIdleConns, cfg.ClickHouse.MaxOpenConns)
	}

	// ClickHouse timeout validation
	if err := validatePositiveDuration(cfg.ClickHouse.DialTimeout, "clickhouse.dial_timeout"); err != nil {
		return err
	}
	if err := validatePositiveDuration(cfg.ClickHouse.MaxLifetime, "clickhouse.max_lifetime"); err != nil {
		return err
	}

	// Pipeline configuration validation
	if cfg.Pipeline.BatchSize <= 0 {
		return fmt.Errorf("pipeline.batch_size must be positive")
	}

	// Pipeline worker count with upper bound to prevent resource exhaustion
	if err := validateRange(cfg.Pipeline.WorkerCount, 1, 100, "pipeline.worker_count"); err != nil {
		return err
	}

	// Pipeline max retries validation
	if cfg.Pipeline.MaxRetries < 0 {
		return fmt.Errorf("pipeline.max_retries must be non-negative, got %d", cfg.Pipeline.MaxRetries)
	}

	// Pipeline buffer size validation
	if err := validateRange(cfg.Pipeline.BufferSize, 100, 10000000, "pipeline.buffer_size"); err != nil {
		return err
	}
	if err := validateRange(cfg.Pipeline.WorkerChannelBufferSize, 100, 1000000, "pipeline.worker_channel_buffer_size"); err != nil {
		return err
	}

	// Pipeline timeout validation
	if err := validatePositiveDuration(cfg.Pipeline.BatchTimeout, "pipeline.batch_timeout"); err != nil {
		return err
	}
	if err := validatePositiveDuration(cfg.Pipeline.RetryDelay, "pipeline.retry_delay"); err != nil {
		return err
	}
	if err := validatePositiveDuration(cfg.Pipeline.FlushInterval, "pipeline.flush_interval"); err != nil {
		return err
	}
	if err := validatePositiveDuration(cfg.Pipeline.DDLFlushTimeout, "pipeline.ddl_flush_timeout"); err != nil {
		return err
	}
	if err := validatePositiveDuration(cfg.Pipeline.WorkerChannelTimeout, "pipeline.worker_channel_timeout"); err != nil {
		return err
	}

	// Snapshot configuration validation (only when enabled)
	if cfg.Snapshot.Enabled {
		// Snapshot chunk size with reasonable bounds
		if err := validateRange(cfg.Snapshot.ChunkSize, 100, 1000000, "snapshot.chunk_size"); err != nil {
			return err
		}

		if cfg.Snapshot.ParallelTables <= 0 {
			return fmt.Errorf("snapshot.parallel_tables must be positive")
		}

		if cfg.Snapshot.MaxRetries < 0 {
			return fmt.Errorf("snapshot.max_retries must be non-negative")
		}

		// Snapshot timeout validation
		if err := validatePositiveDuration(cfg.Snapshot.Timeout, "snapshot.timeout"); err != nil {
			return err
		}
		if err := validatePositiveDuration(cfg.Snapshot.RetryDelay, "snapshot.retry_delay"); err != nil {
			return err
		}
		if err := validatePositiveDuration(cfg.Snapshot.LockTimeout, "snapshot.lock_timeout"); err != nil {
			return err
		}

		validLockStrategies := map[string]bool{
			"auto":         true,
			"flush_tables": true,
			"backup_locks": true,
			"none":         true,
			"no_lock":      true,
		}
		if !validLockStrategies[cfg.Snapshot.LockStrategy] {
			return fmt.Errorf("snapshot.lock_strategy must be one of: auto, flush_tables, backup_locks, none")
		}
	}

	// State configuration validation
	if err := validateDurationMinimum(cfg.State.CheckpointInterval, time.Second, "state.checkpoint_interval"); err != nil {
		return err
	}
	if err := validateDurationMinimum(cfg.State.RetentionPeriod, time.Hour, "state.retention_period"); err != nil {
		return err
	}

	// Monitoring configuration validation (only when enabled)
	if cfg.Monitoring.Enabled {
		if err := validatePort(cfg.Monitoring.Port, "monitoring.port"); err != nil {
			return err
		}
	}

	// Logging configuration validation
	if err := validateRange(cfg.Logging.MaxSize, 1, 1000, "logging.max_size"); err != nil {
		return err
	}
	if err := validateRange(cfg.Logging.MaxBackups, 0, 100, "logging.max_backups"); err != nil {
		return err
	}
	if err := validateRange(cfg.Logging.MaxAge, 0, 365, "logging.max_age"); err != nil {
		return err
	}

	return nil
}

// validatePort checks if a port number is in the valid range (1-65535)
func validatePort(port int, name string) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("%s must be between 1 and 65535, got %d", name, port)
	}
	return nil
}

// validatePositiveDuration checks if a duration is positive
func validatePositiveDuration(d time.Duration, name string) error {
	if d <= 0 {
		return fmt.Errorf("%s must be positive, got %v", name, d)
	}
	return nil
}

// validateDurationMinimum checks if a duration meets a minimum threshold
func validateDurationMinimum(d time.Duration, minimum time.Duration, name string) error {
	if d < minimum {
		return fmt.Errorf("%s must be at least %v, got %v", name, minimum, d)
	}
	return nil
}

// validateRange checks if an integer is within a specified range
func validateRange(value int, min int, max int, name string) error {
	if value < min || value > max {
		return fmt.Errorf("%s must be between %d and %d, got %d", name, min, max, value)
	}
	return nil
}
