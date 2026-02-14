package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateReplicationParams(t *testing.T) {
	tests := []struct {
		name        string
		zooPath     string
		replicaName string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "both empty is valid",
			zooPath:     "",
			replicaName: "",
			expectError: false,
		},
		{
			name:        "both set is valid",
			zooPath:     "/clickhouse/tables/{shard}/{database}/{table}",
			replicaName: "{replica}",
			expectError: false,
		},
		{
			name:        "only zoo_path set is invalid",
			zooPath:     "/clickhouse/tables/{shard}/{database}/{table}",
			replicaName: "",
			expectError: true,
			errorMsg:    "schema.zoo_path and schema.replica_name must both be set or both be empty",
		},
		{
			name:        "only replica_name set is invalid",
			zooPath:     "",
			replicaName: "{replica}",
			expectError: true,
			errorMsg:    "schema.zoo_path and schema.replica_name must both be set or both be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := minimalValidConfig()
			cfg.Schema.ZooPath = tt.zooPath
			cfg.Schema.ReplicaName = tt.replicaName

			err := validate(cfg)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if err.Error() != tt.errorMsg {
					t.Fatalf("expected error %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error but got: %v", err)
				}
			}
		})
	}
}

func TestLoadWithReplicationParams(t *testing.T) {
	configContent := `
mysql:
  host: "localhost"
  port: 3306
  username: "root"
  password: "pass"
  database: "test"
clickhouse:
  addresses: ["localhost:9000"]
  username: "default"
  database: "test"
schema:
  default_engine: "ReplicatedReplacingMergeTree"
  zoo_path: "/clickhouse/tables/{shard}/{database}/{table}"
  replica_name: "{replica}"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Schema.DefaultEngine != "ReplicatedReplacingMergeTree" {
		t.Errorf("expected engine ReplicatedReplacingMergeTree, got %s", cfg.Schema.DefaultEngine)
	}
	if cfg.Schema.ZooPath != "/clickhouse/tables/{shard}/{database}/{table}" {
		t.Errorf("expected zoo_path, got %s", cfg.Schema.ZooPath)
	}
	if cfg.Schema.ReplicaName != "{replica}" {
		t.Errorf("expected replica_name, got %s", cfg.Schema.ReplicaName)
	}
}

// minimalValidConfig returns a Config with all required fields set to valid values.
func minimalValidConfig() *Config {
	return &Config{
		MySQL: MySQLConfig{
			Host:               "localhost",
			Port:               3306,
			Username:           "root",
			Password:           "pass",
			Database:           "test",
			ServerID:           1001,
			Flavor:             "mysql",
			SSLMode:            SSLModeDisabled,
			EventChannelBuffer: 10000,
		},
		ClickHouse: ClickHouseConfig{
			Addresses:    []string{"localhost:9000"},
			Database:     "default",
			Username:     "default",
			DialTimeout:  10_000_000_000, // 10s
			MaxOpenConns: 10,
			MaxIdleConns: 5,
			MaxLifetime:  3_600_000_000_000, // 1h
		},
		Pipeline: PipelineConfig{
			BatchSize:               500,
			BatchTimeout:            2_000_000_000,
			MaxRetries:              3,
			RetryDelay:              1_000_000_000,
			WorkerCount:             4,
			BufferSize:              10000,
			WorkerChannelBufferSize: 1000,
			FlushInterval:           2_000_000_000,
			DDLFlushTimeout:         60_000_000_000,
			WorkerChannelTimeout:    30_000_000_000,
		},
		Monitoring: MonitoringConfig{
			Enabled:     true,
			Port:        8080,
			MetricsPath: "/metrics",
			HealthPath:  "/health",
		},
		Logging: LoggingConfig{
			Level:      "info",
			Format:     "json",
			OutputPath: "stdout",
			MaxSize:    100,
			MaxBackups: 3,
			MaxAge:     7,
		},
		State: StateConfig{
			Type:               "clickhouse",
			CheckpointInterval: 30_000_000_000,
			RetentionPeriod:    604_800_000_000_000, // 168h
		},
		Schema: SchemaConfig{
			DefaultEngine:       "ReplacingMergeTree",
			PreserveNullable:    true,
			TimestampPrecision:  3,
			AllowDestructiveDDL: true,
		},
	}
}
