package schema

import (
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
)

func TestGenerateCreateTableDDL_ReplicatedWithZooPath(t *testing.T) {
	logger := zap.NewNop()
	translator := NewTranslator(logger, &TranslationOptions{
		Engine:              clickhouse.EngineReplicatedReplacingMergeTree,
		ZooPath:             "/clickhouse/tables/{shard}/{database}/{table}",
		ReplicaName:         "{replica}",
		PreserveNullability: true,
		TimestampPrecision:  3,
		DefaultStringLength: 255,
		CustomTypeMappings:  make(map[string]string),
	})

	schema := &ClickHouseSchema{
		Database: "test_db",
		Table:    "users",
		Columns: []ClickHouseColumn{
			{Name: "id", Type: "Int32"},
			{Name: "name", Type: "String"},
			{Name: "_is_deleted", Type: "UInt8", DefaultValue: "0"},
			{Name: "_version", Type: "UInt64", DefaultValue: "0"},
		},
		Engine:   clickhouse.EngineReplicatedReplacingMergeTree,
		OrderBy:  []string{"id"},
		Settings: map[string]string{"allow_nullable_key": "1"},
	}

	ddl, err := translator.GenerateCreateTableDDL(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', _version, _is_deleted)"
	if !strings.Contains(ddl, expected) {
		t.Errorf("DDL should contain %q\nGot:\n%s", expected, ddl)
	}
}

func TestGenerateCreateTableDDL_ReplicatedWithoutZooPath(t *testing.T) {
	logger := zap.NewNop()
	translator := NewTranslator(logger, &TranslationOptions{
		Engine:              clickhouse.EngineReplicatedReplacingMergeTree,
		PreserveNullability: true,
		TimestampPrecision:  3,
		DefaultStringLength: 255,
		CustomTypeMappings:  make(map[string]string),
	})

	schema := &ClickHouseSchema{
		Database: "test_db",
		Table:    "users",
		Columns: []ClickHouseColumn{
			{Name: "id", Type: "Int32"},
			{Name: "_is_deleted", Type: "UInt8", DefaultValue: "0"},
			{Name: "_version", Type: "UInt64", DefaultValue: "0"},
		},
		Engine:   clickhouse.EngineReplicatedReplacingMergeTree,
		OrderBy:  []string{"id"},
		Settings: map[string]string{"allow_nullable_key": "1"},
	}

	ddl, err := translator.GenerateCreateTableDDL(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "ENGINE = ReplicatedReplacingMergeTree(_version, _is_deleted)"
	if !strings.Contains(ddl, expected) {
		t.Errorf("DDL should contain %q\nGot:\n%s", expected, ddl)
	}

	// Should NOT contain zoo_path params
	if strings.Contains(ddl, "'/clickhouse") {
		t.Errorf("DDL should not contain zoo_path when not configured\nGot:\n%s", ddl)
	}
}

func TestGenerateCreateTableDDL_ReplacingMergeTree(t *testing.T) {
	logger := zap.NewNop()
	translator := NewTranslator(logger, &TranslationOptions{
		Engine:              clickhouse.EngineReplacingMergeTree,
		PreserveNullability: true,
		TimestampPrecision:  3,
		DefaultStringLength: 255,
		CustomTypeMappings:  make(map[string]string),
	})

	schema := &ClickHouseSchema{
		Database: "test_db",
		Table:    "users",
		Columns: []ClickHouseColumn{
			{Name: "id", Type: "Int32"},
			{Name: "_is_deleted", Type: "UInt8", DefaultValue: "0"},
			{Name: "_version", Type: "UInt64", DefaultValue: "0"},
		},
		Engine:   clickhouse.EngineReplacingMergeTree,
		OrderBy:  []string{"id"},
		Settings: map[string]string{"allow_nullable_key": "1"},
	}

	ddl, err := translator.GenerateCreateTableDDL(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "ENGINE = ReplacingMergeTree(_version, _is_deleted)"
	if !strings.Contains(ddl, expected) {
		t.Errorf("DDL should contain %q\nGot:\n%s", expected, ddl)
	}
}

func TestGetDefaultEngine(t *testing.T) {
	logger := zap.NewNop()

	tests := []struct {
		name     string
		engine   clickhouse.TableEngine
		expected clickhouse.TableEngine
	}{
		{
			name:     "ReplacingMergeTree",
			engine:   clickhouse.EngineReplacingMergeTree,
			expected: clickhouse.EngineReplacingMergeTree,
		},
		{
			name:     "ReplicatedReplacingMergeTree",
			engine:   clickhouse.EngineReplicatedReplacingMergeTree,
			expected: clickhouse.EngineReplicatedReplacingMergeTree,
		},
		{
			name:     "unsupported engine falls back to ReplacingMergeTree",
			engine:   clickhouse.TableEngine("MergeTree"),
			expected: clickhouse.EngineReplacingMergeTree,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := NewTranslator(logger, &TranslationOptions{
				Engine:              tt.engine,
				DefaultStringLength: 255,
				CustomTypeMappings:  make(map[string]string),
			})

			got := translator.GetDefaultEngine()
			if got != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, got)
			}
		})
	}
}
