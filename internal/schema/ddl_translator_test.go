package schema

import (
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
)

func TestDDLTranslator_CreateTable_UsesConfiguredEngine(t *testing.T) {
	tests := []struct {
		name            string
		engine          clickhouse.TableEngine
		zooPath         string
		replicaName     string
		expectedContain string
	}{
		{
			name:            "ReplacingMergeTree",
			engine:          clickhouse.EngineReplacingMergeTree,
			expectedContain: "ENGINE = ReplacingMergeTree(_version, _is_deleted)",
		},
		{
			name:            "ReplicatedReplacingMergeTree without zoo_path",
			engine:          clickhouse.EngineReplicatedReplacingMergeTree,
			expectedContain: "ENGINE = ReplicatedReplacingMergeTree(_version, _is_deleted)",
		},
		{
			name:            "ReplicatedReplacingMergeTree with zoo_path",
			engine:          clickhouse.EngineReplicatedReplacingMergeTree,
			zooPath:         "/clickhouse/tables/{shard}/{database}/{table}",
			replicaName:     "{replica}",
			expectedContain: "ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', _version, _is_deleted)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			translator := NewTranslator(logger, &TranslationOptions{
				Engine:              tt.engine,
				ZooPath:             tt.zooPath,
				ReplicaName:         tt.replicaName,
				PreserveNullability: true,
				TimestampPrecision:  3,
				DefaultStringLength: 255,
				CustomTypeMappings:  make(map[string]string),
			})

			ddlTranslator := NewDDLTranslator(translator, logger)

			mysqlDDL := &DDLStatement{
				Type:     DDLTypeCreateTable,
				Database: "mydb",
				Table:    "test_table",
				Columns: []DDLColumn{
					{Name: "id", Type: "INT", Nullable: false, Attributes: map[string]string{"primary_key": "true"}},
					{Name: "name", Type: "VARCHAR(255)", Nullable: true, Attributes: map[string]string{}},
				},
			}

			result, err := ddlTranslator.Translate(mysqlDDL, tt.engine, "ch_db")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(result.Statements) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(result.Statements))
			}

			stmt := result.Statements[0]
			if !strings.Contains(stmt, tt.expectedContain) {
				t.Errorf("DDL should contain %q\nGot:\n%s", tt.expectedContain, stmt)
			}
		})
	}
}
