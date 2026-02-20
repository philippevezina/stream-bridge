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

func TestDDLTranslator_RenameTable_SinglePair(t *testing.T) {
	logger := zap.NewNop()
	translator := NewTranslator(logger, &TranslationOptions{
		Engine:              clickhouse.EngineReplacingMergeTree,
		PreserveNullability: true,
		TimestampPrecision:  3,
		DefaultStringLength: 255,
		CustomTypeMappings:  make(map[string]string),
	})
	ddlTranslator := NewDDLTranslator(translator, logger)

	mysqlDDL := &DDLStatement{
		Type:     DDLTypeRenameTable,
		Database: "mydb",
		Table:    "old_table",
		RenamePairs: []RenamePair{
			{FromDatabase: "mydb", FromTable: "old_table", ToDatabase: "mydb", ToTable: "new_table"},
		},
	}

	result, err := ddlTranslator.Translate(mysqlDDL, clickhouse.EngineReplacingMergeTree, "ch_db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Type != DDLTypeRenameTable {
		t.Fatalf("expected RENAME_TABLE type, got %s", result.Type)
	}

	if len(result.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(result.Statements))
	}

	expected := "RENAME TABLE `ch_db`.`old_table` TO `ch_db`.`new_table`"
	if result.Statements[0] != expected {
		t.Errorf("expected statement:\n%s\ngot:\n%s", expected, result.Statements[0])
	}

	if result.IsDestructive {
		t.Error("RENAME TABLE should not be destructive")
	}
}

func TestDDLTranslator_RenameTable_MultiPair(t *testing.T) {
	logger := zap.NewNop()
	translator := NewTranslator(logger, &TranslationOptions{
		Engine:              clickhouse.EngineReplacingMergeTree,
		PreserveNullability: true,
		TimestampPrecision:  3,
		DefaultStringLength: 255,
		CustomTypeMappings:  make(map[string]string),
	})
	ddlTranslator := NewDDLTranslator(translator, logger)

	mysqlDDL := &DDLStatement{
		Type:     DDLTypeRenameTable,
		Database: "mydb",
		Table:    "integration_invoice_details",
		RenamePairs: []RenamePair{
			{FromDatabase: "mydb", FromTable: "integration_invoice_details", ToDatabase: "mydb", ToTable: "_integration_invoice_details_old"},
			{FromDatabase: "mydb", FromTable: "_integration_invoice_details_new", ToDatabase: "mydb", ToTable: "integration_invoice_details"},
		},
	}

	result, err := ddlTranslator.Translate(mysqlDDL, clickhouse.EngineReplacingMergeTree, "ch_db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if !strings.Contains(stmt, "RENAME TABLE") {
		t.Errorf("expected RENAME TABLE statement, got: %s", stmt)
	}
	if !strings.Contains(stmt, "`ch_db`.`integration_invoice_details` TO `ch_db`.`_integration_invoice_details_old`") {
		t.Errorf("missing first rename pair in: %s", stmt)
	}
	if !strings.Contains(stmt, "`ch_db`.`_integration_invoice_details_new` TO `ch_db`.`integration_invoice_details`") {
		t.Errorf("missing second rename pair in: %s", stmt)
	}
}

func TestDDLTranslator_RenameTable_EmptyPairs(t *testing.T) {
	logger := zap.NewNop()
	translator := NewTranslator(logger, &TranslationOptions{
		Engine:              clickhouse.EngineReplacingMergeTree,
		PreserveNullability: true,
		TimestampPrecision:  3,
		DefaultStringLength: 255,
		CustomTypeMappings:  make(map[string]string),
	})
	ddlTranslator := NewDDLTranslator(translator, logger)

	mysqlDDL := &DDLStatement{
		Type:        DDLTypeRenameTable,
		RenamePairs: []RenamePair{},
	}

	_, err := ddlTranslator.Translate(mysqlDDL, clickhouse.EngineReplacingMergeTree, "ch_db")
	if err == nil {
		t.Fatal("expected error for empty rename pairs")
	}
}
