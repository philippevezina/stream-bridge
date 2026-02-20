package schema

import (
	"testing"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/common"
)

func TestDDLCacheUpdater_RenameTable(t *testing.T) {
	logger := zap.NewNop()
	cacheManager := NewCacheManager(logger)
	translator := NewTranslator(logger, &TranslationOptions{
		PreserveNullability: true,
		CustomTypeMappings:  make(map[string]string),
	})
	updater := NewDDLCacheUpdater(cacheManager, translator, logger)

	// Seed cache with a table
	cacheManager.SetTableSchema(&common.TableInfo{
		Database:    "ch_db",
		Name:        "old_table",
		Columns:     map[string]common.Column{"id": {Name: "id", Type: "Int64"}},
		ColumnOrder: []string{"id"},
	})

	// Verify it exists
	if !cacheManager.HasTable("ch_db", "old_table") {
		t.Fatal("expected old_table to exist in cache")
	}

	mysqlDDL := &DDLStatement{
		Type: DDLTypeRenameTable,
		RenamePairs: []RenamePair{
			{FromDatabase: "mydb", FromTable: "old_table", ToDatabase: "mydb", ToTable: "new_table"},
		},
	}

	err := updater.UpdateCacheAfterDDL(mysqlDDL, "ch_db", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// old_table should be gone
	if cacheManager.HasTable("ch_db", "old_table") {
		t.Error("old_table should have been removed from cache")
	}

	// new_table should exist with same schema
	schema, err := cacheManager.GetTableSchema("ch_db", "new_table")
	if err != nil {
		t.Fatalf("new_table should exist in cache: %v", err)
	}
	if schema.Name != "new_table" {
		t.Errorf("expected table name 'new_table', got %s", schema.Name)
	}
	if _, hasID := schema.Columns["id"]; !hasID {
		t.Error("expected 'id' column to be preserved after rename")
	}
}

func TestDDLCacheUpdater_RenameTable_SourceNotInCache(t *testing.T) {
	logger := zap.NewNop()
	cacheManager := NewCacheManager(logger)
	translator := NewTranslator(logger, &TranslationOptions{
		PreserveNullability: true,
		CustomTypeMappings:  make(map[string]string),
	})
	updater := NewDDLCacheUpdater(cacheManager, translator, logger)

	// Don't seed cache — source table doesn't exist
	mysqlDDL := &DDLStatement{
		Type: DDLTypeRenameTable,
		RenamePairs: []RenamePair{
			{FromDatabase: "mydb", FromTable: "nonexistent", ToDatabase: "mydb", ToTable: "new_name"},
		},
	}

	// Should not error — just skip silently
	err := updater.UpdateCacheAfterDDL(mysqlDDL, "ch_db", "")
	if err != nil {
		t.Fatalf("should not error when source not in cache: %v", err)
	}
}

func TestDDLCacheUpdater_RenameTable_MultiPair(t *testing.T) {
	logger := zap.NewNop()
	cacheManager := NewCacheManager(logger)
	translator := NewTranslator(logger, &TranslationOptions{
		PreserveNullability: true,
		CustomTypeMappings:  make(map[string]string),
	})
	updater := NewDDLCacheUpdater(cacheManager, translator, logger)

	// Seed cache with original table and _new table
	cacheManager.SetTableSchema(&common.TableInfo{
		Database:    "ch_db",
		Name:        "invoice_details",
		Columns:     map[string]common.Column{"id": {Name: "id", Type: "Int64"}},
		ColumnOrder: []string{"id"},
	})
	cacheManager.SetTableSchema(&common.TableInfo{
		Database:    "ch_db",
		Name:        "_invoice_details_new",
		Columns:     map[string]common.Column{"id": {Name: "id", Type: "Int64"}, "new_col": {Name: "new_col", Type: "String"}},
		ColumnOrder: []string{"id", "new_col"},
	})

	mysqlDDL := &DDLStatement{
		Type: DDLTypeRenameTable,
		RenamePairs: []RenamePair{
			{FromDatabase: "mydb", FromTable: "invoice_details", ToDatabase: "mydb", ToTable: "_invoice_details_old"},
			{FromDatabase: "mydb", FromTable: "_invoice_details_new", ToDatabase: "mydb", ToTable: "invoice_details"},
		},
	}

	err := updater.UpdateCacheAfterDDL(mysqlDDL, "ch_db", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// invoice_details should now have the _new schema (with new_col)
	schema, err := cacheManager.GetTableSchema("ch_db", "invoice_details")
	if err != nil {
		t.Fatalf("invoice_details should exist: %v", err)
	}
	if _, hasNewCol := schema.Columns["new_col"]; !hasNewCol {
		t.Error("invoice_details should have 'new_col' after pt-osc rename swap")
	}

	// _invoice_details_old should have the original schema (no new_col)
	oldSchema, err := cacheManager.GetTableSchema("ch_db", "_invoice_details_old")
	if err != nil {
		t.Fatalf("_invoice_details_old should exist: %v", err)
	}
	if _, hasNewCol := oldSchema.Columns["new_col"]; hasNewCol {
		t.Error("_invoice_details_old should NOT have 'new_col'")
	}

	// _invoice_details_new should no longer exist
	if cacheManager.HasTable("ch_db", "_invoice_details_new") {
		t.Error("_invoice_details_new should have been removed from cache")
	}
}
