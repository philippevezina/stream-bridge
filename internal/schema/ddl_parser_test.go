package schema

import (
	"testing"

	"go.uber.org/zap"
)

func TestParseCreateTable_SkipsConstraintDefinitions(t *testing.T) {
	parser := NewDDLParser(zap.NewNop())

	sql := "CREATE TABLE `integration_invoice_details` (" +
		"`id` bigint NOT NULL AUTO_INCREMENT, " +
		"`slug` varchar(255) DEFAULT NULL, " +
		"`user_id` bigint DEFAULT NULL, " +
		"`customer_id` bigint DEFAULT NULL, " +
		"PRIMARY KEY (`id`), " +
		"UNIQUE KEY `index_integration_invoice_details_on_slug` (`slug`) USING BTREE, " +
		"UNIQUE KEY `index_unique_integration_invoice_details_not_deleted` (`user_id`,`reference`,`reference_scope`,`not_deleted`) USING BTREE, " +
		"KEY `index_integration_invoice_details_on_customer_id` (`customer_id`) USING BTREE" +
		") ENGINE=InnoDB"

	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stmt.Type != DDLTypeCreateTable {
		t.Fatalf("expected CREATE_TABLE, got %s", stmt.Type)
	}

	if stmt.Table != "integration_invoice_details" {
		t.Fatalf("expected table 'integration_invoice_details', got %s", stmt.Table)
	}

	// Should only contain actual columns, not constraint/index definitions
	if len(stmt.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(stmt.Columns))
	}

	expectedColumns := []string{"id", "slug", "user_id", "customer_id"}
	for i, col := range stmt.Columns {
		if col.Name != expectedColumns[i] {
			t.Errorf("column %d: expected %s, got %s", i, expectedColumns[i], col.Name)
		}
	}
}

func TestParseAlterTable_SkipsIndexOperations(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "ADD INDEX",
			sql:  "ALTER TABLE `integration_invoice_details` ADD INDEX `index_integration_invoice_details_on_repair_order_source` (`repair_order_source_type`, `repair_order_source_id`)",
		},
		{
			name: "ADD KEY",
			sql:  "ALTER TABLE `test_table` ADD KEY `idx_name` (`name`)",
		},
		{
			name: "ADD UNIQUE KEY",
			sql:  "ALTER TABLE `test_table` ADD UNIQUE KEY `idx_unique_name` (`name`)",
		},
		{
			name: "ADD UNIQUE INDEX",
			sql:  "ALTER TABLE `test_table` ADD UNIQUE INDEX `idx_unique_name` (`name`)",
		},
		{
			name: "ADD PRIMARY KEY",
			sql:  "ALTER TABLE `test_table` ADD PRIMARY KEY (`id`)",
		},
		{
			name: "DROP INDEX",
			sql:  "ALTER TABLE `test_table` DROP INDEX `idx_name`",
		},
		{
			name: "DROP KEY",
			sql:  "ALTER TABLE `test_table` DROP KEY `idx_name`",
		},
		{
			name: "DROP PRIMARY KEY",
			sql:  "ALTER TABLE `test_table` DROP PRIMARY KEY",
		},
		{
			name: "DROP FOREIGN KEY",
			sql:  "ALTER TABLE `test_table` DROP FOREIGN KEY `fk_user`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewDDLParser(zap.NewNop())

			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if stmt.Type != DDLTypeAlterTable {
				t.Fatalf("expected ALTER_TABLE, got %s", stmt.Type)
			}

			// Index operations should be skipped, resulting in zero operations
			if len(stmt.Operations) != 0 {
				t.Errorf("expected 0 operations (index ops should be skipped), got %d", len(stmt.Operations))
			}
		})
	}
}

func TestParseAlterTable_MixedColumnAndIndexOperations(t *testing.T) {
	parser := NewDDLParser(zap.NewNop())

	sql := "ALTER TABLE `test_table` " +
		"ADD COLUMN `new_col` varchar(255) DEFAULT NULL, " +
		"ADD INDEX `idx_new_col` (`new_col`), " +
		"DROP COLUMN `old_col`"

	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 2 operations: ADD COLUMN and DROP COLUMN (index op skipped)
	if len(stmt.Operations) != 2 {
		t.Fatalf("expected 2 operations, got %d", len(stmt.Operations))
	}

	if stmt.Operations[0].Action != DDLActionAddColumn {
		t.Errorf("expected ADD_COLUMN, got %s", stmt.Operations[0].Action)
	}
	if stmt.Operations[0].Column.Name != "new_col" {
		t.Errorf("expected column name 'new_col', got %s", stmt.Operations[0].Column.Name)
	}

	if stmt.Operations[1].Action != DDLActionDropColumn {
		t.Errorf("expected DROP_COLUMN, got %s", stmt.Operations[1].Action)
	}
}

func TestIsConstraintDefinition(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"PRIMARY KEY (`id`)", true},
		{"KEY `idx_name` (`name`) USING BTREE", true},
		{"INDEX `idx_name` (`name`)", true},
		{"UNIQUE KEY `idx_name` (`name`) USING BTREE", true},
		{"FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)", true},
		{"CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)", true},
		{"`id` bigint NOT NULL", false},
		{"`name` varchar(255) DEFAULT NULL", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := isConstraintDefinition(tt.input); got != tt.expected {
				t.Errorf("isConstraintDefinition(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestIsIndexOperation(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"ADD INDEX `idx_name` (`name`)", true},
		{"ADD KEY `idx_name` (`name`)", true},
		{"ADD UNIQUE KEY `idx_name` (`name`)", true},
		{"ADD UNIQUE INDEX `idx_name` (`name`)", true},
		{"ADD PRIMARY KEY (`id`)", true},
		{"ADD FULLTEXT INDEX `idx_name` (`name`)", true},
		{"ADD SPATIAL INDEX `idx_name` (`geom`)", true},
		{"DROP INDEX `idx_name`", true},
		{"DROP KEY `idx_name`", true},
		{"DROP PRIMARY KEY", true},
		{"DROP FOREIGN KEY `fk_user`", true},
		{"ADD COLUMN `name` VARCHAR(255)", false},
		{"DROP COLUMN `name`", false},
		{"MODIFY COLUMN `name` VARCHAR(512)", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := isIndexOperation(tt.input); got != tt.expected {
				t.Errorf("isIndexOperation(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
