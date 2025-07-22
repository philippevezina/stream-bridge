package schema

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/security"
)

type DDLTranslator struct {
	translator *Translator
	logger     *zap.Logger
}

type ClickHouseDDL struct {
	Statements    []string          `json:"statements"`
	Type          DDLType           `json:"type"`
	Database      string            `json:"database"`
	Table         string            `json:"table"`
	IsDestructive bool              `json:"is_destructive"`
	Warnings      []string          `json:"warnings"`
	Metadata      map[string]string `json:"metadata"`
}

func NewDDLTranslator(translator *Translator, logger *zap.Logger) *DDLTranslator {
	return &DDLTranslator{
		translator: translator,
		logger:     logger,
	}
}

func (dt *DDLTranslator) Translate(mysqlDDL *DDLStatement, targetEngine clickhouse.TableEngine, targetDatabase string) (*ClickHouseDDL, error) {
	if mysqlDDL == nil {
		return nil, fmt.Errorf("mysql DDL statement cannot be nil")
	}

	// Validate target engine before proceeding
	if err := dt.validateTableEngine(targetEngine); err != nil {
		return nil, fmt.Errorf("invalid target engine: %w", err)
	}

	// Validate target database
	if targetDatabase == "" {
		return nil, fmt.Errorf("target database cannot be empty")
	}

	chDDL := &ClickHouseDDL{
		Type:     mysqlDDL.Type,
		Database: targetDatabase,
		Table:    mysqlDDL.Table,
		Metadata: make(map[string]string),
	}

	switch mysqlDDL.Type {
	case DDLTypeCreateTable:
		return dt.translateCreateTable(mysqlDDL, targetEngine, chDDL)
	case DDLTypeAlterTable:
		return dt.translateAlterTable(mysqlDDL, targetEngine, chDDL)
	case DDLTypeDropTable:
		return dt.translateDropTable(mysqlDDL, chDDL)
	default:
		return nil, fmt.Errorf("unsupported DDL type: %s", mysqlDDL.Type)
	}
}

func (dt *DDLTranslator) translateCreateTable(mysqlDDL *DDLStatement, targetEngine clickhouse.TableEngine, chDDL *ClickHouseDDL) (*ClickHouseDDL, error) {
	chSchema := &ClickHouseSchema{
		Database: chDDL.Database,
		Table:    mysqlDDL.Table,
		Engine:   targetEngine,
		Settings: make(map[string]string),
	}

	var primaryKeys []string

	for _, mysqlCol := range mysqlDDL.Columns {
		chCol, err := dt.translateDDLColumn(mysqlCol)
		if err != nil {
			dt.logger.Warn("Failed to translate column in DDL, using String fallback",
				zap.String("column", mysqlCol.Name),
				zap.String("type", mysqlCol.Type),
				zap.Error(err))

			// Use String fallback but preserve other column properties and add warning
			chCol = &ClickHouseColumn{
				Name:         mysqlCol.Name,
				Type:         "String",
				DefaultValue: dt.translateFallbackDefaultValue(mysqlCol.DefaultValue),
				Comment:      mysqlCol.Comment,
				Attributes:   mysqlCol.Attributes,
			}

			// Add warning about type conversion to track potential data issues
			chDDL.Warnings = append(chDDL.Warnings,
				fmt.Sprintf("Column '%s' type '%s' could not be translated, using String fallback",
					mysqlCol.Name, mysqlCol.Type))
		}

		if mysqlCol.Attributes["primary_key"] == "true" {
			primaryKeys = append(primaryKeys, mysqlCol.Name)
		}

		chSchema.Columns = append(chSchema.Columns, *chCol)
	}

	chSchema.PrimaryKey = primaryKeys

	if len(primaryKeys) > 0 {
		chSchema.OrderBy = primaryKeys
	} else {
		for _, col := range chSchema.Columns {
			if !strings.Contains(col.Type, "Array") &&
				!strings.Contains(col.Type, "Map") {
				chSchema.OrderBy = []string{col.Name}
				break
			}
		}
	}

	// Add required columns for ReplacingMergeTree engines
	switch targetEngine {
	case clickhouse.EngineReplacingMergeTree, clickhouse.EngineReplicatedReplacingMergeTree:
		// Check if _is_deleted column already exists
		hasIsDeleted := false
		hasVersion := false
		for _, col := range chSchema.Columns {
			if col.Name == "_is_deleted" {
				hasIsDeleted = true
			}
			if col.Name == "_version" {
				hasVersion = true
			}
		}

		// Add _is_deleted column if missing (for soft deletes)
		if !hasIsDeleted {
			chSchema.Columns = append(chSchema.Columns, ClickHouseColumn{
				Name:         "_is_deleted",
				Type:         "UInt8",
				DefaultValue: "0",
			})
		}

		// Add _version column if missing (for ReplacingMergeTree version tracking)
		if !hasVersion {
			chSchema.Columns = append(chSchema.Columns, ClickHouseColumn{
				Name:         "_version",
				Type:         "UInt64",
				DefaultValue: "now64()",
			})
		}

		chSchema.Settings["allow_nullable_key"] = "1"
	default:
		return nil, fmt.Errorf("unsupported table engine: %s. Only ReplacingMergeTree and ReplicatedReplacingMergeTree are supported", targetEngine)
	}

	ddlStatement, err := dt.translator.GenerateCreateTableDDL(chSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ClickHouse CREATE TABLE DDL: %w", err)
	}

	chDDL.Statements = []string{ddlStatement}
	chDDL.Metadata["engine"] = string(targetEngine)
	chDDL.Metadata["column_count"] = fmt.Sprintf("%d", len(chSchema.Columns))

	dt.logger.Debug("Translated CREATE TABLE DDL",
		zap.String("database", chSchema.Database),
		zap.String("table", chSchema.Table),
		zap.String("engine", string(targetEngine)))

	return chDDL, nil
}

func (dt *DDLTranslator) translateAlterTable(mysqlDDL *DDLStatement, _ clickhouse.TableEngine, chDDL *ClickHouseDDL) (*ClickHouseDDL, error) {
	var statements []string
	var warnings []string

	for _, operation := range mysqlDDL.Operations {
		switch operation.Action {
		case DDLActionAddColumn:
			stmt, warning, err := dt.translateAddColumn(chDDL.Database, mysqlDDL.Table, operation)
			if err != nil {
				return nil, fmt.Errorf("failed to translate ADD COLUMN: %w", err)
			}
			if stmt != "" {
				statements = append(statements, stmt)
			}
			if warning != "" {
				warnings = append(warnings, warning)
			}

		case DDLActionDropColumn:
			stmt, warning, err := dt.translateDropColumn(chDDL.Database, mysqlDDL.Table, operation)
			if err != nil {
				return nil, fmt.Errorf("failed to translate DROP COLUMN: %w", err)
			}
			if stmt != "" {
				statements = append(statements, stmt)
				chDDL.IsDestructive = true
			}
			if warning != "" {
				warnings = append(warnings, warning)
			}

		case DDLActionModifyColumn:
			stmt, warning, err := dt.translateModifyColumn(chDDL.Database, mysqlDDL.Table, operation)
			if err != nil {
				return nil, fmt.Errorf("failed to translate MODIFY COLUMN: %w", err)
			}
			if stmt != "" {
				statements = append(statements, stmt)
			}
			if warning != "" {
				warnings = append(warnings, warning)
			}

		case DDLActionChangeColumn:
			stmt, warning, err := dt.translateChangeColumn(chDDL.Database, mysqlDDL.Table, operation)
			if err != nil {
				return nil, fmt.Errorf("failed to translate CHANGE COLUMN: %w", err)
			}
			if stmt != "" {
				statements = append(statements, stmt)
			}
			if warning != "" {
				warnings = append(warnings, warning)
			}

		default:
			warning := fmt.Sprintf("Unsupported ALTER operation: %s", operation.Action)
			warnings = append(warnings, warning)
			dt.logger.Warn("Unsupported ALTER operation",
				zap.String("action", string(operation.Action)),
				zap.String("database", chDDL.Database),
				zap.String("table", mysqlDDL.Table))
		}
	}

	chDDL.Statements = statements
	chDDL.Warnings = warnings
	chDDL.Metadata["operation_count"] = fmt.Sprintf("%d", len(mysqlDDL.Operations))
	chDDL.Metadata["statement_count"] = fmt.Sprintf("%d", len(statements))

	dt.logger.Debug("Translated ALTER TABLE DDL",
		zap.String("database", chDDL.Database),
		zap.String("table", mysqlDDL.Table),
		zap.Int("operations", len(mysqlDDL.Operations)),
		zap.Int("statements", len(statements)),
		zap.Int("warnings", len(warnings)))

	return chDDL, nil
}

func (dt *DDLTranslator) translateDropTable(mysqlDDL *DDLStatement, chDDL *ClickHouseDDL) (*ClickHouseDDL, error) {
	// SECURITY: Validate identifiers before using in SQL
	if err := security.ValidateIdentifier(chDDL.Database, "database name"); err != nil {
		return nil, fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(mysqlDDL.Table, "table name"); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	escapedDB := security.EscapeIdentifier(chDDL.Database)
	escapedTable := security.EscapeIdentifier(mysqlDDL.Table)
	dropStatement := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", escapedDB, escapedTable)

	chDDL.Statements = []string{dropStatement}
	chDDL.IsDestructive = true
	chDDL.Metadata["destructive"] = "true"

	dt.logger.Debug("Translated DROP TABLE DDL",
		zap.String("database", chDDL.Database),
		zap.String("table", mysqlDDL.Table))

	return chDDL, nil
}

func (dt *DDLTranslator) translateAddColumn(database, table string, operation DDLOperation) (string, string, error) {
	// SECURITY: Validate identifiers
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return "", "", fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return "", "", fmt.Errorf("invalid table name: %w", err)
	}

	if operation.Column == nil {
		return "", "", fmt.Errorf("ADD COLUMN operation missing column definition")
	}

	chCol, err := dt.translateDDLColumn(*operation.Column)
	if err != nil {
		return "", "", fmt.Errorf("failed to translate column type: %w", err)
	}

	// SECURITY: Validate column name
	if err := security.ValidateIdentifier(chCol.Name, "column name"); err != nil {
		return "", "", fmt.Errorf("invalid column name: %w", err)
	}

	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(table)
	escapedCol := security.EscapeIdentifier(chCol.Name)

	var alterStatement strings.Builder
	// Use IF NOT EXISTS for idempotency - safe to retry if column already exists
	alterStatement.WriteString(fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s",
		escapedDB, escapedTable, escapedCol, chCol.Type))

	if chCol.DefaultValue != "" {
		alterStatement.WriteString(fmt.Sprintf(" DEFAULT %s", chCol.DefaultValue))
	}

	warning := ""
	if strings.Contains(chCol.Type, "Nullable") {
		warning = fmt.Sprintf("Added nullable column '%s' - existing rows will have NULL values", chCol.Name)
	}

	return alterStatement.String(), warning, nil
}

func (dt *DDLTranslator) translateDropColumn(database, table string, operation DDLOperation) (string, string, error) {
	// SECURITY: Validate identifiers
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return "", "", fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return "", "", fmt.Errorf("invalid table name: %w", err)
	}

	if operation.Column == nil {
		return "", "", fmt.Errorf("DROP COLUMN operation missing column definition")
	}

	// SECURITY: Validate column name
	if err := security.ValidateIdentifier(operation.Column.Name, "column name"); err != nil {
		return "", "", fmt.Errorf("invalid column name: %w", err)
	}

	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(table)
	escapedCol := security.EscapeIdentifier(operation.Column.Name)

	// Use IF EXISTS for idempotency - safe to retry if column already dropped
	alterStatement := fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s",
		escapedDB, escapedTable, escapedCol)

	warning := fmt.Sprintf("Dropping column '%s' is destructive and cannot be undone", operation.Column.Name)

	return alterStatement, warning, nil
}

func (dt *DDLTranslator) translateModifyColumn(database, table string, operation DDLOperation) (string, string, error) {
	// SECURITY: Validate identifiers
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return "", "", fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return "", "", fmt.Errorf("invalid table name: %w", err)
	}

	if operation.Column == nil {
		return "", "", fmt.Errorf("MODIFY COLUMN operation missing column definition")
	}

	chCol, err := dt.translateDDLColumn(*operation.Column)
	if err != nil {
		return "", "", fmt.Errorf("failed to translate column type: %w", err)
	}

	// SECURITY: Validate column name
	if err := security.ValidateIdentifier(chCol.Name, "column name"); err != nil {
		return "", "", fmt.Errorf("invalid column name: %w", err)
	}

	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(table)
	escapedCol := security.EscapeIdentifier(chCol.Name)

	var alterStatement strings.Builder
	alterStatement.WriteString(fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s",
		escapedDB, escapedTable, escapedCol, chCol.Type))

	if chCol.DefaultValue != "" {
		alterStatement.WriteString(fmt.Sprintf(" DEFAULT %s", chCol.DefaultValue))
	}

	warning := ""
	if strings.Contains(chCol.Type, "String") && operation.Column.Type != "String" {
		warning = fmt.Sprintf("Column '%s' type changed to String - data conversion may be lossy", chCol.Name)
	}

	return alterStatement.String(), warning, nil
}

func (dt *DDLTranslator) translateChangeColumn(database, table string, operation DDLOperation) (string, string, error) {
	// SECURITY: Validate identifiers
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return "", "", fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return "", "", fmt.Errorf("invalid table name: %w", err)
	}

	if operation.Column == nil {
		return "", "", fmt.Errorf("CHANGE COLUMN operation missing column definition")
	}

	if operation.OldName == "" {
		return "", "", fmt.Errorf("CHANGE COLUMN operation missing old column name")
	}

	// SECURITY: Validate column names
	if err := security.ValidateIdentifier(operation.OldName, "old column name"); err != nil {
		return "", "", fmt.Errorf("invalid old column name: %w", err)
	}

	chCol, err := dt.translateDDLColumn(*operation.Column)
	if err != nil {
		return "", "", fmt.Errorf("failed to translate column type: %w", err)
	}

	// SECURITY: Validate new column name
	if err := security.ValidateIdentifier(chCol.Name, "new column name"); err != nil {
		return "", "", fmt.Errorf("invalid new column name: %w", err)
	}

	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(table)
	escapedOldCol := security.EscapeIdentifier(operation.OldName)
	escapedNewCol := security.EscapeIdentifier(operation.Column.Name)

	var alterStatement strings.Builder

	if operation.OldName != operation.Column.Name {
		alterStatement.WriteString(fmt.Sprintf("ALTER TABLE %s.%s RENAME COLUMN %s TO %s",
			escapedDB, escapedTable, escapedOldCol, escapedNewCol))

		alterStatement.WriteString("; ")
	}

	alterStatement.WriteString(fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN %s %s",
		escapedDB, escapedTable, escapedNewCol, chCol.Type))

	if chCol.DefaultValue != "" {
		alterStatement.WriteString(fmt.Sprintf(" DEFAULT %s", chCol.DefaultValue))
	}

	warning := ""
	if operation.OldName != operation.Column.Name {
		warning = fmt.Sprintf("Column renamed from '%s' to '%s' and type changed", operation.OldName, operation.Column.Name)
	}

	return alterStatement.String(), warning, nil
}

func (dt *DDLTranslator) translateDDLColumn(mysqlCol DDLColumn) (*ClickHouseColumn, error) {
	clickhouseType, err := dt.translator.mapMySQLTypeToClickHouse(mysqlCol.Type)
	if err != nil {
		return nil, err
	}

	if dt.translator.preserveNullability && mysqlCol.Nullable {
		if mysqlCol.Attributes["primary_key"] != "true" {
			clickhouseType = fmt.Sprintf("Nullable(%s)", clickhouseType)
		}
	}

	chCol := &ClickHouseColumn{
		Name:         mysqlCol.Name,
		Type:         clickhouseType,
		DefaultValue: dt.translator.translateDefaultValue(mysqlCol.DefaultValue, clickhouseType),
		Comment:      mysqlCol.Comment,
		Attributes:   mysqlCol.Attributes,
	}

	return chCol, nil
}

func (dt *DDLTranslator) IsDestructive(mysqlDDL *DDLStatement) bool {
	if mysqlDDL.Type == DDLTypeDropTable {
		return true
	}

	if mysqlDDL.Type == DDLTypeAlterTable {
		for _, operation := range mysqlDDL.Operations {
			if operation.Action == DDLActionDropColumn {
				return true
			}
		}
	}

	return false
}

func (dt *DDLTranslator) GetSupportedOperations() []DDLAction {
	return []DDLAction{
		DDLActionAddColumn,
		DDLActionDropColumn,
		DDLActionModifyColumn,
		DDLActionChangeColumn,
	}
}

// translateFallbackDefaultValue handles default value translation when falling back to String type
func (dt *DDLTranslator) translateFallbackDefaultValue(mysqlDefault string) string {
	if mysqlDefault == "" {
		return ""
	}

	// For String fallback, quote the default value if it's not already quoted or a function
	if strings.ToUpper(mysqlDefault) == "NULL" {
		return "NULL"
	}

	// Common MySQL functions that should be translated for ClickHouse String context
	switch strings.ToUpper(mysqlDefault) {
	case "CURRENT_TIMESTAMP", "NOW()":
		return "'1970-01-01 00:00:00'" // Safe default for String type
	case "CURRENT_DATE":
		return "'1970-01-01'" // Safe default for String type
	default:
		// If it's not already quoted, quote it for String type
		if !strings.HasPrefix(mysqlDefault, "'") && !strings.HasPrefix(mysqlDefault, "\"") {
			return fmt.Sprintf("'%s'", strings.ReplaceAll(mysqlDefault, "'", "\\'"))
		}
		return mysqlDefault
	}
}

// validateTableEngine validates that the target engine is supported
func (dt *DDLTranslator) validateTableEngine(engine clickhouse.TableEngine) error {
	switch engine {
	case clickhouse.EngineReplacingMergeTree, clickhouse.EngineReplicatedReplacingMergeTree:
		return nil
	case "":
		return fmt.Errorf("table engine cannot be empty")
	default:
		return fmt.Errorf("unsupported table engine '%s'. Only ReplacingMergeTree and ReplicatedReplacingMergeTree are supported", engine)
	}
}
