package schema

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/common"
)

// DDLCacheUpdater handles updating the schema cache after DDL operations are applied
type DDLCacheUpdater struct {
	cacheManager *CacheManager
	translator   *Translator
	logger       *zap.Logger
}

// NewDDLCacheUpdater creates a new DDL cache updater
func NewDDLCacheUpdater(cacheManager *CacheManager, translator *Translator, logger *zap.Logger) *DDLCacheUpdater {
	return &DDLCacheUpdater{
		cacheManager: cacheManager,
		translator:   translator,
		logger:       logger,
	}
}

// GetTableSchema retrieves table schema from cache
func (u *DDLCacheUpdater) GetTableSchema(database, table string) (*common.TableInfo, error) {
	return u.cacheManager.GetTableSchema(database, table)
}

// InvalidateTable removes a table from the cache, forcing a fresh lookup on next access
func (u *DDLCacheUpdater) InvalidateTable(database, table string) {
	u.cacheManager.RemoveTableSchema(database, table)
	u.logger.Info("Invalidated table schema cache",
		zap.String("database", database),
		zap.String("table", table))
}

// UpdateCacheAfterDDL updates the schema cache based on the applied DDL operation
// targetDatabase and targetTable are the actual ClickHouse database/table names where the DDL was applied
func (u *DDLCacheUpdater) UpdateCacheAfterDDL(mysqlDDL *DDLStatement, targetDatabase, targetTable string) error {
	switch mysqlDDL.Type {
	case DDLTypeCreateTable:
		return u.handleCreateTable(mysqlDDL, targetDatabase, targetTable)
	case DDLTypeAlterTable:
		return u.handleAlterTable(mysqlDDL, targetDatabase, targetTable)
	case DDLTypeDropTable:
		return u.handleDropTable(mysqlDDL, targetDatabase, targetTable)
	default:
		u.logger.Debug("Ignoring unsupported DDL type for cache update",
			zap.String("type", string(mysqlDDL.Type)),
			zap.String("database", targetDatabase),
			zap.String("table", targetTable))
		return nil
	}
}

// handleCreateTable adds a new table schema to the cache
func (u *DDLCacheUpdater) handleCreateTable(mysqlDDL *DDLStatement, targetDatabase, targetTable string) error {
	tableInfo := &common.TableInfo{
		Database:    targetDatabase,
		Name:        targetTable,
		Columns:     make(map[string]common.Column),
		ColumnOrder: make([]string, 0, len(mysqlDDL.Columns)),
	}

	// Convert DDL columns to common.Column format
	for _, ddlCol := range mysqlDDL.Columns {
		column := common.Column{
			Name:          ddlCol.Name,
			Type:          ddlCol.Type,
			Nullable:      ddlCol.Nullable,
			DefaultValue:  ddlCol.DefaultValue,
			IsPrimaryKey:  ddlCol.Attributes["primary_key"] == "true",
			AutoIncrement: ddlCol.Attributes["auto_increment"] == "true",
		}

		tableInfo.Columns[ddlCol.Name] = column
		tableInfo.ColumnOrder = append(tableInfo.ColumnOrder, ddlCol.Name)
	}

	// Store in cache
	u.cacheManager.SetTableSchema(tableInfo)

	u.logger.Info("Created table schema in cache",
		zap.String("database", targetDatabase),
		zap.String("table", targetTable),
		zap.Int("columns", len(tableInfo.Columns)))

	return nil
}

// handleAlterTable updates an existing table schema in the cache
func (u *DDLCacheUpdater) handleAlterTable(mysqlDDL *DDLStatement, targetDatabase, targetTable string) error {
	// Get current schema from cache
	currentSchema, err := u.cacheManager.GetTableSchema(targetDatabase, targetTable)
	if err != nil {
		u.logger.Warn("Table not found in cache during ALTER, will skip cache update",
			zap.String("database", targetDatabase),
			zap.String("table", targetTable),
			zap.Error(err))
		return nil // Don't fail if table not in cache
	}

	// Create a copy to modify
	newSchema := &common.TableInfo{
		Database:    currentSchema.Database,
		Name:        currentSchema.Name,
		Engine:      currentSchema.Engine,
		Columns:     make(map[string]common.Column),
		ColumnOrder: make([]string, len(currentSchema.ColumnOrder)),
	}

	// Copy existing columns
	for name, col := range currentSchema.Columns {
		newSchema.Columns[name] = common.Column{
			Name:          col.Name,
			Type:          col.Type,
			Nullable:      col.Nullable,
			DefaultValue:  col.DefaultValue,
			IsPrimaryKey:  col.IsPrimaryKey,
			AutoIncrement: col.AutoIncrement,
		}
	}
	copy(newSchema.ColumnOrder, currentSchema.ColumnOrder)

	// Apply each ALTER operation
	for _, operation := range mysqlDDL.Operations {
		if err := u.applyAlterOperation(newSchema, operation); err != nil {
			return fmt.Errorf("failed to apply ALTER operation: %w", err)
		}
	}

	// Update cache with modified schema
	u.cacheManager.SetTableSchema(newSchema)

	u.logger.Info("Updated table schema in cache",
		zap.String("database", targetDatabase),
		zap.String("table", targetTable),
		zap.Int("operations", len(mysqlDDL.Operations)))

	return nil
}

// handleDropTable removes a table schema from the cache
func (u *DDLCacheUpdater) handleDropTable(mysqlDDL *DDLStatement, targetDatabase, targetTable string) error {
	u.cacheManager.RemoveTableSchema(targetDatabase, targetTable)

	u.logger.Info("Removed table schema from cache",
		zap.String("database", targetDatabase),
		zap.String("table", targetTable))

	return nil
}

// applyAlterOperation applies a single ALTER operation to a table schema
func (u *DDLCacheUpdater) applyAlterOperation(schema *common.TableInfo, operation DDLOperation) error {
	switch operation.Action {
	case DDLActionAddColumn:
		return u.addColumn(schema, operation.Column)
	case DDLActionDropColumn:
		return u.dropColumn(schema, operation.Column.Name)
	case DDLActionModifyColumn:
		return u.modifyColumn(schema, operation.Column)
	case DDLActionChangeColumn:
		return u.changeColumn(schema, operation.OldName, operation.Column)
	default:
		u.logger.Debug("Ignoring unsupported ALTER operation for cache update",
			zap.String("action", string(operation.Action)),
			zap.String("database", schema.Database),
			zap.String("table", schema.Name))
		return nil
	}
}

// addColumn adds a new column to the schema
func (u *DDLCacheUpdater) addColumn(schema *common.TableInfo, ddlColumn *DDLColumn) error {
	if _, exists := schema.Columns[ddlColumn.Name]; exists {
		return fmt.Errorf("column %s already exists", ddlColumn.Name)
	}

	column := common.Column{
		Name:          ddlColumn.Name,
		Type:          ddlColumn.Type,
		Nullable:      ddlColumn.Nullable,
		DefaultValue:  ddlColumn.DefaultValue,
		IsPrimaryKey:  ddlColumn.Attributes["primary_key"] == "true",
		AutoIncrement: ddlColumn.Attributes["auto_increment"] == "true",
	}

	schema.Columns[ddlColumn.Name] = column
	schema.ColumnOrder = append(schema.ColumnOrder, ddlColumn.Name)

	u.logger.Debug("Added column to schema",
		zap.String("table", schema.Name),
		zap.String("column", ddlColumn.Name),
		zap.String("type", ddlColumn.Type))

	return nil
}

// dropColumn removes a column from the schema
func (u *DDLCacheUpdater) dropColumn(schema *common.TableInfo, columnName string) error {
	if _, exists := schema.Columns[columnName]; !exists {
		return fmt.Errorf("column %s does not exist", columnName)
	}

	// Remove from columns map
	delete(schema.Columns, columnName)

	// Remove from column order
	for i, name := range schema.ColumnOrder {
		if name == columnName {
			schema.ColumnOrder = append(schema.ColumnOrder[:i], schema.ColumnOrder[i+1:]...)
			break
		}
	}

	u.logger.Debug("Dropped column from schema",
		zap.String("table", schema.Name),
		zap.String("column", columnName))

	return nil
}

// modifyColumn updates an existing column definition
func (u *DDLCacheUpdater) modifyColumn(schema *common.TableInfo, ddlColumn *DDLColumn) error {
	if _, exists := schema.Columns[ddlColumn.Name]; !exists {
		return fmt.Errorf("column %s does not exist", ddlColumn.Name)
	}

	column := common.Column{
		Name:          ddlColumn.Name,
		Type:          ddlColumn.Type,
		Nullable:      ddlColumn.Nullable,
		DefaultValue:  ddlColumn.DefaultValue,
		IsPrimaryKey:  ddlColumn.Attributes["primary_key"] == "true",
		AutoIncrement: ddlColumn.Attributes["auto_increment"] == "true",
	}

	schema.Columns[ddlColumn.Name] = column

	u.logger.Debug("Modified column in schema",
		zap.String("table", schema.Name),
		zap.String("column", ddlColumn.Name),
		zap.String("type", ddlColumn.Type))

	return nil
}

// changeColumn renames and/or modifies a column
func (u *DDLCacheUpdater) changeColumn(schema *common.TableInfo, oldName string, ddlColumn *DDLColumn) error {
	if _, exists := schema.Columns[oldName]; !exists {
		return fmt.Errorf("column %s does not exist", oldName)
	}

	// Create new column
	column := common.Column{
		Name:          ddlColumn.Name,
		Type:          ddlColumn.Type,
		Nullable:      ddlColumn.Nullable,
		DefaultValue:  ddlColumn.DefaultValue,
		IsPrimaryKey:  ddlColumn.Attributes["primary_key"] == "true",
		AutoIncrement: ddlColumn.Attributes["auto_increment"] == "true",
	}

	// Remove old column
	delete(schema.Columns, oldName)

	// Add new column
	schema.Columns[ddlColumn.Name] = column

	// Update column order
	for i, name := range schema.ColumnOrder {
		if name == oldName {
			schema.ColumnOrder[i] = ddlColumn.Name
			break
		}
	}

	u.logger.Debug("Changed column in schema",
		zap.String("table", schema.Name),
		zap.String("old_name", oldName),
		zap.String("new_name", ddlColumn.Name),
		zap.String("type", ddlColumn.Type))

	return nil
}
