package schema

import (
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/common"
)

// CacheManager manages table schema information in memory
// Thread-safe operations for getting, setting, and updating table schemas
type CacheManager struct {
	schemaCache map[string]*common.TableInfo
	mu          sync.RWMutex
	logger      *zap.Logger
}

// NewCacheManager creates a new schema cache manager
func NewCacheManager(logger *zap.Logger) *CacheManager {
	return &CacheManager{
		schemaCache: make(map[string]*common.TableInfo),
		logger:      logger,
	}
}

// GetTableSchema retrieves table schema from cache
func (c *CacheManager) GetTableSchema(database, table string) (*common.TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := fmt.Sprintf("%s.%s", database, table)
	if schema, exists := c.schemaCache[key]; exists {
		// Return a deep copy to prevent external modifications
		schemaCopy := c.copyTableInfo(schema)
		return schemaCopy, nil
	}

	return nil, fmt.Errorf("table schema not found: %s.%s", database, table)
}

// SetTableSchema stores a table schema in cache
func (c *CacheManager) SetTableSchema(tableInfo *common.TableInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", tableInfo.Database, tableInfo.Name)
	c.schemaCache[key] = c.copyTableInfo(tableInfo)

	c.logger.Debug("Schema stored in cache",
		zap.String("database", tableInfo.Database),
		zap.String("table", tableInfo.Name),
		zap.Int("columns", len(tableInfo.Columns)))
}

// UpdateTableSchema updates an existing table schema or creates it if it doesn't exist
func (c *CacheManager) UpdateTableSchema(tableInfo *common.TableInfo) {
	c.SetTableSchema(tableInfo) // SetTableSchema already handles create/update
}

// RemoveTableSchema removes a table schema from cache
func (c *CacheManager) RemoveTableSchema(database, table string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s.%s", database, table)
	if _, exists := c.schemaCache[key]; exists {
		delete(c.schemaCache, key)
		c.logger.Debug("Schema removed from cache",
			zap.String("database", database),
			zap.String("table", table))
	}
}

// GetAllTables returns all cached table schemas
func (c *CacheManager) GetAllTables() map[string]*common.TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make(map[string]*common.TableInfo)
	for key, schema := range c.schemaCache {
		result[key] = c.copyTableInfo(schema)
	}
	return result
}

// HasTable checks if a table schema exists in cache
func (c *CacheManager) HasTable(database, table string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := fmt.Sprintf("%s.%s", database, table)
	_, exists := c.schemaCache[key]
	return exists
}

// CacheStats returns statistics about the cache
func (c *CacheManager) CacheStats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"cached_tables": len(c.schemaCache),
	}
}

// LoadFromDiscovery initializes cache with schemas from discovery
func (c *CacheManager) LoadFromDiscovery(schemas map[string]*common.TableInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, schema := range schemas {
		c.schemaCache[key] = c.copyTableInfo(schema)
	}

	c.logger.Info("Cache initialized from discovery",
		zap.Int("tables_loaded", len(schemas)))
}

// Dispose clears all cached schemas and frees memory
func (c *CacheManager) Dispose() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear the cache map to free memory
	c.schemaCache = make(map[string]*common.TableInfo)

	c.logger.Info("Schema cache disposed and memory freed")
}

// copyTableInfo creates a deep copy of TableInfo to prevent external modifications
func (c *CacheManager) copyTableInfo(original *common.TableInfo) *common.TableInfo {
	if original == nil {
		return nil
	}

	copy := &common.TableInfo{
		Database:    original.Database,
		Name:        original.Name,
		Engine:      original.Engine,
		Columns:     make(map[string]common.Column),
		ColumnOrder: make([]string, len(original.ColumnOrder)),
	}

	// Deep copy columns
	for name, col := range original.Columns {
		copy.Columns[name] = common.Column{
			Name:          col.Name,
			Type:          col.Type,
			Nullable:      col.Nullable,
			DefaultValue:  col.DefaultValue,
			IsPrimaryKey:  col.IsPrimaryKey,
			AutoIncrement: col.AutoIncrement,
		}
	}

	// Copy column order
	copy.ColumnOrder = append(copy.ColumnOrder[:0], original.ColumnOrder...)

	return copy
}
