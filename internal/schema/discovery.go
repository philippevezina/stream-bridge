package schema

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-mysql-org/go-mysql/client"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/mysql/connector"
)

type Discovery struct {
	cfg         *config.MySQLConfig
	logger      *zap.Logger
	connector   *connector.Connector
	conn        *client.Conn
	schemaCache map[string]*common.TableInfo
	mu          sync.RWMutex
	running     bool
	stopChan    chan struct{}
}

func NewDiscovery(cfg *config.MySQLConfig, logger *zap.Logger) *Discovery {
	return &Discovery{
		cfg:         cfg,
		logger:      logger,
		connector:   connector.New(cfg, logger),
		schemaCache: make(map[string]*common.TableInfo),
		stopChan:    make(chan struct{}),
	}
}

func (d *Discovery) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return fmt.Errorf("schema discovery is already running")
	}

	if err := d.connect(); err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	if err := d.initialSchemaLoad(ctx); err != nil {
		return fmt.Errorf("failed to load initial schema: %w", err)
	}

	d.running = true

	d.logger.Info("Schema discovery started",
		zap.String("host", d.cfg.Host),
		zap.Int("port", d.cfg.Port),
		zap.String("database", d.cfg.Database))

	return nil
}

func (d *Discovery) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}

	close(d.stopChan)
	d.running = false

	if d.conn != nil {
		d.conn.Close()
	}

	d.logger.Info("Schema discovery stopped")
	return nil
}

// Dispose clears all cached schemas and frees memory
func (d *Discovery) Dispose() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Clear the schema cache to free memory
	d.schemaCache = make(map[string]*common.TableInfo)

	d.logger.Info("Schema discovery cache disposed and memory freed")
}

func (d *Discovery) GetTableSchema(database, table string) (*common.TableInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	key := fmt.Sprintf("%s.%s", database, table)
	if schema, exists := d.schemaCache[key]; exists {
		schemaCopy := *schema
		return &schemaCopy, nil
	}

	return nil, fmt.Errorf("table schema not found: %s.%s", database, table)
}

func (d *Discovery) GetAllTables() map[string]*common.TableInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[string]*common.TableInfo)
	for key, schema := range d.schemaCache {
		schemaCopy := *schema
		result[key] = &schemaCopy
	}
	return result
}

func (d *Discovery) IsRunning() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.running
}

func (d *Discovery) connect() error {
	conn, err := d.connector.Connect(d.cfg.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	d.conn = conn
	return nil
}

func (d *Discovery) initialSchemaLoad(ctx context.Context) error {
	tables, err := d.getTableList(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table list: %w", err)
	}

	d.logger.Info("Loading initial schema",
		zap.Int("table_count", len(tables)))

	for _, tableName := range tables {
		schema, err := d.loadTableSchema(ctx, d.cfg.Database, tableName)
		if err != nil {
			d.logger.Warn("Failed to load schema for table",
				zap.String("table", tableName),
				zap.Error(err))
			continue
		}

		key := fmt.Sprintf("%s.%s", d.cfg.Database, tableName)
		d.schemaCache[key] = schema
	}

	d.logger.Info("Initial schema load completed",
		zap.Int("loaded_tables", len(d.schemaCache)))

	return nil
}

func (d *Discovery) getTableList(_ context.Context) ([]string, error) {
	query := "SHOW TABLES"
	result, err := d.conn.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute SHOW TABLES: %w", err)
	}

	var tables []string
	for i := 0; i < result.RowNumber(); i++ {
		tableName, _ := result.GetString(i, 0)
		tables = append(tables, tableName)
	}

	return tables, nil
}

func (d *Discovery) loadTableSchema(_ context.Context, database, table string) (*common.TableInfo, error) {
	query := fmt.Sprintf(`
		SELECT 
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COLUMN_KEY,
			EXTRA
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
		ORDER BY ORDINAL_POSITION
	`, database, table)

	result, err := d.conn.Execute(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}

	if result.RowNumber() == 0 {
		return nil, fmt.Errorf("table not found: %s.%s", database, table)
	}

	tableInfo := &common.TableInfo{
		Database:    database,
		Name:        table,
		Columns:     make(map[string]common.Column),
		ColumnOrder: make([]string, 0, result.RowNumber()),
	}

	for i := 0; i < result.RowNumber(); i++ {
		columnName, _ := result.GetString(i, 0)
		dataType, _ := result.GetString(i, 1)
		isNullable, _ := result.GetString(i, 2)
		columnDefault, _ := result.GetString(i, 3)
		columnKey, _ := result.GetString(i, 4)
		extra, _ := result.GetString(i, 5)

		column := common.Column{
			Name:          columnName,
			Type:          dataType,
			Nullable:      isNullable == "YES",
			DefaultValue:  columnDefault,
			IsPrimaryKey:  columnKey == "PRI",
			AutoIncrement: extra == "auto_increment",
		}

		tableInfo.Columns[columnName] = column
		tableInfo.ColumnOrder = append(tableInfo.ColumnOrder, columnName)
	}

	return tableInfo, nil
}
