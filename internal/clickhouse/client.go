package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/security"
)

// TableInfoCache defines the interface for caching table schema information
type TableInfoCache interface {
	GetTableSchema(database, table string) (*common.TableInfo, error)
	SetTableSchema(tableInfo *common.TableInfo)
}

type Client struct {
	cfg          *config.ClickHouseConfig
	logger       *zap.Logger
	db           *sql.DB
	cacheManager TableInfoCache
}

type TableEngine string

const (
	EngineReplacingMergeTree           TableEngine = "ReplacingMergeTree"
	EngineReplicatedReplacingMergeTree TableEngine = "ReplicatedReplacingMergeTree"
)

func NewClient(cfg *config.ClickHouseConfig, cacheManager TableInfoCache, logger *zap.Logger) (*Client, error) {
	client := &Client{
		cfg:          cfg,
		logger:       logger,
		cacheManager: cacheManager,
	}

	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	return client, nil
}

// PopulateCacheFromClickHouse initializes the schema cache with existing ClickHouse tables
func (c *Client) PopulateCacheFromClickHouse(ctx context.Context) error {
	if c.cacheManager == nil {
		c.logger.Warn("No cache manager available, skipping cache population")
		return nil
	}

	c.logger.Info("Populating schema cache from ClickHouse system tables")

	// Get list of all tables in the configured database
	tables, err := c.getTableList(ctx)
	if err != nil {
		return fmt.Errorf("failed to get table list: %w", err)
	}

	c.logger.Info("Found tables in ClickHouse",
		zap.String("database", c.cfg.Database),
		zap.Int("table_count", len(tables)))

	// Populate cache for each table
	for _, tableName := range tables {
		tableInfo, err := c.queryTableInfoFromDatabase(ctx, c.cfg.Database, tableName)
		if err != nil {
			c.logger.Warn("Failed to load schema for table",
				zap.String("table", tableName),
				zap.Error(err))
			continue
		}

		c.cacheManager.SetTableSchema(tableInfo)
		c.logger.Debug("Cached schema for table",
			zap.String("table", tableName),
			zap.Int("columns", len(tableInfo.Columns)))
	}

	c.logger.Info("Schema cache population completed",
		zap.Int("tables_cached", len(tables)))

	return nil
}

// getTableList retrieves all table names from the configured ClickHouse database
func (c *Client) getTableList(ctx context.Context) ([]string, error) {
	query := `
		SELECT name 
		FROM system.tables 
		WHERE database = ? AND engine != 'View'
		ORDER BY name
	`

	rows, err := c.db.QueryContext(ctx, query, c.cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to query table list: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// IsValidEngine validates that the engine is one of the supported ReplacingMergeTree variants
func IsValidEngine(engine TableEngine) bool {
	return engine == EngineReplacingMergeTree || engine == EngineReplicatedReplacingMergeTree
}

func (c *Client) connect() error {
	options := &clickhouse.Options{
		Addr: c.cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: c.cfg.Database,
			Username: c.cfg.Username,
			Password: c.cfg.Password,
		},
		DialTimeout: c.cfg.DialTimeout,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	if c.cfg.EnableSSL {
		options.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	conn := clickhouse.OpenDB(options)
	conn.SetMaxOpenConns(c.cfg.MaxOpenConns)
	conn.SetMaxIdleConns(c.cfg.MaxIdleConns)
	conn.SetConnMaxLifetime(c.cfg.MaxLifetime)

	if err := conn.Ping(); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	c.db = conn
	c.logger.Info("Connected to ClickHouse",
		zap.Strings("addresses", c.cfg.Addresses),
		zap.String("database", c.cfg.Database))

	return nil
}

func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Client) Ping(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

func (c *Client) GetDB() *sql.DB {
	return c.db
}

func (c *Client) GetDatabase() string {
	return c.cfg.Database
}

func (c *Client) CreateTable(ctx context.Context, table *common.TableInfo, engine TableEngine) error {
	if !IsValidEngine(engine) {
		return fmt.Errorf("unsupported table engine: %s. Only ReplacingMergeTree and ReplicatedReplacingMergeTree are supported", engine)
	}

	query := c.buildCreateTableQuery(table, engine)

	c.logger.Debug("Creating table",
		zap.String("database", table.Database),
		zap.String("table", table.Name),
		zap.String("query", query))

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table %s.%s: %w", table.Database, table.Name, err)
	}

	c.logger.Info("Table created successfully",
		zap.String("database", table.Database),
		zap.String("table", table.Name))

	return nil
}

// DropTable drops a table from ClickHouse.
// The database parameter represents the source MySQL database but operations are performed
// in the configured ClickHouse database (c.cfg.Database) for consistent data organization.
func (c *Client) DropTable(ctx context.Context, database, table string) error {
	// SECURITY: Validate identifiers before using in SQL queries
	if err := security.ValidateIdentifier(c.cfg.Database, "database name"); err != nil {
		return fmt.Errorf("invalid database name in config: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// Note: 'database' parameter is the source MySQL database name, but we always use
	// the configured ClickHouse database for actual operations
	escapedDB := security.EscapeIdentifier(c.cfg.Database)
	escapedTable := security.EscapeIdentifier(table)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", escapedDB, escapedTable)

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop table %s.%s: %w", c.cfg.Database, table, err)
	}

	c.logger.Info("Table dropped successfully",
		zap.String("source_database", database),
		zap.String("clickhouse_database", c.cfg.Database),
		zap.String("table", table))

	return nil
}

// TableExists checks if a table exists in ClickHouse.
// The database parameter represents the source MySQL database but operations are performed
// in the configured ClickHouse database (c.cfg.Database) for consistent data organization.
func (c *Client) TableExists(ctx context.Context, database, table string) (bool, error) {
	// Note: 'database' parameter is the source MySQL database name, but we always use
	// the configured ClickHouse database for actual operations
	query := `
		SELECT COUNT(*) 
		FROM system.tables 
		WHERE database = ? AND name = ?
	`

	var count int
	err := c.db.QueryRowContext(ctx, query, c.cfg.Database, table).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return count > 0, nil
}

// GetTableEngine returns the engine type of a ClickHouse table.
// The database parameter represents the source MySQL database but operations are performed
// in the configured ClickHouse database (c.cfg.Database) for consistent data organization.
func (c *Client) GetTableEngine(ctx context.Context, database, table string) (TableEngine, error) {
	// Note: 'database' parameter is the source MySQL database name, but we always use
	// the configured ClickHouse database for actual operations
	query := `
		SELECT engine 
		FROM system.tables 
		WHERE database = ? AND name = ?
	`

	var engine string
	err := c.db.QueryRowContext(ctx, query, c.cfg.Database, table).Scan(&engine)
	if err != nil {
		return "", fmt.Errorf("failed to get table engine: %w", err)
	}

	return TableEngine(engine), nil
}

// Insert inserts events into ClickHouse table.
// The database parameter represents the source MySQL database but operations are performed
// in the configured ClickHouse database (c.cfg.Database) for consistent data organization.
func (c *Client) Insert(ctx context.Context, database, table string, events []*common.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Note: 'database' parameter is the source MySQL database name, but we always use
	// the configured ClickHouse database for actual operations to maintain data consistency
	tableInfo, err := c.getTableInfoFromCache(ctx, c.cfg.Database, table)
	if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	query, values, err := c.buildInsertQuery(tableInfo, events)
	if err != nil {
		return fmt.Errorf("failed to build insert query: %w", err)
	}

	c.logger.Debug("Executing insert",
		zap.String("source_database", database),
		zap.String("clickhouse_database", c.cfg.Database),
		zap.String("table", table),
		zap.Int("event_count", len(events)))

	_, err = c.db.ExecContext(ctx, query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert into %s.%s: %w", c.cfg.Database, table, err)
	}

	c.logger.Debug("Insert completed successfully",
		zap.String("source_database", database),
		zap.String("clickhouse_database", c.cfg.Database),
		zap.String("table", table),
		zap.Int("rows_inserted", len(events)))

	return nil
}

// UpdateBatch processes multiple update events in a single batch operation
func (c *Client) UpdateBatch(ctx context.Context, database, table string, events []*common.Event) error {
	// Only ReplacingMergeTree and ReplicatedReplacingMergeTree are supported
	// For ReplacingMergeTree, updates are just inserts with newer version timestamp
	// The ReplacingMergeTree engine will automatically handle deduplication based on _version
	return c.Insert(ctx, database, table, events)
}

// DeleteBatch processes multiple delete events in a single batch operation
func (c *Client) DeleteBatch(ctx context.Context, database, table string, events []*common.Event) error {
	// Only ReplacingMergeTree and ReplicatedReplacingMergeTree are supported
	// Use soft delete pattern: 1 indicates deleted record (ClickHouse ReplacingMergeTree convention)

	// Process all events to set _is_deleted flag
	for _, event := range events {
		// Initialize Data map from OldData if nil (DELETE events have data in OldData)
		if event.Data == nil {
			if event.OldData != nil {
				event.Data = event.OldData
			} else {
				event.Data = make(map[string]interface{})
			}
		}
		event.Data["_is_deleted"] = 1
	}

	// Insert all deletion markers in a single batch
	return c.Insert(ctx, database, table, events)
}

// getTableInfoFromCache retrieves table info from cache first, with fallback to database query
func (c *Client) getTableInfoFromCache(ctx context.Context, database, table string) (*common.TableInfo, error) {
	// Try to get from cache first
	if c.cacheManager != nil {
		if tableInfo, err := c.cacheManager.GetTableSchema(database, table); err == nil {
			c.logger.Debug("Table info retrieved from cache",
				zap.String("database", database),
				zap.String("table", table))
			return tableInfo, nil
		}
	}

	// Cache miss - query database and update cache
	c.logger.Debug("Cache miss, querying ClickHouse for table info",
		zap.String("database", database),
		zap.String("table", table))

	tableInfo, err := c.queryTableInfoFromDatabase(ctx, database, table)
	if err != nil {
		return nil, err
	}

	// Update cache for future use
	if c.cacheManager != nil {
		c.cacheManager.SetTableSchema(tableInfo)
		c.logger.Debug("Table info cached for future use",
			zap.String("database", database),
			zap.String("table", table))
	}

	return tableInfo, nil
}

// queryTableInfoFromDatabase queries ClickHouse system tables for table metadata
func (c *Client) queryTableInfoFromDatabase(ctx context.Context, database, table string) (*common.TableInfo, error) {
	query := `
		SELECT 
			name,
			type,
			is_in_primary_key,
			default_expression
		FROM system.columns 
		WHERE database = ? AND table = ?
		ORDER BY position
	`

	// Always use configured ClickHouse database, not the passed database parameter
	rows, err := c.db.QueryContext(ctx, query, c.cfg.Database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table info: %w", err)
	}
	defer rows.Close()

	tableInfo := &common.TableInfo{
		Database:    c.cfg.Database,
		Name:        table,
		Columns:     make(map[string]common.Column),
		ColumnOrder: make([]string, 0),
	}

	for rows.Next() {
		var name, colType, defaultExpr string
		var isPrimaryKey bool

		err := rows.Scan(&name, &colType, &isPrimaryKey, &defaultExpr)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		tableInfo.Columns[name] = common.Column{
			Name:         name,
			Type:         colType,
			IsPrimaryKey: isPrimaryKey,
			DefaultValue: defaultExpr,
			Nullable:     strings.Contains(strings.ToLower(colType), "nullable"),
		}
		tableInfo.ColumnOrder = append(tableInfo.ColumnOrder, name)
	}

	return tableInfo, nil
}

// QueryTableInfoFromDatabase queries ClickHouse system tables for current table metadata
// This bypasses the cache and queries the actual database state
func (c *Client) QueryTableInfoFromDatabase(ctx context.Context, database, table string) (*common.TableInfo, error) {
	return c.queryTableInfoFromDatabase(ctx, database, table)
}

func (c *Client) buildCreateTableQuery(table *common.TableInfo, engine TableEngine) string {
	// SECURITY: Validate database and table names
	if err := security.ValidateIdentifier(c.cfg.Database, "database name"); err != nil {
		c.logger.Error("Invalid database name in config", zap.Error(err))
		return ""
	}
	if err := security.ValidateIdentifier(table.Name, "table name"); err != nil {
		c.logger.Error("Invalid table name", zap.Error(err), zap.String("table", table.Name))
		return ""
	}

	var columns []string
	var primaryKeys []string

	// Check for existing special columns
	hasIsDeleted := false
	hasVersionColumn := false

	// Add existing columns from MySQL schema
	for _, col := range table.Columns {
		// SECURITY: Validate column names
		if err := security.ValidateIdentifier(col.Name, "column name"); err != nil {
			c.logger.Error("Invalid column name", zap.Error(err), zap.String("column", col.Name))
			continue // Skip invalid column names
		}

		escapedCol := security.EscapeIdentifier(col.Name)
		colDef := fmt.Sprintf("%s %s", escapedCol, c.mysqlToClickHouseType(col.Type))
		columns = append(columns, colDef)

		if col.IsPrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}

		if col.Name == "_is_deleted" {
			hasIsDeleted = true
		}
		if col.Name == "_version" {
			hasVersionColumn = true
		}
	}

	// Add required columns for ReplacingMergeTree engines (consistent with schema translator)
	if engine == EngineReplacingMergeTree || engine == EngineReplicatedReplacingMergeTree {
		// Add _is_deleted column if missing (for soft deletes)
		if !hasIsDeleted {
			columns = append(columns, "`_is_deleted` UInt8 DEFAULT 0")
		}

		// Add _version column if missing (for ReplacingMergeTree version tracking)
		if !hasVersionColumn {
			columns = append(columns, "`_version` UInt64 DEFAULT 0")
		}
	}

	escapedDB := security.EscapeIdentifier(c.cfg.Database)
	escapedTable := security.EscapeIdentifier(table.Name)
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n  %s\n)",
		escapedDB, escapedTable, strings.Join(columns, ",\n  "))

	// For ReplacingMergeTree engines, specify both version and sign columns (consistent with schema translator)
	engineClause := fmt.Sprintf("ENGINE = %s", engine)
	if engine == EngineReplacingMergeTree || engine == EngineReplicatedReplacingMergeTree {
		engineClause = fmt.Sprintf("ENGINE = %s(_version, _is_deleted)", engine)
	}

	if len(primaryKeys) > 0 {
		// SECURITY: Escape primary key column names in ORDER BY clause
		escapedPKs := make([]string, 0, len(primaryKeys))
		for _, pk := range primaryKeys {
			escapedPKs = append(escapedPKs, security.EscapeIdentifier(pk))
		}
		engineClause += fmt.Sprintf("\nORDER BY (%s)", strings.Join(escapedPKs, ", "))
	}

	query += fmt.Sprintf("\n%s", engineClause)

	return query
}

func (c *Client) buildInsertQuery(tableInfo *common.TableInfo, events []*common.Event) (string, []interface{}, error) {
	if len(events) == 0 {
		return "", nil, fmt.Errorf("no events to insert")
	}

	// SECURITY: Validate database and table names
	if err := security.ValidateIdentifier(c.cfg.Database, "database name"); err != nil {
		return "", nil, fmt.Errorf("invalid database name in config: %w", err)
	}
	if err := security.ValidateIdentifier(tableInfo.Name, "table name"); err != nil {
		return "", nil, fmt.Errorf("invalid table name: %w", err)
	}

	// Use consistent column ordering for both names and values
	columnNames := getColumnNames(tableInfo)
	columns := make([]string, 0, len(columnNames))
	for _, name := range columnNames {
		// SECURITY: Validate and escape column names
		if err := security.ValidateIdentifier(name, "column name"); err != nil {
			return "", nil, fmt.Errorf("invalid column name %q: %w", name, err)
		}
		columns = append(columns, security.EscapeIdentifier(name))
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	escapedDB := security.EscapeIdentifier(c.cfg.Database)
	escapedTable := security.EscapeIdentifier(tableInfo.Name)
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES ",
		escapedDB, escapedTable, strings.Join(columns, ", "))

	c.logger.Debug("INSERT column order",
		zap.String("table", tableInfo.Name),
		zap.Strings("columns", columnNames))

	var values []interface{}
	var rowPlaceholders []string

	for _, event := range events {
		rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))

		for _, colName := range columnNames {
			var value interface{}
			var exists bool

			// Special handling for ReplacingMergeTree version and soft delete columns
			if colName == "_version" && event.Data[colName] == nil {
				// Use MySQL event timestamp for version to preserve original transaction ordering
				value = event.Timestamp.UnixNano()
				exists = true
			} else if colName == "_is_deleted" && event.Data[colName] == nil {
				// Default _is_deleted to 0 for active records (ClickHouse ReplacingMergeTree convention: 0=active, 1=deleted)
				value = 0
				exists = true
			} else {
				value, exists = event.Data[colName]
			}

			if exists && value != nil {
				// Convert MySQL data value to ClickHouse compatible format
				convertedValue, err := c.convertMySQLValueToClickHouse(value, tableInfo.Columns[colName].Type)
				if err != nil {
					c.logger.Warn("Failed to convert value, using original",
						zap.String("column", colName),
						zap.String("type", tableInfo.Columns[colName].Type),
						zap.Error(err))
					values = append(values, value)
				} else {
					values = append(values, convertedValue)
				}
			} else {
				values = append(values, nil)
			}
		}
	}

	query += strings.Join(rowPlaceholders, ", ")
	return query, values, nil
}

func (c *Client) mysqlToClickHouseType(mysqlType string) string {
	mysqlType = strings.ToLower(mysqlType)

	switch {
	case strings.Contains(mysqlType, "tinyint(1)"):
		return "UInt8"
	case strings.Contains(mysqlType, "tinyint"):
		return "Int8"
	case strings.Contains(mysqlType, "smallint"):
		return "Int16"
	case strings.Contains(mysqlType, "mediumint"):
		return "Int32"
	case strings.Contains(mysqlType, "bigint"):
		return "Int64"
	case strings.Contains(mysqlType, "int"):
		return "Int32"
	case strings.Contains(mysqlType, "float"):
		return "Float32"
	case strings.Contains(mysqlType, "double"):
		return "Float64"
	case strings.Contains(mysqlType, "decimal"):
		return "Decimal64(4)"
	case strings.Contains(mysqlType, "varchar"), strings.Contains(mysqlType, "text"):
		return "String"
	case strings.Contains(mysqlType, "char"):
		return "FixedString(255)"
	case strings.Contains(mysqlType, "datetime"):
		return "DateTime64(3)"
	case strings.Contains(mysqlType, "timestamp"):
		return "DateTime64(3)"
	case strings.Contains(mysqlType, "date"):
		return "Date"
	case strings.Contains(mysqlType, "time"):
		return "String"
	case strings.Contains(mysqlType, "json"):
		return "String"
	case strings.Contains(mysqlType, "blob"), strings.Contains(mysqlType, "binary"):
		return "String"
	default:
		return "String"
	}
}

func getColumnNames(tableInfo *common.TableInfo) []string {
	// Use the column order from MySQL schema discovery to preserve original ordering
	if len(tableInfo.ColumnOrder) > 0 {
		return tableInfo.ColumnOrder
	}

	// Fallback to alphabetical sorting if ColumnOrder is not available
	columns := make([]string, 0, len(tableInfo.Columns))
	for name := range tableInfo.Columns {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	return columns
}

func (c *Client) ExecuteDDL(ctx context.Context, ddlStatement string) error {
	c.logger.Debug("Executing DDL statement", zap.String("statement", ddlStatement))

	// Add statement validation
	if strings.TrimSpace(ddlStatement) == "" {
		return fmt.Errorf("DDL statement cannot be empty")
	}

	// Log potentially destructive operations
	upperStmt := strings.ToUpper(strings.TrimSpace(ddlStatement))
	if strings.HasPrefix(upperStmt, "DROP ") || strings.HasPrefix(upperStmt, "TRUNCATE ") {
		c.logger.Warn("Executing potentially destructive DDL statement",
			zap.String("statement", ddlStatement))
	}

	// Execute with proper context timeout
	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := c.db.ExecContext(execCtx, ddlStatement)
	if err != nil {
		// Provide more context for common ClickHouse errors
		if strings.Contains(err.Error(), "already exists") {
			c.logger.Warn("DDL statement failed because object already exists",
				zap.String("statement", ddlStatement),
				zap.Error(err))
			return fmt.Errorf("DDL execution failed - object already exists: %w", err)
		}
		if strings.Contains(err.Error(), "doesn't exist") {
			c.logger.Warn("DDL statement failed because object doesn't exist",
				zap.String("statement", ddlStatement),
				zap.Error(err))
			return fmt.Errorf("DDL execution failed - object doesn't exist: %w", err)
		}
		if strings.Contains(err.Error(), "timeout") {
			c.logger.Error("DDL statement execution timeout",
				zap.String("statement", ddlStatement),
				zap.Error(err))
			return fmt.Errorf("DDL execution timeout after 30s: %w", err)
		}

		c.logger.Error("DDL statement execution failed",
			zap.String("statement", ddlStatement),
			zap.Error(err))
		return fmt.Errorf("failed to execute DDL statement: %w", err)
	}

	c.logger.Info("DDL statement executed successfully",
		zap.String("statement", ddlStatement))
	return nil
}

func (c *Client) ExecuteQuery(ctx context.Context, query string, args ...interface{}) error {
	c.logger.Debug("Executing query", zap.String("query", query))

	_, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	c.logger.Debug("Query executed successfully")
	return nil
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.db.QueryRowContext(ctx, query, args...)
}

// convertMySQLValueToClickHouse converts MySQL data values to ClickHouse compatible format
func (c *Client) convertMySQLValueToClickHouse(value interface{}, clickhouseType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// Handle Nullable types by extracting the inner type
	if strings.HasPrefix(clickhouseType, "Nullable(") && strings.HasSuffix(clickhouseType, ")") {
		innerType := clickhouseType[9 : len(clickhouseType)-1] // Remove "Nullable(" and ")"
		return c.convertMySQLValueToClickHouse(value, innerType)
	}

	// Convert based on ClickHouse type
	switch {
	case strings.HasPrefix(clickhouseType, "DateTime64"):
		return c.convertToDateTime(value)
	case clickhouseType == "Date":
		return c.convertToDate(value)
	case strings.HasPrefix(clickhouseType, "DateTime"):
		return c.convertToDateTime(value)
	case strings.Contains(clickhouseType, "String") || strings.HasPrefix(clickhouseType, "FixedString"):
		return c.convertToString(value)
	case strings.Contains(clickhouseType, "Int") || strings.Contains(clickhouseType, "UInt"):
		return value, nil // Keep numeric types as-is
	case strings.Contains(clickhouseType, "Float"):
		return value, nil // Keep float types as-is
	case strings.Contains(clickhouseType, "Decimal"):
		return c.convertToDecimal(value)
	default:
		// For unknown types, convert to string
		return c.convertToString(value)
	}
}

// convertToDateTime handles conversion to ClickHouse DateTime/DateTime64 format
func (c *Client) convertToDateTime(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case time.Time:
		// Convert Go time to ClickHouse format: "2006-01-02 15:04:05.000"
		return v.Format("2006-01-02 15:04:05.000"), nil
	case string:
		// Parse MySQL datetime string and reformat for ClickHouse
		if v == "" || v == "0000-00-00 00:00:00" {
			return "1970-01-01 00:00:00.000", nil
		}

		// Try parsing common MySQL datetime formats
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02 15:04:05.000",
			"2006-01-02T15:04:05",
			"2006-01-02T15:04:05.000",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t.Format("2006-01-02 15:04:05.000"), nil
			}
		}

		// If parsing fails, return the original string and let ClickHouse handle it
		return v, nil
	case []byte:
		// Convert byte array to string and retry
		return c.convertToDateTime(string(v))
	default:
		// For other types, convert to string
		return fmt.Sprintf("%v", value), nil
	}
}

// convertToDate handles conversion to ClickHouse Date format
func (c *Client) convertToDate(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case time.Time:
		return v.Format("2006-01-02"), nil
	case string:
		if v == "" || v == "0000-00-00" {
			return "1970-01-01", nil
		}

		// Try parsing common MySQL date formats
		formats := []string{
			"2006-01-02",
			"2006-01-02 15:04:05", // Extract date part from datetime
		}

		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t.Format("2006-01-02"), nil
			}
		}

		return v, nil
	case []byte:
		return c.convertToDate(string(v))
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

// convertToString handles conversion to ClickHouse String types
func (c *Client) convertToString(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case []byte:
		return string(v), nil
	case string:
		return v, nil
	default:
		return fmt.Sprintf("%v", value), nil
	}
}

// convertToDecimal handles conversion to ClickHouse Decimal types
// MySQL's go-mysql library returns DECIMAL columns as []byte containing the string representation
func (c *Client) convertToDecimal(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case []byte:
		// go-mysql returns DECIMAL as []byte containing the string representation
		return string(v), nil
	case string:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		// Numeric types can be passed through - ClickHouse will handle the conversion
		return v, nil
	default:
		// For other types, convert to string representation
		return fmt.Sprintf("%v", value), nil
	}
}
