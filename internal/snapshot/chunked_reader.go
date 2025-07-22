package snapshot

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/mysql/connector"
	"github.com/philippevezina/stream-bridge/internal/security"
)

type ChunkedReader struct {
	cfg       *config.MySQLConfig
	logger    *zap.Logger
	connector *connector.Connector
	chunkChan chan *ChunkInfo
	errorChan chan error
	stopChan  chan struct{}
	stopOnce  sync.Once
	mu        sync.Mutex
	stopped   bool
}

func NewChunkedReader(cfg *config.MySQLConfig, logger *zap.Logger) *ChunkedReader {
	return &ChunkedReader{
		cfg:       cfg,
		logger:    logger,
		connector: connector.New(cfg, logger),
		chunkChan: make(chan *ChunkInfo, 100),
		errorChan: make(chan error, 10),
		stopChan:  make(chan struct{}),
	}
}

func (cr *ChunkedReader) Start(ctx context.Context) error {
	// Test connection to ensure MySQL is reachable
	conn, err := cr.connector.Connect(cr.cfg.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	conn.Close()

	cr.logger.Info("Chunked reader started")
	return nil
}

func (cr *ChunkedReader) Stop() error {
	cr.mu.Lock()
	if cr.stopped {
		cr.mu.Unlock()
		return nil
	}
	cr.stopped = true
	cr.mu.Unlock()

	cr.stopOnce.Do(func() {
		if cr.stopChan != nil {
			close(cr.stopChan)
		}
		if cr.chunkChan != nil {
			close(cr.chunkChan)
		}
		if cr.errorChan != nil {
			close(cr.errorChan)
		}
		cr.logger.Info("Chunked reader stopped")
	})
	return nil
}

func (cr *ChunkedReader) GetTableRowCount(ctx context.Context, database, table string) (int64, error) {
	// SECURITY: Validate all identifiers before using in SQL queries
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return 0, fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return 0, fmt.Errorf("invalid table name: %w", err)
	}

	// Create a new connection for this operation
	conn, err := cr.connector.Connect(cr.cfg.Database)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer conn.Close()

	// Escape identifiers for safe SQL interpolation (MySQL uses backticks)
	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(table)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", escapedDB, escapedTable)

	result, err := conn.Execute(query)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count for %s.%s: %w", database, table, err)
	}

	if result.RowNumber() == 0 {
		return 0, nil
	}

	// Get the count as int64 - COUNT(*) always returns a numeric value
	count, err := result.GetInt(0, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to get count value: %w", err)
	}

	return count, nil
}

func (cr *ChunkedReader) ReadTableChunks(ctx context.Context, database, table string, chunkSize int, chunkChan chan<- *ChunkInfo) error {
	totalRows, err := cr.GetTableRowCount(ctx, database, table)
	if err != nil {
		return fmt.Errorf("failed to get table row count: %w", err)
	}

	if totalRows == 0 {
		cr.logger.Debug("Table is empty, skipping",
			zap.String("database", database),
			zap.String("table", table))
		return nil
	}

	cr.logger.Info("Starting chunked read",
		zap.String("database", database),
		zap.String("table", table),
		zap.Int64("total_rows", totalRows),
		zap.Int("chunk_size", chunkSize))

	primaryKey, err := cr.getPrimaryKey(database, table)
	if err != nil {
		cr.logger.Warn("No primary key found, using LIMIT/OFFSET",
			zap.String("database", database),
			zap.String("table", table))
		return cr.readChunksWithOffset(ctx, database, table, chunkSize, totalRows, chunkChan)
	}

	return cr.readChunksWithPrimaryKey(ctx, database, table, chunkSize, totalRows, primaryKey, chunkChan)
}

func (cr *ChunkedReader) readChunksWithPrimaryKey(ctx context.Context, database, table string, chunkSize int, totalRows int64, primaryKey string, chunkChan chan<- *ChunkInfo) error {
	// SECURITY: Validate all identifiers before using in SQL queries
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}
	if err := security.ValidateIdentifier(primaryKey, "primary key column name"); err != nil {
		return fmt.Errorf("invalid primary key column name: %w", err)
	}

	// Create a new connection for this operation
	conn, err := cr.connector.Connect(cr.cfg.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer conn.Close()

	var lastValue interface{}
	chunkIndex := 0
	processedRows := int64(0)

	// Escape identifiers for safe SQL interpolation (MySQL uses backticks)
	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(table)
	escapedPK := security.EscapeIdentifier(primaryKey)

	for processedRows < totalRows {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cr.stopChan:
			return fmt.Errorf("reader stopped")
		default:
		}

		// Build query with parameterized WHERE clause for lastValue
		var query string
		var queryArgs []interface{}

		if lastValue != nil {
			// Use parameterized query for the value comparison
			query = fmt.Sprintf("SELECT * FROM %s.%s WHERE %s > ? ORDER BY %s LIMIT %d",
				escapedDB, escapedTable, escapedPK, escapedPK, chunkSize)
			queryArgs = []interface{}{lastValue}
		} else {
			query = fmt.Sprintf("SELECT * FROM %s.%s ORDER BY %s LIMIT %d",
				escapedDB, escapedTable, escapedPK, chunkSize)
			queryArgs = []interface{}{}
		}

		result, err := conn.Execute(query, queryArgs...)
		if err != nil {
			return fmt.Errorf("failed to execute chunk query: %w", err)
		}

		if result.RowNumber() == 0 {
			break
		}

		chunk := &ChunkInfo{
			Database:    database,
			Table:       table,
			ChunkIndex:  chunkIndex,
			StartOffset: processedRows,
			EndOffset:   processedRows + int64(result.RowNumber()),
			OrderBy:     primaryKey,
			Data:        make([]map[string]interface{}, 0, result.RowNumber()),
		}

		// Convert result to map slice
		for i := 0; i < result.RowNumber(); i++ {
			row := make(map[string]interface{})
			for j := 0; j < result.ColumnNumber(); j++ {
				columnName := string(result.Fields[j].Name)
				value, err := result.GetValue(i, j)
				if err != nil {
					return fmt.Errorf("failed to get value at row %d, column %d: %w", i, j, err)
				}
				row[columnName] = value
			}
			chunk.Data = append(chunk.Data, row)

			// Update last value for next iteration
			if i == result.RowNumber()-1 {
				lastValue, _ = result.GetValue(i, cr.getPrimaryKeyColumnIndex(result, primaryKey))
			}
		}

		select {
		case chunkChan <- chunk:
		case <-ctx.Done():
			return ctx.Err()
		case <-cr.stopChan:
			return fmt.Errorf("reader stopped")
		}

		processedRows += int64(result.RowNumber())
		chunkIndex++

		cr.logger.Debug("Chunk processed",
			zap.String("database", database),
			zap.String("table", table),
			zap.Int("chunk_index", chunkIndex),
			zap.Int64("processed_rows", processedRows),
			zap.Int64("total_rows", totalRows))
	}

	return nil
}

func (cr *ChunkedReader) readChunksWithOffset(ctx context.Context, database, table string, chunkSize int, totalRows int64, chunkChan chan<- *ChunkInfo) error {
	// SECURITY: Validate all identifiers before using in SQL queries
	if err := security.ValidateIdentifier(database, "database name"); err != nil {
		return fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(table, "table name"); err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	// Create a new connection for this operation
	conn, err := cr.connector.Connect(cr.cfg.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer conn.Close()

	chunkIndex := 0
	processedRows := int64(0)

	// Escape identifiers for safe SQL interpolation (MySQL uses backticks)
	escapedDB := security.EscapeIdentifier(database)
	escapedTable := security.EscapeIdentifier(table)

	for processedRows < totalRows {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cr.stopChan:
			return fmt.Errorf("reader stopped")
		default:
		}

		query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d OFFSET %d",
			escapedDB, escapedTable, chunkSize, processedRows)

		result, err := conn.Execute(query)
		if err != nil {
			return fmt.Errorf("failed to execute chunk query: %w", err)
		}

		if result.RowNumber() == 0 {
			break
		}

		chunk := &ChunkInfo{
			Database:    database,
			Table:       table,
			ChunkIndex:  chunkIndex,
			StartOffset: processedRows,
			EndOffset:   processedRows + int64(result.RowNumber()),
			Data:        make([]map[string]interface{}, 0, result.RowNumber()),
		}

		// Convert result to map slice
		for i := 0; i < result.RowNumber(); i++ {
			row := make(map[string]interface{})
			for j := 0; j < result.ColumnNumber(); j++ {
				columnName := string(result.Fields[j].Name)
				value, err := result.GetValue(i, j)
				if err != nil {
					return fmt.Errorf("failed to get value at row %d, column %d: %w", i, j, err)
				}
				row[columnName] = value
			}
			chunk.Data = append(chunk.Data, row)
		}

		select {
		case chunkChan <- chunk:
		case <-ctx.Done():
			return ctx.Err()
		case <-cr.stopChan:
			return fmt.Errorf("reader stopped")
		}

		processedRows += int64(result.RowNumber())
		chunkIndex++

		cr.logger.Debug("Chunk processed",
			zap.String("database", database),
			zap.String("table", table),
			zap.Int("chunk_index", chunkIndex),
			zap.Int64("processed_rows", processedRows),
			zap.Int64("total_rows", totalRows))
	}

	return nil
}

func (cr *ChunkedReader) getPrimaryKey(database, table string) (string, error) {
	// Create a new connection for this operation
	conn, err := cr.connector.Connect(cr.cfg.Database)
	if err != nil {
		return "", fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer conn.Close()

	// SECURITY FIX: Use parameterized query to prevent SQL injection
	// MySQL supports '?' placeholders for value parameters in INFORMATION_SCHEMA queries
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME = ?
		AND COLUMN_KEY = 'PRI'
		ORDER BY ORDINAL_POSITION
		LIMIT 1
	`

	result, err := conn.Execute(query, database, table)
	if err != nil {
		return "", fmt.Errorf("failed to get primary key: %w", err)
	}

	if result.RowNumber() == 0 {
		return "", fmt.Errorf("no primary key found")
	}

	primaryKey, err := result.GetString(0, 0)
	if err != nil {
		return "", fmt.Errorf("failed to parse primary key: %w", err)
	}

	return primaryKey, nil
}

func (cr *ChunkedReader) getPrimaryKeyColumnIndex(result *mysql.Result, primaryKey string) int {
	for i, field := range result.Fields {
		if string(field.Name) == primaryKey {
			return i
		}
	}
	return 0
}

func (cr *ChunkedReader) ChunkChan() <-chan *ChunkInfo {
	return cr.chunkChan
}

func (cr *ChunkedReader) ErrorChan() <-chan error {
	return cr.errorChan
}
