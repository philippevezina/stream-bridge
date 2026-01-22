package snapshot

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/schema"
)

type Loader struct {
	chClient   *clickhouse.Client
	translator *schema.Translator
	logger     *zap.Logger
	batchSize  int
}

func NewLoader(chClient *clickhouse.Client, translator *schema.Translator, logger *zap.Logger, batchSize int) *Loader {
	return &Loader{
		chClient:   chClient,
		translator: translator,
		logger:     logger,
		batchSize:  batchSize,
	}
}

func (l *Loader) LoadChunk(ctx context.Context, chunk *ChunkInfo, snapshotTime time.Time) error {
	if len(chunk.Data) == 0 {
		return nil
	}

	// Split raw data into batches - transformation now handled by ClickHouse client
	batches := l.splitIntoBatches(chunk.Data)

	for i, batch := range batches {
		if err := l.insertBatch(ctx, chunk.Database, chunk.Table, batch, snapshotTime); err != nil {
			return fmt.Errorf("failed to insert batch %d: %w", i, err)
		}
	}

	l.logger.Debug("Chunk loaded successfully",
		zap.String("database", chunk.Database),
		zap.String("table", chunk.Table),
		zap.Int("chunk_index", chunk.ChunkIndex),
		zap.Int("rows", len(chunk.Data)))

	return nil
}

func (l *Loader) splitIntoBatches(data []map[string]interface{}) [][]map[string]interface{} {
	if len(data) <= l.batchSize {
		return [][]map[string]interface{}{data}
	}

	batches := make([][]map[string]interface{}, 0)
	for i := 0; i < len(data); i += l.batchSize {
		end := i + l.batchSize
		if end > len(data) {
			end = len(data)
		}
		batches = append(batches, data[i:end])
	}

	return batches
}

func (l *Loader) insertBatch(ctx context.Context, database, table string, batch []map[string]interface{}, snapshotTime time.Time) error {
	if len(batch) == 0 {
		return nil
	}

	// Convert batch data to Event format for ClickHouse client
	events := make([]*common.Event, 0, len(batch))
	for _, row := range batch {
		event := &common.Event{
			Type:      common.EventTypeInsert,
			Database:  database, // This is the MySQL database name (for logging/filtering)
			Table:     table,
			Data:      row,
			Timestamp: snapshotTime, // Use consistent snapshot timestamp for proper _version ordering
		}
		events = append(events, event)
	}

	// Use ClickHouse client Insert method (which now uses configured ClickHouse database)
	if err := l.chClient.Insert(ctx, database, table, events); err != nil {
		return fmt.Errorf("failed to insert batch using ClickHouse client: %w", err)
	}

	return nil
}

func (l *Loader) LoadTable(ctx context.Context, database, table string, chunkChan <-chan *ChunkInfo, snapshotTime time.Time) error {
	l.logger.Info("Starting table load",
		zap.String("database", database),
		zap.String("table", table))

	processedChunks := 0
	totalRows := int64(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-chunkChan:
			if !ok {
				l.logger.Info("Table load completed",
					zap.String("database", database),
					zap.String("table", table),
					zap.Int("processed_chunks", processedChunks),
					zap.Int64("total_rows", totalRows))
				return nil
			}

			if chunk.Database != database || chunk.Table != table {
				continue // Skip chunks for other tables
			}

			startTime := time.Now()
			if err := l.LoadChunk(ctx, chunk, snapshotTime); err != nil {
				return fmt.Errorf("failed to load chunk %d: %w", chunk.ChunkIndex, err)
			}

			processedChunks++
			totalRows += int64(len(chunk.Data))

			l.logger.Debug("Chunk loaded",
				zap.String("database", database),
				zap.String("table", table),
				zap.Int("chunk_index", chunk.ChunkIndex),
				zap.Int("chunk_rows", len(chunk.Data)),
				zap.Int64("total_rows", totalRows),
				zap.Duration("duration", time.Since(startTime)))
		}
	}
}
