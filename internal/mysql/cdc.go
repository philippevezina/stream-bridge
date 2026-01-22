package mysql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/mysql/connector"
	"github.com/philippevezina/stream-bridge/internal/state"
)

// Flusher interface allows CDC to flush pending events before critical checkpoints
type Flusher interface {
	FlushAndWaitForCompletion(ctx context.Context, timeout time.Duration) error
}

type CDC struct {
	cfg           *config.MySQLConfig
	logger        *zap.Logger
	connector     *connector.Connector
	syncer        *replication.BinlogSyncer
	streamer      *replication.BinlogStreamer
	position      mysql.Position
	eventChan     chan *common.Event
	errorChan     chan error
	running       bool
	mu            sync.RWMutex
	wg            sync.WaitGroup
	tableFilter   *common.TableFilter
	stateManager  *state.Manager
	flusher       Flusher // Pipeline flusher for ensuring event consistency
	eventCount    uint64  // Changed to atomic uint64
	lastEventTime time.Time

	// Add shutdown synchronization
	shutdownOnce sync.Once
	shutdown     chan struct{}
}

func NewCDC(cfg *config.MySQLConfig, stateManager *state.Manager, flusher Flusher, logger *zap.Logger) (*CDC, error) {
	tableFilter, err := common.NewTableFilter(cfg.TableFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to create table filter: %w", err)
	}

	cdc := &CDC{
		cfg:          cfg,
		logger:       logger,
		connector:    connector.New(cfg, logger),
		eventChan:    make(chan *common.Event, cfg.EventChannelBuffer),
		errorChan:    make(chan error, 100),
		tableFilter:  tableFilter,
		stateManager: stateManager,
		flusher:      flusher,
		shutdown:     make(chan struct{}),
	}

	return cdc, nil
}

func (c *CDC) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("CDC is already running")
	}

	if err := c.setupSyncer(ctx); err != nil {
		return fmt.Errorf("failed to setup syncer: %w", err)
	}

	streamer, err := c.syncer.StartSync(c.position)
	if err != nil {
		return fmt.Errorf("failed to start sync: %w", err)
	}
	c.streamer = streamer

	c.running = true

	c.wg.Add(1)
	go c.processEvents(ctx)

	c.logger.Info("MySQL CDC started",
		zap.String("position_file", c.position.Name),
		zap.Uint32("position_offset", c.position.Pos))

	return nil
}

func (c *CDC) Stop() error {
	c.mu.Lock()

	if !c.running {
		c.mu.Unlock()
		return nil
	}

	c.running = false

	// Signal shutdown to all goroutines
	c.shutdownOnce.Do(func() {
		close(c.shutdown)
	})

	// Close syncer first to stop binlog streaming
	if c.syncer != nil {
		c.syncer.Close()
	}

	c.mu.Unlock()

	// Wait for goroutines to finish
	c.logger.Info("Waiting for MySQL CDC goroutines to stop...")
	c.wg.Wait()

	// Channels are closed by the processEvents goroutine (last writer)
	c.logger.Info("MySQL CDC stopped")
	return nil
}

func (c *CDC) EventChan() <-chan *common.Event {
	return c.eventChan
}

func (c *CDC) ErrorChan() <-chan error {
	return c.errorChan
}

func (c *CDC) GetPosition() common.BinlogPosition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return common.BinlogPosition{
		File:   c.position.Name,
		Offset: c.position.Pos,
	}
}

func (c *CDC) SetPosition(pos common.BinlogPosition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.position = mysql.Position{
		Name: pos.File,
		Pos:  pos.Offset,
	}
}

func (c *CDC) IsRunning() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.running
}

func (c *CDC) setupSyncer(ctx context.Context) error {
	// Add context checking at the beginning
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before syncer setup: %w", ctx.Err())
	default:
	}

	syncerCfg := replication.BinlogSyncerConfig{
		ServerID: c.cfg.ServerID,
		Flavor:   c.cfg.Flavor,
		Host:     c.cfg.Host,
		Port:     uint16(c.cfg.Port),
		User:     c.cfg.Username,
		Password: c.cfg.Password,
	}

	// Configure SSL/TLS based on ssl_mode using connector
	if c.cfg.SSLMode != config.SSLModeDisabled {
		tlsConfig, err := c.connector.GetTLSConfig()
		if err != nil {
			if c.cfg.SSLMode == config.SSLModePreferred {
				// For preferred mode, log warning but continue without SSL
				c.logger.Warn("Failed to build TLS config for preferred mode, falling back to plaintext", zap.Error(err))
			} else {
				// For required, verify_ca, and verify_identity modes, fail hard
				return fmt.Errorf("failed to build TLS config for %s mode: %w", c.cfg.SSLMode, err)
			}
		} else if tlsConfig != nil {
			syncerCfg.TLSConfig = tlsConfig
			c.logger.Info("SSL/TLS enabled for MySQL binlog syncer", zap.String("mode", c.cfg.SSLMode))
		}
	}

	c.syncer = replication.NewBinlogSyncer(syncerCfg)

	if c.position.Name == "" {
		// Note: caller (Start or recoverConnection) holds c.mu
		if err := c.loadFromCheckpointLocked(ctx); err != nil {
			c.logger.Warn("Failed to load from state storage, starting from current position", zap.Error(err))

			// Pass context to getCurrentPosition
			pos, err := c.getCurrentPosition(ctx)
			if err != nil {
				return fmt.Errorf("failed to get current position: %w", err)
			}
			c.position = pos
		}
	}

	return nil
}

func (c *CDC) getCurrentPosition(ctx context.Context) (mysql.Position, error) {
	// Check context before proceeding
	select {
	case <-ctx.Done():
		return mysql.Position{}, fmt.Errorf("context cancelled during get current position: %w", ctx.Err())
	default:
	}

	conn, err := c.connector.Connect("")
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer conn.Close()

	result, err := conn.Execute("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to show master status: %w", err)
	}

	if result.RowNumber() == 0 {
		return mysql.Position{}, fmt.Errorf("no master status available")
	}

	file, _ := result.GetString(0, 0)
	pos, _ := result.GetUint(0, 1)

	return mysql.Position{Name: file, Pos: uint32(pos)}, nil
}

// loadFromCheckpointLocked loads position and lastEventTime from checkpoint storage.
// REQUIRES: c.mu must be held by the caller.
func (c *CDC) loadFromCheckpointLocked(ctx context.Context) error {
	if c.stateManager == nil {
		return fmt.Errorf("state manager not available")
	}

	// Check context before proceeding
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled during checkpoint load: %w", ctx.Err())
	default:
	}

	lastPosition := c.stateManager.GetLastPosition()
	if lastPosition == nil {
		return fmt.Errorf("no checkpoint found")
	}

	c.position = mysql.Position{
		Name: lastPosition.File,
		Pos:  lastPosition.Offset,
	}

	// Restore lastEventTime from checkpoint to prevent Unix epoch issue
	checkpoint := c.stateManager.GetCurrentCheckpoint()
	if checkpoint != nil {
		c.lastEventTime = checkpoint.LastBinlogTimestamp
	}

	c.logger.Info("Loaded position from checkpoint",
		zap.String("file", lastPosition.File),
		zap.Uint32("offset", lastPosition.Offset),
		zap.String("gtid", lastPosition.GTID))

	return nil
}

func (c *CDC) processEvents(ctx context.Context) {
	defer c.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("Panic in event processing", zap.Any("panic", r))
			c.safeErrorSend(fmt.Errorf("panic in event processing: %v", r))
		}
	}()

	// Close channels when this goroutine exits (last writer)
	defer func() {
		c.mu.Lock()
		if c.eventChan != nil {
			close(c.eventChan)
			c.eventChan = nil
		}
		if c.errorChan != nil {
			close(c.errorChan)
			c.errorChan = nil
		}
		c.mu.Unlock()
	}()

	for {
		// Get next event - this will block until an event is available or context is cancelled
		ev, err := c.streamer.GetEvent(ctx)

		// Immediately check for shutdown signals after GetEvent returns
		// This ensures responsive shutdown without waiting for next loop iteration
		select {
		case <-ctx.Done():
			c.logger.Info("Context cancelled, stopping event processing")
			return
		case <-c.shutdown:
			c.logger.Info("Shutdown signal received, stopping event processing")
			return
		default:
			// Continue to error/event handling
		}

		if err != nil {
			// Check if error is due to context cancellation during shutdown
			if ctx.Err() != nil {
				c.logger.Debug("Event retrieval stopped due to context cancellation")
				return
			}

			c.logger.Error("Failed to get event", zap.Error(err))

			// Check if this is a sync error that requires reconnection
			if c.isSyncError(err) {
				c.logger.Warn("Binlog sync error detected, attempting to recover connection")
				if recoveryErr := c.recoverConnection(ctx); recoveryErr != nil {
					c.logger.Error("Failed to recover binlog connection", zap.Error(recoveryErr))
					c.safeErrorSend(fmt.Errorf("connection recovery failed: %w", recoveryErr))
					// Wait before retrying to avoid rapid retry loops
					time.Sleep(5 * time.Second)
				} else {
					c.logger.Info("Successfully recovered binlog connection")
				}
			} else {
				c.safeErrorSend(err)
			}
			continue
		}

		// Handle rotation events specially
		if rotateEvent, ok := ev.Event.(*replication.RotateEvent); ok {
			c.handleRotateEvent(ctx, rotateEvent)
			continue
		}

		c.updatePosition(ev.Header)

		events := c.convertEvent(ev)
		if events != nil && len(events) > 0 {
			// Process all events from this binlog event
			eventsToSend := make([]*common.Event, 0)
			var lastTimestamp time.Time

			for _, event := range events {
				if event.Type == common.EventTypeDDL || c.shouldProcessTable(event.Database, event.Table) {
					eventsToSend = append(eventsToSend, event)
					lastTimestamp = event.Timestamp
				}
			}

			// If we have events to send, update counters and send them
			if len(eventsToSend) > 0 {
				// Use atomic increment for event count (no mutex needed)
				atomic.AddUint64(&c.eventCount, uint64(len(eventsToSend)))

				c.mu.Lock()
				c.lastEventTime = lastTimestamp
				c.mu.Unlock()

				if c.stateManager != nil {
					c.stateManager.IncrementEventCount()
				}

				// Send all events from this binlog event
				for _, event := range eventsToSend {
					if !c.safeSendEvent(event) {
						// Event not sent due to shutdown or timeout
						return
					}
				}

				c.tryCreateCheckpoint(ctx)
			}
		}
	}
}

func (c *CDC) updatePosition(header *replication.EventHeader) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if header.LogPos > 0 {
		c.position.Pos = header.LogPos
	}
}

func (c *CDC) handleRotateEvent(ctx context.Context, e *replication.RotateEvent) {
	c.mu.Lock()
	oldFile := c.position.Name

	// Update position to new binlog file
	c.position.Name = string(e.NextLogName)
	c.position.Pos = uint32(e.Position)

	newFile := c.position.Name
	c.mu.Unlock()

	c.logger.Info("Binlog rotation detected",
		zap.String("old_file", oldFile),
		zap.String("new_file", newFile),
		zap.Uint32("position", c.position.Pos))

	// Force checkpoint creation after rotation to persist new position
	if c.stateManager != nil {
		// Flush all pending events before creating checkpoint to ensure consistency
		if c.flusher != nil {
			c.logger.Info("Flushing pending events before rotation checkpoint")
			flushTimeout := 30 * time.Second
			if err := c.flusher.FlushAndWaitForCompletion(ctx, flushTimeout); err != nil {
				c.logger.Error("Failed to flush events before rotation checkpoint",
					zap.Error(err),
					zap.Duration("timeout", flushTimeout))
				c.safeErrorSend(fmt.Errorf("rotation checkpoint failed: %w", err))
				return
			}
			c.logger.Info("Successfully flushed events before rotation checkpoint")
		}

		c.mu.RLock()
		position := common.BinlogPosition{
			File:   c.position.Name,
			Offset: c.position.Pos,
		}
		lastEventTime := c.lastEventTime
		c.mu.RUnlock()

		err := c.stateManager.CreateCheckpoint(ctx, position, lastEventTime)
		if err != nil {
			c.logger.Error("Failed to create rotation checkpoint", zap.Error(err))
			c.safeErrorSend(err)
		} else {
			c.logger.Info("Created rotation checkpoint",
				zap.String("new_position", fmt.Sprintf("%s:%d", position.File, position.Offset)))
		}
	}
}

func (c *CDC) tryCreateCheckpoint(ctx context.Context) {
	if c.stateManager == nil {
		return
	}

	if !c.stateManager.ShouldCreateCheckpoint() {
		return
	}

	// Flush all pending events before creating checkpoint to ensure data consistency.
	// This guarantees that checkpoints are only saved AFTER data is written to ClickHouse,
	// preventing data loss on crash/restart.
	if c.flusher != nil {
		flushTimeout := 30 * time.Second
		if err := c.flusher.FlushAndWaitForCompletion(ctx, flushTimeout); err != nil {
			c.logger.Warn("Skipping checkpoint creation: failed to flush pending events",
				zap.Error(err),
				zap.Duration("timeout", flushTimeout))
			return
		}
	}

	c.mu.RLock()
	position := common.BinlogPosition{
		File:   c.position.Name,
		Offset: c.position.Pos,
	}
	lastEventTime := c.lastEventTime
	c.mu.RUnlock()

	err := c.stateManager.CreateCheckpoint(ctx, position, lastEventTime)
	if err != nil {
		c.logger.Error("Failed to create checkpoint", zap.Error(err))
		c.safeErrorSend(err)
		return
	}

	c.logger.Debug("Checkpoint created",
		zap.String("position", fmt.Sprintf("%s:%d", position.File, position.Offset)))
}

// safeSendEvent safely sends an event to the eventChan, checking shutdown state
// and ensuring the channel is not closed before sending to prevent panic.
// This function blocks until the event is sent or shutdown occurs, applying
// backpressure to the binlog reader rather than dropping data.
func (c *CDC) safeSendEvent(event *common.Event) bool {
	// Capture channel reference under lock to prevent race with channel closure
	c.mu.RLock()
	if c.eventChan == nil {
		c.mu.RUnlock()
		return false
	}
	ch := c.eventChan
	c.mu.RUnlock()

	select {
	case <-c.shutdown:
		return false
	case ch <- event:
		return true
	}
}

// safeErrorSend safely sends an error to the errorChan, checking shutdown state
// and ensuring the channel is not closed before sending to prevent panic.
func (c *CDC) safeErrorSend(err error) {
	// Capture channel reference under lock to prevent race with channel closure
	c.mu.RLock()
	if c.errorChan == nil {
		c.mu.RUnlock()
		c.logger.Debug("Error channel closed, logging error instead", zap.Error(err))
		return
	}
	ch := c.errorChan
	c.mu.RUnlock()

	select {
	case <-c.shutdown:
		// Shutting down, just log
		c.logger.Debug("Error during shutdown", zap.Error(err))
	case ch <- err:
		// Successfully sent
	default:
		// Channel full, log it
		c.logger.Error("Error channel full, dropping error", zap.Error(err))
	}
}

func (c *CDC) convertEvent(ev *replication.BinlogEvent) []*common.Event {
	switch e := ev.Event.(type) {
	case *replication.RowsEvent:
		return c.convertRowsEvent(ev.Header, e)
	case *replication.QueryEvent:
		queryEvent := c.convertQueryEvent(ev.Header, e)
		if queryEvent != nil {
			return []*common.Event{queryEvent}
		}
		return nil
	default:
		return nil
	}
}

func (c *CDC) convertRowsEvent(header *replication.EventHeader, e *replication.RowsEvent) []*common.Event {
	var eventType common.EventType
	var events []*common.Event

	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		eventType = common.EventTypeInsert
		if len(e.Rows) == 0 {
			return nil
		}

		database := string(e.Table.Schema)
		table := string(e.Table.Table)

		// Process all rows in the INSERT event
		for i, row := range e.Rows {
			event, err := c.createSingleRowEvent(header, e, eventType, row, nil)
			if err != nil {
				c.logger.Error("CRITICAL: Cannot create INSERT event due to missing binlog metadata - STOPPING CDC",
					zap.String("database", database),
					zap.String("table", table),
					zap.Int("row_index", i),
					zap.String("position", fmt.Sprintf("%s:%d", c.position.Name, c.position.Pos)),
					zap.Error(err))
				// Send fatal error to signal CDC should stop - do not skip rows as this causes silent data loss
				c.safeErrorSend(fmt.Errorf("FATAL: missing binlog metadata for INSERT in %s.%s row %d: %w", database, table, i, err))
				// Return nil to stop processing - the error channel will signal shutdown
				return nil
			}
			events = append(events, event)
		}

		return events

	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		eventType = common.EventTypeUpdate
		if len(e.Rows) < 2 {
			database := string(e.Table.Schema)
			table := string(e.Table.Table)
			c.logger.Warn("UPDATE event with insufficient rows",
				zap.String("database", database),
				zap.String("table", table),
				zap.Int("row_count", len(e.Rows)))
			return nil
		}

		database := string(e.Table.Schema)
		table := string(e.Table.Table)

		// UPDATE events contain pairs of rows: [before, after, before, after, ...]
		// Process all row pairs
		for i := 0; i < len(e.Rows); i += 2 {
			if i+1 >= len(e.Rows) {
				c.logger.Warn("UPDATE event has incomplete row pair",
					zap.String("database", database),
					zap.String("table", table),
					zap.Int("row_index", i),
					zap.Int("total_rows", len(e.Rows)))
				break
			}

			event, err := c.createSingleRowEvent(header, e, eventType, e.Rows[i+1], e.Rows[i])
			if err != nil {
				c.logger.Error("CRITICAL: Cannot create UPDATE event due to missing binlog metadata - STOPPING CDC",
					zap.String("database", database),
					zap.String("table", table),
					zap.Int("pair_index", i/2),
					zap.String("position", fmt.Sprintf("%s:%d", c.position.Name, c.position.Pos)),
					zap.Error(err))
				// Send fatal error to signal CDC should stop - do not skip rows as this causes silent data loss
				c.safeErrorSend(fmt.Errorf("FATAL: missing binlog metadata for UPDATE in %s.%s pair %d: %w", database, table, i/2, err))
				// Return nil to stop processing - the error channel will signal shutdown
				return nil
			}
			events = append(events, event)
		}

		return events

	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		eventType = common.EventTypeDelete
		if len(e.Rows) == 0 {
			return nil
		}

		database := string(e.Table.Schema)
		table := string(e.Table.Table)

		// Process all rows in the DELETE event
		for i, row := range e.Rows {
			event, err := c.createSingleRowEvent(header, e, eventType, nil, row)
			if err != nil {
				c.logger.Error("CRITICAL: Cannot create DELETE event due to missing binlog metadata - STOPPING CDC",
					zap.String("database", database),
					zap.String("table", table),
					zap.Int("row_index", i),
					zap.String("position", fmt.Sprintf("%s:%d", c.position.Name, c.position.Pos)),
					zap.Error(err))
				// Send fatal error to signal CDC should stop - do not skip rows as this causes silent data loss
				c.safeErrorSend(fmt.Errorf("FATAL: missing binlog metadata for DELETE in %s.%s row %d: %w", database, table, i, err))
				// Return nil to stop processing - the error channel will signal shutdown
				return nil
			}
			events = append(events, event)
		}

		return events

	default:
		return nil
	}
}

func (c *CDC) createSingleRowEvent(header *replication.EventHeader, e *replication.RowsEvent, eventType common.EventType, data []interface{}, oldData []interface{}) (*common.Event, error) {
	var dataMap, oldDataMap map[string]interface{}
	var err error

	if data != nil {
		dataMap, err = c.rowToMap(data, e.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to map data row: %w", err)
		}
	}
	if oldData != nil {
		oldDataMap, err = c.rowToMap(oldData, e.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to map old data row: %w", err)
		}
	}

	return &common.Event{
		ID:        uuid.New().String(),
		Type:      eventType,
		Database:  string(e.Table.Schema),
		Table:     string(e.Table.Table),
		Timestamp: time.Unix(int64(header.Timestamp), 0),
		Position:  c.GetPosition(),
		Data:      dataMap,
		OldData:   oldDataMap,
	}, nil
}

func (c *CDC) convertQueryEvent(header *replication.EventHeader, e *replication.QueryEvent) *common.Event {
	// Skip transaction control statements without logging
	if c.isTransactionControlStatement(string(e.Query)) {
		return nil
	}

	return &common.Event{
		ID:        uuid.New().String(),
		Type:      common.EventTypeDDL,
		Database:  string(e.Schema),
		Timestamp: time.Unix(int64(header.Timestamp), 0),
		Position:  c.GetPosition(),
		SQL:       string(e.Query),
	}
}

func (c *CDC) rowToMap(row []interface{}, tableMap *replication.TableMapEvent) (map[string]interface{}, error) {
	// Validate input parameters
	if row == nil {
		return nil, fmt.Errorf("row data is nil")
	}

	if tableMap == nil {
		return nil, fmt.Errorf("TableMapEvent is nil - binlog metadata required")
	}

	// Safely access table metadata with additional nil checks
	var database, table string
	if tableMap.Schema != nil {
		database = string(tableMap.Schema)
	}
	if tableMap.Table != nil {
		table = string(tableMap.Table)
	}

	columnNames := tableMap.ColumnNameString()
	if len(columnNames) == 0 {
		return nil, fmt.Errorf("no column names available in binlog metadata for table %s.%s", database, table)
	}

	result := make(map[string]interface{})

	// Map row values to actual column names from binlog metadata
	for i, value := range row {
		if i < len(columnNames) {
			result[columnNames[i]] = value
		} else {
			// Row has more columns than binlog metadata - this is a data consistency issue
			return nil, fmt.Errorf("row has more columns (%d) than binlog metadata (%d) for table %s.%s",
				len(row), len(columnNames), database, table)
		}
	}

	return result, nil
}

func (c *CDC) shouldProcessTable(database, table string) bool {
	return c.tableFilter.ShouldProcessTable(database, table)
}

// isTransactionControlStatement checks if the SQL statement is a transaction control statement
// that should be silently skipped (BEGIN, COMMIT, ROLLBACK, SAVEPOINT)
func (c *CDC) isTransactionControlStatement(sql string) bool {
	trimmed := strings.TrimSpace(strings.ToUpper(sql))
	return trimmed == "BEGIN" || trimmed == "COMMIT" || trimmed == "ROLLBACK" ||
		strings.HasPrefix(trimmed, "SAVEPOINT ")
}

// isSyncError checks if the error indicates a binlog sync issue that requires reconnection
func (c *CDC) isSyncError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "last sync error or closed") ||
		strings.Contains(errStr, "sync error") ||
		strings.Contains(errStr, "connection lost") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "invalid stream header") ||
		strings.Contains(errStr, "unexpected eof")
}

// recoverConnection attempts to recover the binlog connection by recreating the syncer and streamer
func (c *CDC) recoverConnection(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Close existing syncer if it exists
	if c.syncer != nil {
		c.syncer.Close()
		c.syncer = nil
	}
	c.streamer = nil

	// Track if we need to fall back to current position
	needsRecoveryCheckpoint := false

	// Validate current position before recovery
	if err := c.validatePosition(); err != nil {
		c.logger.Warn("Current position is invalid, falling back to current MySQL position", zap.Error(err))
		pos, err := c.getCurrentPosition(ctx)
		if err != nil {
			return fmt.Errorf("failed to get current position during recovery: %w", err)
		}
		c.position = pos
		needsRecoveryCheckpoint = true
	}

	// Recreate syncer with validated position
	if err := c.setupSyncer(ctx); err != nil {
		return fmt.Errorf("failed to setup syncer during recovery: %w", err)
	}

	// Start new streamer
	streamer, err := c.syncer.StartSync(c.position)
	if err != nil {
		return fmt.Errorf("failed to start sync during recovery: %w", err)
	}
	c.streamer = streamer

	c.logger.Info("Binlog connection recovered successfully",
		zap.String("position_file", c.position.Name),
		zap.Uint32("position_offset", c.position.Pos))

	// If we had to fall back to current position, create a recovery checkpoint
	// to prevent the same issue on next restart
	if needsRecoveryCheckpoint && c.stateManager != nil {
		if err := c.stateManager.CreateCheckpoint(ctx, common.BinlogPosition{
			File:   c.position.Name,
			Offset: c.position.Pos,
		}, time.Now()); err != nil {
			c.logger.Warn("Failed to create recovery checkpoint", zap.Error(err))
		} else {
			c.logger.Info("Created recovery checkpoint after position fallback")
		}
	}

	return nil
}

// validatePosition checks if the current binlog position is still valid in MySQL
func (c *CDC) validatePosition() error {
	if c.position.Name == "" {
		return fmt.Errorf("position file is empty")
	}

	conn, err := c.connector.Connect("")
	if err != nil {
		return fmt.Errorf("failed to connect to MySQL for position validation: %w", err)
	}
	defer conn.Close()

	// Check if the binlog file exists
	result, err := conn.Execute("SHOW BINARY LOGS")
	if err != nil {
		return fmt.Errorf("failed to show binary logs: %w", err)
	}

	binlogExists := false
	for i := 0; i < result.RowNumber(); i++ {
		logName, _ := result.GetString(i, 0)
		if logName == c.position.Name {
			binlogExists = true
			break
		}
	}

	if !binlogExists {
		return fmt.Errorf("binlog file %s no longer exists on server", c.position.Name)
	}

	c.logger.Debug("Position validation successful",
		zap.String("file", c.position.Name),
		zap.Uint32("offset", c.position.Pos))

	return nil
}
