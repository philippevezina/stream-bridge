package snapshot

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/schema"
	"github.com/philippevezina/stream-bridge/internal/state"
)

type Manager struct {
	cfg             *config.SnapshotConfig
	mysqlCfg        *config.MySQLConfig
	chCfg           *config.ClickHouseConfig
	logger          *zap.Logger
	stateManager    *state.Manager
	schemaDiscovery *schema.Discovery
	chClient        *clickhouse.Client
	translator      *schema.Translator
	coordinator     *Coordinator
	reader          *ChunkedReader
	loader          *Loader
	progress        *state.SnapshotProgress
	tableFilter     *common.TableFilter
	mu              sync.RWMutex
	running         bool
	stopChan        chan struct{}
}

func NewManager(
	cfg *config.SnapshotConfig,
	mysqlCfg *config.MySQLConfig,
	chCfg *config.ClickHouseConfig,
	stateManager *state.Manager,
	schemaDiscovery *schema.Discovery,
	chClient *clickhouse.Client,
	translator *schema.Translator,
	logger *zap.Logger,
) (*Manager, error) {
	tableFilter, err := common.NewTableFilter(mysqlCfg.TableFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to create table filter: %w", err)
	}

	return &Manager{
		cfg:             cfg,
		mysqlCfg:        mysqlCfg,
		chCfg:           chCfg,
		logger:          logger,
		stateManager:    stateManager,
		schemaDiscovery: schemaDiscovery,
		chClient:        chClient,
		translator:      translator,
		tableFilter:     tableFilter,
		stopChan:        make(chan struct{}),
	}, nil
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("snapshot manager is already running")
	}

	if !m.cfg.Enabled {
		m.logger.Info("Snapshot is disabled, skipping")
		return nil
	}

	m.logger.Info("Starting snapshot manager")

	// Initialize components
	if err := m.initializeComponents(ctx); err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}

	// Check if we need to resume an existing snapshot
	if m.cfg.ResumeOnFailure {
		m.logger.Info("Checking for resumable snapshots")
		if err := m.checkForExistingSnapshot(ctx); err != nil {
			return fmt.Errorf("failed to check for existing snapshot: %w", err)
		}
	}

	// If no existing snapshot, create a new one
	if m.progress == nil {
		m.logger.Info("Creating new snapshot")
		if err := m.createNewSnapshot(ctx); err != nil {
			return fmt.Errorf("failed to create new snapshot: %w", err)
		}
	}

	m.running = true
	return nil
}

func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	m.logger.Info("Stopping snapshot manager")
	close(m.stopChan)

	// Stop components
	if m.coordinator != nil {
		m.coordinator.Stop()
	}

	m.running = false
	return nil
}

func (m *Manager) ExecuteSnapshot(ctx context.Context) error {
	if !m.cfg.Enabled {
		return nil
	}

	// Count tables with data loading enabled for progress tracking
	dataLoadTables := 0
	for _, tableSnapshot := range m.progress.Tables {
		if tableSnapshot.DataLoadEnabled {
			dataLoadTables++
		}
	}

	m.logger.Info("Executing snapshot",
		zap.String("snapshot_id", m.progress.ID),
		zap.Int("total_tables", m.progress.TotalTables),
		zap.Int("data_load_tables", dataLoadTables),
		zap.Int("schema_only_tables", m.progress.TotalTables-dataLoadTables))

	// Update progress status
	m.progress.Status = "IN_PROGRESS"
	m.progress.StartTime = time.Now()

	if err := m.persistProgress(ctx); err != nil {
		return fmt.Errorf("failed to persist progress: %w", err)
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()

	// Execute snapshot with concurrency control
	if err := m.executeWithConcurrency(timeoutCtx); err != nil {
		m.progress.Status = "FAILED"
		m.progress.Error = err.Error()
		endTime := time.Now()
		m.progress.EndTime = &endTime

		if persistErr := m.persistProgress(ctx); persistErr != nil {
			m.logger.Error("Failed to persist failed snapshot progress", zap.Error(persistErr))
		}

		return fmt.Errorf("snapshot execution failed: %w", err)
	}

	// Count final results
	completedTables := 0
	schemaOnlyTables := 0
	failedTables := 0

	for _, tableSnapshot := range m.progress.Tables {
		switch tableSnapshot.Status {
		case "COMPLETED":
			completedTables++
		case "SCHEMA_ONLY":
			schemaOnlyTables++
		case "FAILED":
			failedTables++
		}
	}

	// Mark as completed
	m.progress.Status = "COMPLETED"
	m.progress.CompletedTables = completedTables
	m.progress.FailedTables = failedTables
	endTime := time.Now()
	m.progress.EndTime = &endTime

	if err := m.persistProgress(ctx); err != nil {
		return fmt.Errorf("failed to persist final progress: %w", err)
	}

	m.logger.Info("Snapshot completed successfully",
		zap.String("snapshot_id", m.progress.ID),
		zap.Duration("duration", endTime.Sub(m.progress.StartTime)),
		zap.Int("data_loaded_tables", completedTables),
		zap.Int("schema_only_tables", schemaOnlyTables),
		zap.Int("failed_tables", failedTables))

	return nil
}

func (m *Manager) GetProgress() *state.SnapshotProgress {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.progress == nil {
		return nil
	}

	// Return a copy to avoid race conditions
	progressCopy := *m.progress
	progressCopy.Tables = make(map[string]*state.TableSnapshot)
	for k, v := range m.progress.Tables {
		// Lock the table snapshot and copy fields individually (not the mutex)
		v.Mu.RLock()
		tableCopy := &state.TableSnapshot{
			Database:        v.Database,
			Table:           v.Table,
			Status:          v.Status,
			TotalRows:       v.TotalRows,
			ProcessedRows:   v.ProcessedRows,
			ChunkSize:       v.ChunkSize,
			DataLoadEnabled: v.DataLoadEnabled,
			StartTime:       v.StartTime,
			EndTime:         v.EndTime,
			Error:           v.Error,
			Position:        v.Position,
		}
		v.Mu.RUnlock()
		progressCopy.Tables[k] = tableCopy
	}

	return &progressCopy
}

func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

func (m *Manager) initializeComponents(ctx context.Context) error {
	// Initialize chunked reader
	m.reader = NewChunkedReader(m.mysqlCfg, m.logger.With(zap.String("component", "chunked-reader")))

	// Initialize loader
	m.loader = NewLoader(m.chClient, m.translator, m.logger.With(zap.String("component", "loader")), m.cfg.ChunkSize)

	// Initialize coordinator
	m.coordinator = NewCoordinator(m.mysqlCfg, m.cfg, m.logger.With(zap.String("component", "coordinator")))

	// Start coordinator to perform server detection and lock strategy determination
	if err := m.coordinator.Start(ctx); err != nil {
		return fmt.Errorf("failed to start coordinator: %w", err)
	}

	return nil
}

func (m *Manager) checkForExistingSnapshot(ctx context.Context) error {
	m.logger.Info("Checking for existing snapshot to resume")

	// Get the latest snapshot progress from state storage
	latestProgress, err := m.stateManager.GetLatestSnapshotProgress(ctx)
	if err != nil {
		return fmt.Errorf("failed to get latest snapshot progress: %w", err)
	}

	// If no snapshot found, we'll create a new one
	if latestProgress == nil {
		m.logger.Info("No existing snapshot found")
		return nil
	}

	// Check if the snapshot is resumable (PENDING or IN_PROGRESS)
	if latestProgress.Status != "PENDING" && latestProgress.Status != "IN_PROGRESS" {
		m.logger.Info("Latest snapshot is not resumable",
			zap.String("snapshot_id", latestProgress.ID),
			zap.String("status", latestProgress.Status))
		return nil
	}

	// Validate that the consistent position is still valid
	// For now, we'll assume it's valid if the snapshot is recent
	if time.Since(latestProgress.CreatedAt) > 24*time.Hour {
		m.logger.Warn("Existing snapshot is too old, will create new one",
			zap.String("snapshot_id", latestProgress.ID),
			zap.Duration("age", time.Since(latestProgress.CreatedAt)))
		return nil
	}

	// Filter out completed tables to only process remaining ones
	filteredTables := make(map[string]*state.TableSnapshot)
	completedCount := 0
	for tableKey, tableSnap := range latestProgress.Tables {
		tableSnap.Mu.RLock()
		status := tableSnap.Status
		tableSnap.Mu.RUnlock()

		if status == "COMPLETED" || status == "SCHEMA_ONLY" {
			completedCount++
		} else {
			// Reset IN_PROGRESS tables back to PENDING for retry
			if status == "IN_PROGRESS" {
				tableSnap.Mu.Lock()
				tableSnap.Status = "PENDING"
				tableSnap.Error = ""
				tableSnap.Mu.Unlock()
			}
			filteredTables[tableKey] = tableSnap
		}
	}

	// Update progress with filtered tables
	latestProgress.Tables = filteredTables
	latestProgress.CompletedTables = completedCount
	latestProgress.Status = "IN_PROGRESS"

	m.progress = latestProgress

	m.logger.Info("Resuming existing snapshot",
		zap.String("snapshot_id", latestProgress.ID),
		zap.Int("total_tables", latestProgress.TotalTables),
		zap.Int("completed_tables", completedCount),
		zap.Int("remaining_tables", len(filteredTables)))

	return nil
}

func (m *Manager) createNewSnapshot(ctx context.Context) error {
	// Get all tables from schema discovery
	allTables := m.schemaDiscovery.GetAllTables()

	// Filter tables to determine which ones should have data loaded
	dataLoadTables := m.filterTables(allTables)

	// Create snapshot progress for ALL tables
	m.progress = &state.SnapshotProgress{
		ID:          uuid.New().String(),
		Status:      "PENDING",
		TotalTables: len(allTables),
		Tables:      make(map[string]*state.TableSnapshot),
	}

	if m.cfg.SkipDataLoad {
		m.logger.Info("Creating snapshot with data loading disabled (schema-only mode)",
			zap.String("snapshot_id", m.progress.ID),
			zap.Int("total_tables", len(allTables)))
	} else {
		m.logger.Info("Creating new snapshot",
			zap.String("snapshot_id", m.progress.ID),
			zap.Int("total_tables", len(allTables)),
			zap.Int("data_load_tables", len(dataLoadTables)))
	}

	// Initialize table snapshots for ALL tables
	for tableKey, tableInfo := range allTables {
		// Check if this table should have data loaded
		// Data load is disabled if global skip_data_load is set, OR if table doesn't match filter
		_, matchesFilter := dataLoadTables[tableKey]
		shouldLoadData := !m.cfg.SkipDataLoad && matchesFilter

		m.progress.Tables[tableKey] = &state.TableSnapshot{
			Database:        tableInfo.Database,
			Table:           tableInfo.Name,
			Status:          "PENDING",
			ChunkSize:       m.cfg.ChunkSize,
			DataLoadEnabled: shouldLoadData,
		}
	}

	// Capture consistent position
	if err := m.captureConsistentPosition(ctx); err != nil {
		return fmt.Errorf("failed to capture consistent position: %w", err)
	}

	return nil
}

func (m *Manager) filterTables(allTables map[string]*common.TableInfo) map[string]*common.TableInfo {
	filteredTables := make(map[string]*common.TableInfo)

	for tableName, tableInfo := range allTables {
		if m.tableFilter.ShouldProcessTable(m.mysqlCfg.Database, tableName) {
			filteredTables[tableName] = tableInfo
		}
	}

	m.logger.Info("Filtered tables for snapshot",
		zap.Int("total_tables", len(allTables)),
		zap.Int("filtered_tables", len(filteredTables)))

	return filteredTables
}

func (m *Manager) captureConsistentPosition(ctx context.Context) error {
	position, err := m.coordinator.CaptureConsistentPosition(ctx)
	if err != nil {
		return fmt.Errorf("failed to capture consistent position: %w", err)
	}

	// Create checkpoint immediately when position is captured using the snapshot's StartTime
	// This ensures proper timestamp ordering and allows for resumption consistency
	err = m.stateManager.CreateCheckpoint(ctx, *position, m.progress.StartTime)
	if err != nil {
		return fmt.Errorf("failed to create snapshot checkpoint: %w", err)
	}

	m.logger.Info("Created snapshot checkpoint at consistent position",
		zap.String("snapshot_id", m.progress.ID),
		zap.String("position", fmt.Sprintf("%s:%d", position.File, position.Offset)),
		zap.String("gtid", position.GTID))

	return nil
}

func (m *Manager) executeWithConcurrency(ctx context.Context) error {
	// Start chunked reader
	if err := m.reader.Start(ctx); err != nil {
		return fmt.Errorf("failed to start chunked reader: %w", err)
	}

	// Create channels for coordination
	tableChan := make(chan string, len(m.progress.Tables))
	errorChan := make(chan error, m.cfg.ParallelTables)
	doneChan := make(chan string, len(m.progress.Tables))

	// Populate table channel
	for tableKey := range m.progress.Tables {
		tableChan <- tableKey
	}
	close(tableChan)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < m.cfg.ParallelTables; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.worker(ctx, tableChan, errorChan, doneChan)
		}()
	}

	// Wait for completion or error
	go func() {
		wg.Wait()
		close(doneChan)
		close(errorChan)
	}()

	// Monitor progress
	processedTables := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errorChan:
			if err != nil {
				return err
			}
		case tableKey, ok := <-doneChan:
			if !ok {
				// All workers finished
				return nil
			}
			processedTables++

			// Get the table status to determine how to log
			tableSnapshot := m.progress.Tables[tableKey]
			var statusMsg string
			if tableSnapshot != nil {
				switch tableSnapshot.Status {
				case "COMPLETED":
					statusMsg = "data loaded"
				case "SCHEMA_ONLY":
					statusMsg = "schema only"
				case "FAILED":
					statusMsg = "failed"
				default:
					statusMsg = tableSnapshot.Status
				}
			}

			m.logger.Info("Table snapshot processed",
				zap.String("table", tableKey),
				zap.String("status", statusMsg),
				zap.Int("processed", processedTables),
				zap.Int("total", m.progress.TotalTables))

			// Persist progress after each table completion
			if err := m.persistProgress(ctx); err != nil {
				m.logger.Error("Failed to persist progress after table completion",
					zap.String("table", tableKey),
					zap.Error(err))
				// Don't return error, just log it to avoid stopping the snapshot
			}
		}
	}
}

func (m *Manager) worker(ctx context.Context, tableChan <-chan string, errorChan chan<- error, doneChan chan<- string) {
	for {
		select {
		case <-ctx.Done():
			return
		case tableKey, ok := <-tableChan:
			if !ok {
				return
			}

			if err := m.snapshotTable(ctx, tableKey); err != nil {
				select {
				case errorChan <- fmt.Errorf("failed to snapshot table %s: %w", tableKey, err):
				case <-ctx.Done():
				}
				return
			}

			select {
			case doneChan <- tableKey:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (m *Manager) snapshotTable(ctx context.Context, tableKey string) error {
	tableSnapshot := m.progress.Tables[tableKey]
	if tableSnapshot == nil {
		return fmt.Errorf("table snapshot not found: %s", tableKey)
	}

	// Update status
	tableSnapshot.Mu.Lock()
	tableSnapshot.Status = "IN_PROGRESS"
	tableSnapshot.StartTime = time.Now()
	tableSnapshot.Mu.Unlock()

	// Always ensure ClickHouse table exists (schema creation)
	if m.cfg.CreateMissingTables {
		if err := m.ensureTableExists(ctx, tableSnapshot.Database, tableSnapshot.Table); err != nil {
			tableSnapshot.Mu.Lock()
			tableSnapshot.Status = "FAILED"
			tableSnapshot.Error = fmt.Sprintf("failed to create table: %v", err)
			endTime := time.Now()
			tableSnapshot.EndTime = &endTime
			tableSnapshot.Mu.Unlock()
			return fmt.Errorf("failed to ensure table exists: %w", err)
		}
	}

	// Check if data loading is enabled for this table
	if !tableSnapshot.DataLoadEnabled {
		// Schema-only table, mark as completed without loading data
		tableSnapshot.Mu.Lock()
		tableSnapshot.Status = "SCHEMA_ONLY"
		endTime := time.Now()
		tableSnapshot.EndTime = &endTime
		tableSnapshot.Mu.Unlock()

		m.logger.Info("Table schema created without data loading",
			zap.String("database", tableSnapshot.Database),
			zap.String("table", tableSnapshot.Table))

		return nil
	}

	// Get row count for data loading tables
	totalRows, err := m.reader.GetTableRowCount(ctx, tableSnapshot.Database, tableSnapshot.Table)
	if err != nil {
		return fmt.Errorf("failed to get row count: %w", err)
	}

	tableSnapshot.Mu.Lock()
	tableSnapshot.TotalRows = totalRows
	tableSnapshot.Mu.Unlock()

	if totalRows == 0 {
		// Empty table with data loading enabled
		tableSnapshot.Mu.Lock()
		tableSnapshot.Status = "COMPLETED"
		endTime := time.Now()
		tableSnapshot.EndTime = &endTime
		tableSnapshot.Mu.Unlock()

		m.logger.Info("Empty table completed",
			zap.String("database", tableSnapshot.Database),
			zap.String("table", tableSnapshot.Table))

		return nil
	}

	// Create chunks channel for data loading
	chunkChan := make(chan *ChunkInfo, 10)

	// Start reading chunks
	go func() {
		defer close(chunkChan)
		if err := m.reader.ReadTableChunks(ctx, tableSnapshot.Database, tableSnapshot.Table, tableSnapshot.ChunkSize, chunkChan); err != nil {
			m.logger.Error("Failed to read table chunks", zap.Error(err))
		}
	}()

	// Load chunks using the consistent snapshot timestamp from progress.StartTime
	if err := m.loader.LoadTable(ctx, tableSnapshot.Database, tableSnapshot.Table, chunkChan, m.progress.StartTime); err != nil {
		tableSnapshot.Mu.Lock()
		tableSnapshot.Status = "FAILED"
		tableSnapshot.Error = err.Error()
		endTime := time.Now()
		tableSnapshot.EndTime = &endTime
		tableSnapshot.Mu.Unlock()
		return err
	}

	// Mark as completed with data loaded
	tableSnapshot.Mu.Lock()
	tableSnapshot.Status = "COMPLETED"
	tableSnapshot.ProcessedRows = totalRows
	endTime := time.Now()
	tableSnapshot.EndTime = &endTime
	tableSnapshot.Mu.Unlock()

	m.logger.Info("Table data loading completed",
		zap.String("database", tableSnapshot.Database),
		zap.String("table", tableSnapshot.Table),
		zap.Int64("rows", totalRows))

	return nil
}

func (m *Manager) persistProgress(ctx context.Context) error {
	if m.progress == nil {
		return fmt.Errorf("no progress to persist")
	}

	// Set updated timestamp
	m.progress.UpdatedAt = time.Now()
	if m.progress.CreatedAt.IsZero() {
		m.progress.CreatedAt = m.progress.UpdatedAt
	}

	// Persist to state storage
	if err := m.stateManager.SaveSnapshotProgress(ctx, m.progress); err != nil {
		return fmt.Errorf("failed to save snapshot progress: %w", err)
	}

	m.logger.Debug("Snapshot progress persisted",
		zap.String("snapshot_id", m.progress.ID),
		zap.String("status", m.progress.Status),
		zap.Int("completed_tables", m.progress.CompletedTables),
		zap.Int("total_tables", m.progress.TotalTables))

	return nil
}

func (m *Manager) ensureTableExists(ctx context.Context, database, table string) error {
	// Check if table already exists in ClickHouse
	exists, err := m.chClient.TableExists(ctx, database, table)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		m.logger.Debug("Table already exists in ClickHouse",
			zap.String("database", database),
			zap.String("table", table))
		return nil
	}

	m.logger.Info("Creating missing table in ClickHouse",
		zap.String("database", database),
		zap.String("table", table))

	// Get MySQL table schema
	mysqlSchema, err := m.schemaDiscovery.GetTableSchema(database, table)
	if err != nil {
		return fmt.Errorf("failed to get table schema from MySQL: %w", err)
	}

	// Get default engine (default to MergeTree for now)
	defaultEngine := m.getDefaultEngine()

	// Translate MySQL schema to ClickHouse schema with proper ORDER BY
	chSchema, err := m.translator.TranslateTable(mysqlSchema, defaultEngine, m.chCfg.Database)
	if err != nil {
		return fmt.Errorf("failed to translate table schema: %w", err)
	}

	// Generate CREATE TABLE DDL with ORDER BY clause
	ddl, err := m.translator.GenerateCreateTableDDL(chSchema)
	if err != nil {
		return fmt.Errorf("failed to generate CREATE TABLE DDL: %w", err)
	}

	// Execute DDL to create table in ClickHouse
	if err := m.chClient.ExecuteDDL(ctx, ddl); err != nil {
		return fmt.Errorf("failed to create table in ClickHouse: %w", err)
	}

	m.logger.Info("Successfully created table in ClickHouse",
		zap.String("database", database),
		zap.String("table", table),
		zap.String("engine", string(defaultEngine)),
		zap.String("ddl", ddl))

	return nil
}

func (m *Manager) getDefaultEngine() clickhouse.TableEngine {
	// Only ReplacingMergeTree variants are supported
	return clickhouse.EngineReplacingMergeTree
}

// HasCompletedSnapshot checks if there's a completed snapshot in the state storage
func (m *Manager) HasCompletedSnapshot(ctx context.Context) (bool, error) {
	m.logger.Debug("Checking for completed snapshot")

	latestProgress, err := m.stateManager.GetLatestSnapshotProgress(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get latest snapshot progress: %w", err)
	}

	if latestProgress == nil {
		m.logger.Debug("No snapshot found in state storage")
		return false, nil
	}

	isCompleted := latestProgress.Status == "COMPLETED"
	m.logger.Debug("Found snapshot in state storage",
		zap.String("snapshot_id", latestProgress.ID),
		zap.String("status", latestProgress.Status),
		zap.Bool("is_completed", isCompleted))

	return isCompleted, nil
}

// GetLastCompletedSnapshot retrieves the last completed snapshot from state storage
func (m *Manager) GetLastCompletedSnapshot(ctx context.Context) (*state.SnapshotProgress, error) {
	latestProgress, err := m.stateManager.GetLatestSnapshotProgress(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest snapshot progress: %w", err)
	}

	if latestProgress == nil || latestProgress.Status != "COMPLETED" {
		return nil, nil
	}

	return latestProgress, nil
}
