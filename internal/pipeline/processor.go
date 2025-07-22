package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/metrics"
	"github.com/philippevezina/stream-bridge/internal/schema"
	"github.com/philippevezina/stream-bridge/internal/state"
)

type Processor struct {
	cfg            *config.PipelineConfig
	schemaCfg      *config.SchemaConfig
	logger         *zap.Logger
	chClient       *clickhouse.Client
	eventChan      chan *common.Event
	errorChan      chan error
	workers        []*worker
	batcher        *batcher
	running        bool
	mu             sync.RWMutex
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	metrics        *processorMetrics
	metricsManager metrics.Metrics
	stateManager   *state.Manager
	ddlParser      *schema.DDLParser
	ddlTranslator  *schema.DDLTranslator
	translator     *schema.Translator
	cacheUpdater   *schema.DDLCacheUpdater

	// Table-based event queue system for natural ordering
	tableQueues   map[string]*tableEventQueue // per-table FIFO queues
	tableQueuesMu sync.RWMutex                // protects tableQueues map
	tableWorkers  map[string]*tableWorker     // per-table workers

	// Add shutdown synchronization
	shutdownOnce sync.Once
	shutdown     chan struct{}
}

type worker struct {
	id        int
	processor *Processor
	batchChan chan []*common.Event
}

type batcher struct {
	processor *Processor
	buffer    map[string][]*common.Event
	lastFlush time.Time
	mu        sync.Mutex
	// Enhanced batch tracking with per-table coordination
	activeBatches  sync.WaitGroup
	tableBatches   map[string]*sync.WaitGroup // per-table batch tracking
	tableBatchesMu sync.RWMutex               // protects tableBatches map
	// Failed events queue for retry with exponential backoff
	failedQueue   []*failedBatch
	failedQueueMu sync.Mutex
	// Round-robin worker selection
	nextWorkerIndex uint64 // atomic counter for even load distribution
}

// failedBatch represents a batch that failed to be sent to workers
type failedBatch struct {
	key         string
	events      []*common.Event
	failedAt    time.Time
	retryCount  int
	nextRetryAt time.Time
	workerIndex int  // original worker that was blocked
	pendingDone bool // true if activeBatches.Done() should be called when this batch is resolved
}

// tableEventQueue maintains FIFO ordering of events for a specific table
type tableEventQueue struct {
	table      string
	eventChan  chan *common.Event // Channel for event delivery (replaces slice + cond var)
	mu         sync.Mutex
	processing bool
}

// tableWorker processes events sequentially for a specific table
type tableWorker struct {
	id        int
	table     string
	processor *Processor
	queue     *tableEventQueue
	ctx       context.Context
	cancel    context.CancelFunc
	running   bool
	mu        sync.RWMutex
}

type processorMetrics struct {
	eventsProcessed   uint64 // Changed to atomic uint64
	eventsSuccessful  uint64 // Changed to atomic uint64
	eventsFailed      uint64 // Changed to atomic uint64
	batchesProcessed  uint64 // Changed to atomic uint64
	lastProcessedTime time.Time
	startTime         time.Time
	processingRate    float64
	mu                sync.RWMutex // Keep mutex for time fields and rate
}

func NewProcessor(cfg *config.PipelineConfig, schemaCfg *config.SchemaConfig, chClient *clickhouse.Client, stateManager *state.Manager, translator *schema.Translator, cacheManager *schema.CacheManager, metricsManager metrics.Metrics, logger *zap.Logger) *Processor {
	ctx, cancel := context.WithCancel(context.Background())

	ddlParser := schema.NewDDLParser(logger)
	ddlTranslator := schema.NewDDLTranslator(translator, logger)
	cacheUpdater := schema.NewDDLCacheUpdater(cacheManager, translator, logger)

	return &Processor{
		cfg:            cfg,
		schemaCfg:      schemaCfg,
		logger:         logger,
		chClient:       chClient,
		eventChan:      make(chan *common.Event, cfg.BufferSize),
		errorChan:      make(chan error, 1000),
		ctx:            ctx,
		cancel:         cancel,
		metrics:        &processorMetrics{startTime: time.Now()},
		metricsManager: metricsManager,
		stateManager:   stateManager,
		ddlParser:      ddlParser,
		ddlTranslator:  ddlTranslator,
		translator:     translator,
		cacheUpdater:   cacheUpdater,
		tableQueues:    make(map[string]*tableEventQueue),
		tableWorkers:   make(map[string]*tableWorker),
		shutdown:       make(chan struct{}),
	}
}

func (p *Processor) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("processor is already running")
	}

	p.batcher = &batcher{
		processor:    p,
		buffer:       make(map[string][]*common.Event),
		lastFlush:    time.Now(),
		tableBatches: make(map[string]*sync.WaitGroup),
		failedQueue:  make([]*failedBatch, 0),
	}

	p.workers = make([]*worker, p.cfg.WorkerCount)
	for i := 0; i < p.cfg.WorkerCount; i++ {
		p.workers[i] = &worker{
			id:        i,
			processor: p,
			batchChan: make(chan []*common.Event, p.cfg.WorkerChannelBufferSize),
		}
	}

	for _, worker := range p.workers {
		p.wg.Add(1)
		go worker.run(p.ctx)
	}

	p.wg.Add(1)
	go p.batcher.run(p.ctx)

	p.wg.Add(1)
	go p.processEvents(p.ctx)

	p.running = true
	p.logger.Info("Event processor started",
		zap.Int("worker_count", p.cfg.WorkerCount),
		zap.Int("batch_size", p.cfg.BatchSize),
		zap.Duration("batch_timeout", p.cfg.BatchTimeout))

	return nil
}

func (p *Processor) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	// Signal shutdown first
	p.shutdownOnce.Do(func() {
		close(p.shutdown)
	})

	// Cancel context to stop all goroutines (table workers will exit via ctx.Done())
	p.cancel()

	// Wait for all goroutines to finish (now they should exit cleanly)
	p.wg.Wait()

	// Now safe to close channels - no active writers
	if p.eventChan != nil {
		close(p.eventChan)
		p.eventChan = nil
	}
	if p.errorChan != nil {
		close(p.errorChan)
		p.errorChan = nil
	}

	p.running = false
	p.logger.Info("Event processor stopped")
	return nil
}

func (p *Processor) EventChan() chan<- *common.Event {
	return p.eventChan
}

func (p *Processor) EventChanCapacity() int {
	return cap(p.eventChan)
}

func (p *Processor) ErrorChan() <-chan error {
	return p.errorChan
}

func (p *Processor) GetMetrics() common.Metrics {
	// Acquire read lock first to ensure consistent snapshot across all metrics
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	// Read all values while holding the lock for consistency
	eventsProcessed := atomic.LoadUint64(&p.metrics.eventsProcessed)
	eventsSuccessful := atomic.LoadUint64(&p.metrics.eventsSuccessful)
	eventsFailed := atomic.LoadUint64(&p.metrics.eventsFailed)
	processingRate := p.metrics.processingRate
	lastEventTime := p.metrics.lastProcessedTime

	return common.Metrics{
		EventsProcessed:  int64(eventsProcessed),
		EventsSuccessful: int64(eventsSuccessful),
		EventsFailed:     int64(eventsFailed),
		ProcessingRate:   processingRate,
		LastEventTime:    lastEventTime,
		QueueLength:      len(p.eventChan),
	}
}

func (p *Processor) GetWorkerQueueLengths() []int {
	lengths := make([]int, len(p.workers))
	for i, worker := range p.workers {
		lengths[i] = len(worker.batchChan)
	}
	return lengths
}

func (p *Processor) GetBatcherBufferSize() int {
	p.batcher.mu.Lock()
	defer p.batcher.mu.Unlock()

	totalEvents := 0
	for _, events := range p.batcher.buffer {
		totalEvents += len(events)
	}
	return totalEvents
}

func (p *Processor) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

func (p *Processor) processEvents(ctx context.Context) {
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("Panic in event processing", zap.Any("panic", r))
			p.safeSendError(fmt.Errorf("panic in event processing: %v", r))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Event processing stopped due to context cancellation")
			return
		case <-p.shutdown:
			p.logger.Info("Event processing stopped due to shutdown signal")
			return
		case event := <-p.eventChan:
			if event == nil {
				continue
			}

			p.updateMetrics(event)

			// Route event to its table queue
			tableKey := fmt.Sprintf("%s.%s", event.Database, event.Table)
			p.enqueueEventToTable(tableKey, event)
		}
	}
}

func (p *Processor) updateMetrics(_ *common.Event) {
	// Use atomic increment for events processed
	eventsProcessed := atomic.AddUint64(&p.metrics.eventsProcessed, 1)

	p.metrics.mu.Lock()
	defer p.metrics.mu.Unlock()

	p.metrics.lastProcessedTime = time.Now()

	// Calculate processing rate based on total time since start
	elapsed := p.metrics.lastProcessedTime.Sub(p.metrics.startTime)
	if elapsed > 0 && eventsProcessed > 0 {
		p.metrics.processingRate = float64(eventsProcessed) / elapsed.Seconds()
	}
}

func (b *batcher) run(ctx context.Context) {
	defer b.processor.wg.Done()

	ticker := time.NewTicker(b.processor.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.flushAll()
			// Try to process any remaining failed batches during graceful shutdown
			b.processFailedBatchesDuringShutdown()
			return
		case <-ticker.C:
			b.processFailedBatches() // Process failed batches with exponential backoff
			b.flushExpired()
		}
	}
}

// processFailedBatchesDuringShutdown attempts to process all failed batches during shutdown
// with a timeout to prevent indefinite waiting
func (b *batcher) processFailedBatchesDuringShutdown() {
	shutdownTimeout := 30 * time.Second
	deadline := time.Now().Add(shutdownTimeout)

	b.processor.logger.Info("Processing failed batches during shutdown",
		zap.Int("queue_length", b.getFailedQueueLength()),
		zap.Duration("timeout", shutdownTimeout))

	// Try processing failed batches multiple times until queue is empty or timeout
	for time.Now().Before(deadline) {
		queueLength := b.getFailedQueueLength()
		if queueLength == 0 {
			b.processor.logger.Info("All failed batches processed during shutdown")
			return
		}

		b.processor.logger.Debug("Retrying failed batches during shutdown",
			zap.Int("remaining", queueLength))

		b.processFailedBatches()

		// Short sleep to allow workers to process
		time.Sleep(100 * time.Millisecond)
	}

	// Log warning if there are still failed batches after timeout
	remaining := b.getFailedQueueLength()
	if remaining > 0 {
		b.processor.logger.Warn("Shutdown timeout reached with unprocessed failed batches",
			zap.Int("remaining_batches", remaining),
			zap.Duration("timeout", shutdownTimeout))

		// Count total events that will be lost
		b.failedQueueMu.Lock()
		totalEvents := 0
		for _, batch := range b.failedQueue {
			totalEvents += len(batch.events)
		}
		b.failedQueueMu.Unlock()

		atomic.AddUint64(&b.processor.metrics.eventsFailed, uint64(totalEvents))

		b.processor.logger.Error("Failed events lost during shutdown",
			zap.Int("total_events", totalEvents),
			zap.Int("batches", remaining))
	}
}

// getFailedQueueLength returns the current length of the failed queue
func (b *batcher) getFailedQueueLength() int {
	b.failedQueueMu.Lock()
	defer b.failedQueueMu.Unlock()
	return len(b.failedQueue)
}

func (b *batcher) addEvent(event *common.Event) {
	start := time.Now()
	key := fmt.Sprintf("%s.%s", event.Database, event.Table)

	var shouldFlush bool
	var batchSize int

	// Critical section - minimize lock time
	b.mu.Lock()
	b.buffer[key] = append(b.buffer[key], event)
	batchSize = len(b.buffer[key])
	shouldFlush = batchSize >= b.processor.cfg.BatchSize
	b.mu.Unlock()

	// Flush outside of main lock to reduce contention
	if shouldFlush {
		flushStart := time.Now()
		b.processor.logger.Debug("Flushing batch due to size limit",
			zap.String("table", key),
			zap.Int("batch_size", batchSize))

		// Re-acquire lock only for flush operation
		b.mu.Lock()
		b.flushKeyLocked(key)
		b.mu.Unlock()

		flushDuration := time.Since(flushStart)
		if b.processor.metricsManager != nil {
			b.processor.metricsManager.ObserveBatcherOperation("flush", flushDuration)
		}
		if flushDuration > 100*time.Millisecond {
			b.processor.logger.Warn("Slow batch flush operation",
				zap.String("table", key),
				zap.Duration("duration", flushDuration))
		}
	}

	addDuration := time.Since(start)
	if b.processor.metricsManager != nil {
		b.processor.metricsManager.ObserveBatcherOperation("add_event", addDuration)
	}
	if addDuration > 10*time.Millisecond {
		b.processor.logger.Warn("Slow batcher addEvent operation",
			zap.String("table", key),
			zap.Duration("duration", addDuration),
			zap.Int("buffer_size", batchSize))
	}
}

func (b *batcher) flushExpired() {
	start := time.Now()

	b.mu.Lock()
	defer b.mu.Unlock()

	if time.Since(b.lastFlush) >= b.processor.cfg.BatchTimeout {
		b.processor.logger.Debug("Flushing expired batches",
			zap.Duration("time_since_last_flush", time.Since(b.lastFlush)))
		b.flushAllLocked()
		flushDuration := time.Since(start)
		if b.processor.metricsManager != nil {
			b.processor.metricsManager.ObserveBatcherOperation("flush_expired", flushDuration)
		}
		if flushDuration > 500*time.Millisecond {
			b.processor.logger.Warn("Slow batch flush_expired operation",
				zap.Duration("duration", flushDuration))
		}
	}
}

func (b *batcher) flushAll() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushAllLocked()
}

func (b *batcher) flushAllLocked() {
	for key := range b.buffer {
		b.flushKeyLocked(key)
	}
	b.lastFlush = time.Now()
}

func (b *batcher) flushKeyLocked(key string) {
	events := b.buffer[key]
	if len(events) == 0 {
		return
	}

	// Track this batch as active globally
	b.activeBatches.Add(1)

	// Track this batch as active for this specific table
	tableBatchSync := b.getTableBatchSync(key)
	tableBatchSync.Add(1)

	// Create a copy of events before deleting from buffer to handle failure recovery
	eventsCopy := make([]*common.Event, len(events))
	copy(eventsCopy, events)

	// Delete the key from the map
	delete(b.buffer, key)

	// Round-robin worker selection for even load distribution
	workerIndex := int(atomic.AddUint64(&b.nextWorkerIndex, 1) % uint64(len(b.processor.workers)))

	// Add configurable timeout to prevent indefinite blocking if worker channels are full
	timeoutDuration := b.processor.cfg.WorkerChannelTimeout
	if timeoutDuration <= 0 {
		timeoutDuration = 30 * time.Second // Default fallback
	}
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()

	select {
	case b.processor.workers[workerIndex].batchChan <- eventsCopy:
		// Batch successfully sent to worker
	case <-b.processor.ctx.Done():
		// Context cancelled (graceful shutdown)
		// CRITICAL FIX (CRITICAL-9): Do NOT call Done() here if enqueueing for retry.
		// Same reasoning as the timeout case.
		if len(eventsCopy) > 0 {
			// pendingDone=true indicates Done() has NOT been called yet
			b.enqueueFailedBatch(key, eventsCopy, workerIndex, 0, true)
			b.processor.logger.Info("Batch enqueued for retry due to context cancellation",
				zap.String("table", key),
				zap.Int("event_count", len(eventsCopy)))
		} else {
			// Empty batch - safe to mark as done immediately
			b.activeBatches.Done()
			tableBatchSync.Done()
		}
	case <-timeout.C:
		// Worker channel is full and blocking, try alternative approaches
		b.processor.logger.Warn("Worker channel blocked, attempting alternative distribution",
			zap.String("table", key),
			zap.Int("event_count", len(eventsCopy)),
			zap.Int("blocked_worker", workerIndex))

		// Try to distribute to a different worker if this one is blocked
		if !b.tryAlternativeWorkerDistribution(eventsCopy, key, workerIndex) {
			// CRITICAL FIX (CRITICAL-9): Do NOT call Done() here.
			// The batch is still pending - it will be retried. Calling Done() prematurely
			// would allow flush/checkpoint operations to complete before the batch is
			// actually processed, leading to data loss on crash.
			// Done() will be called either:
			// 1. When retry succeeds and worker processes the batch, OR
			// 2. When max retries exceeded (permanent failure)

			// Enqueue to failed queue for retry with exponential backoff
			// pendingDone=true indicates Done() has NOT been called yet
			b.enqueueFailedBatch(key, eventsCopy, workerIndex, 0, true)

			b.processor.logger.Error("All workers blocked, batch enqueued for retry",
				zap.String("table", key),
				zap.Int("event_count", len(eventsCopy)))
		}
	}
}

// tryAlternativeWorkerDistribution attempts to send events to alternative workers when primary worker is blocked
func (b *batcher) tryAlternativeWorkerDistribution(events []*common.Event, key string, blockedWorkerIndex int) bool {
	// Try each worker in round-robin fashion, skipping the blocked one
	for i := 0; i < len(b.processor.workers); i++ {
		if i == blockedWorkerIndex {
			continue // Skip the blocked worker
		}

		// Try non-blocking send to alternative worker
		select {
		case b.processor.workers[i].batchChan <- events:
			b.processor.logger.Debug("Successfully redistributed batch to alternative worker",
				zap.String("table", key),
				zap.Int("event_count", len(events)),
				zap.Int("original_worker", blockedWorkerIndex),
				zap.Int("alternative_worker", i))
			return true
		default:
			// This worker is also full, try next one
			continue
		}
	}

	// All workers are blocked
	return false
}

// enqueueFailedBatch adds a failed batch to the retry queue with exponential backoff.
// If pendingDone is true, the caller has NOT called activeBatches.Done() and expects
// this to be done when the batch is eventually resolved (either retry succeeds or max retries exceeded).
func (b *batcher) enqueueFailedBatch(key string, events []*common.Event, workerIndex int, retryCount int, pendingDone bool) {
	b.failedQueueMu.Lock()
	defer b.failedQueueMu.Unlock()

	now := time.Now()

	// Calculate next retry time with exponential backoff
	// Base delay: 1 second, max delay: 30 seconds
	baseDelay := time.Second
	maxDelay := 30 * time.Second
	retryDelay := time.Duration(1<<uint(retryCount)) * baseDelay
	if retryDelay > maxDelay {
		retryDelay = maxDelay
	}

	failedBatch := &failedBatch{
		key:         key,
		events:      events,
		failedAt:    now,
		retryCount:  retryCount,
		nextRetryAt: now.Add(retryDelay),
		workerIndex: workerIndex,
		pendingDone: pendingDone,
	}

	b.failedQueue = append(b.failedQueue, failedBatch)

	b.processor.logger.Info("Batch enqueued for retry",
		zap.String("table", key),
		zap.Int("event_count", len(events)),
		zap.Int("retry_count", retryCount),
		zap.Duration("retry_delay", retryDelay))
}

// processFailedBatches attempts to retry failed batches
func (b *batcher) processFailedBatches() {
	b.failedQueueMu.Lock()
	defer b.failedQueueMu.Unlock()

	if len(b.failedQueue) == 0 {
		return
	}

	now := time.Now()
	remaining := make([]*failedBatch, 0, len(b.failedQueue))

	for _, batch := range b.failedQueue {
		// Skip if not ready for retry yet
		if now.Before(batch.nextRetryAt) {
			remaining = append(remaining, batch)
			continue
		}

		// Try to send to any available worker
		sent := false
		for i := 0; i < len(b.processor.workers); i++ {
			select {
			case b.processor.workers[i].batchChan <- batch.events:
				b.processor.logger.Info("Successfully retried failed batch",
					zap.String("table", batch.key),
					zap.Int("event_count", len(batch.events)),
					zap.Int("retry_count", batch.retryCount),
					zap.Int("worker", i))
				sent = true

				// CRITICAL FIX (CRITICAL-9): Only add to active batches if the original
				// Done() was already called. If pendingDone is true, the batch is already
				// counted in activeBatches and the worker will call Done() when processing completes.
				if !batch.pendingDone {
					b.activeBatches.Add(1)
					tableBatchSync := b.getTableBatchSync(batch.key)
					tableBatchSync.Add(1)
				}
				break
			default:
				// This worker is full, try next one
				continue
			}
		}

		if !sent {
			// All workers still blocked, increment retry count and re-queue
			const maxRetries = 10
			if batch.retryCount < maxRetries {
				batch.retryCount++

				// Recalculate next retry time
				baseDelay := time.Second
				maxDelay := 30 * time.Second
				retryDelay := time.Duration(1<<uint(batch.retryCount)) * baseDelay
				if retryDelay > maxDelay {
					retryDelay = maxDelay
				}
				batch.nextRetryAt = now.Add(retryDelay)

				remaining = append(remaining, batch)

				b.processor.logger.Warn("Failed to retry batch, will retry again",
					zap.String("table", batch.key),
					zap.Int("event_count", len(batch.events)),
					zap.Int("retry_count", batch.retryCount),
					zap.Duration("next_retry_in", retryDelay))
			} else {
				// Max retries exceeded, mark events as permanently failed
				atomic.AddUint64(&b.processor.metrics.eventsFailed, uint64(len(batch.events)))

				// CRITICAL FIX (CRITICAL-9): If pendingDone is true, we must call Done()
				// now that the batch is permanently failed. This ensures activeBatches
				// count is properly decremented.
				if batch.pendingDone {
					b.activeBatches.Done()
					tableBatchSync := b.getTableBatchSync(batch.key)
					tableBatchSync.Done()
				}

				b.processor.logger.Error("Batch failed permanently after max retries",
					zap.String("table", batch.key),
					zap.Int("event_count", len(batch.events)),
					zap.Int("max_retries", maxRetries))
			}
		}
	}

	b.failedQueue = remaining
}

func (w *worker) run(ctx context.Context) {
	defer w.processor.wg.Done()

	w.processor.logger.Info("Worker started", zap.Int("worker_id", w.id))

	for {
		select {
		case <-ctx.Done():
			w.processor.logger.Info("Worker stopped", zap.Int("worker_id", w.id))
			return
		case batch := <-w.batchChan:
			if len(batch) == 0 {
				continue
			}
			w.processBatch(ctx, batch)
		}
	}
}

func (w *worker) processBatch(ctx context.Context, events []*common.Event) {
	// Mark batch as done when this function completes
	defer w.processor.batcher.activeBatches.Done()

	if len(events) == 0 {
		return
	}

	// Determine table keys for this batch and mark table-specific batches as active
	tableKeys := make(map[string]bool)
	for _, event := range events {
		tableKey := fmt.Sprintf("%s.%s", event.Database, event.Table)
		if !tableKeys[tableKey] {
			tableKeys[tableKey] = true
			// Mark table-specific batch as active
			tableBatchSync := w.processor.batcher.getTableBatchSync(tableKey)
			tableBatchSync.Add(1)
			defer tableBatchSync.Done()
		}
	}

	start := time.Now()
	defer func() {
		duration := time.Since(start)
		w.processor.logger.Debug("Batch processed",
			zap.Int("worker_id", w.id),
			zap.Int("event_count", len(events)),
			zap.Duration("duration", duration))
	}()

	groupedEvents := w.groupEventsByTypeAndTable(events)

	for key, typeEvents := range groupedEvents {
		for eventType, eventList := range typeEvents {
			if err := w.processEventGroup(ctx, key, eventType, eventList); err != nil {
				w.processor.logger.Error("Failed to process event group",
					zap.String("table", key),
					zap.String("event_type", string(eventType)),
					zap.Int("event_count", len(eventList)),
					zap.Error(err))

				// Use atomic increment for failed events count
				atomic.AddUint64(&w.processor.metrics.eventsFailed, uint64(len(eventList)))

				select {
				case w.processor.errorChan <- err:
				default:
				}
			} else {
				// Use atomic increments for success metrics
				atomic.AddUint64(&w.processor.metrics.eventsSuccessful, uint64(len(eventList)))
				atomic.AddUint64(&w.processor.metrics.batchesProcessed, 1)

			}
		}
	}

}

func (w *worker) groupEventsByTypeAndTable(events []*common.Event) map[string]map[common.EventType][]*common.Event {
	grouped := make(map[string]map[common.EventType][]*common.Event)

	for _, event := range events {
		key := fmt.Sprintf("%s.%s", event.Database, event.Table)

		if grouped[key] == nil {
			grouped[key] = make(map[common.EventType][]*common.Event)
		}

		grouped[key][event.Type] = append(grouped[key][event.Type], event)
	}

	return grouped
}

func (w *worker) processEventGroup(ctx context.Context, tableKey string, eventType common.EventType, events []*common.Event) error {
	if len(events) == 0 {
		return nil
	}

	database := events[0].Database
	table := events[0].Table

	var err error
	for attempt := 1; attempt <= w.processor.cfg.MaxRetries; attempt++ {
		switch eventType {
		case common.EventTypeInsert:
			start := time.Now()
			err = w.processor.chClient.Insert(ctx, database, table, events)
			if w.processor.metricsManager != nil {
				w.processor.metricsManager.ObserveClickHouseOperation("insert", time.Since(start))
			}
		case common.EventTypeUpdate:
			err = w.processUpdates(ctx, database, table, events)
		case common.EventTypeDelete:
			err = w.processDeletes(ctx, database, table, events)
		default:
			// DDL events are now handled separately in processDDLEvents
			return fmt.Errorf("unsupported event type: %s", eventType)
		}

		if err == nil {
			return nil
		}

		if attempt < w.processor.cfg.MaxRetries {
			// Calculate retry delay with overflow protection
			retryDelay := w.calculateRetryDelay(attempt, w.processor.cfg.RetryDelay)
			w.processor.logger.Warn("Retrying failed operation",
				zap.String("table", tableKey),
				zap.String("event_type", string(eventType)),
				zap.Int("attempt", attempt),
				zap.Duration("retry_delay", retryDelay),
				zap.Error(err))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}

	return fmt.Errorf("failed after %d attempts: %w", w.processor.cfg.MaxRetries, err)
}

func (w *worker) processUpdates(ctx context.Context, database, table string, events []*common.Event) error {
	start := time.Now()
	err := w.processor.chClient.UpdateBatch(ctx, database, table, events)
	if w.processor.metricsManager != nil {
		w.processor.metricsManager.ObserveClickHouseOperation("update", time.Since(start))
	}
	return err
}

func (w *worker) processDeletes(ctx context.Context, database, table string, events []*common.Event) error {
	start := time.Now()
	err := w.processor.chClient.DeleteBatch(ctx, database, table, events)
	if w.processor.metricsManager != nil {
		w.processor.metricsManager.ObserveClickHouseOperation("delete", time.Since(start))
	}
	return err
}

// calculateRetryDelay calculates retry delay with overflow protection and reasonable upper bounds
func (w *worker) calculateRetryDelay(attempt int, baseDelay time.Duration) time.Duration {
	// Prevent negative attempts or base delays
	if attempt < 1 {
		attempt = 1
	}
	if baseDelay <= 0 {
		baseDelay = time.Second
	}

	// Cap the maximum retry delay to prevent unreasonably long waits
	const maxRetryDelay = 5 * time.Minute

	// Use int64 for calculations to avoid overflow during multiplication
	attemptInt64 := int64(attempt)
	baseDelayNanos := int64(baseDelay)

	// Check for potential overflow before multiplication
	if attemptInt64 > int64(maxRetryDelay)/baseDelayNanos {
		return maxRetryDelay
	}

	calculatedDelay := time.Duration(attemptInt64 * baseDelayNanos)

	// Apply maximum limit
	if calculatedDelay > maxRetryDelay {
		return maxRetryDelay
	}

	return calculatedDelay
}

// executeDDLTransaction executes DDL with transaction support and rollback capabilities
func (p *Processor) executeDDLTransaction(ctx context.Context, event *common.Event) error {
	// Parse and validate DDL
	if !p.ddlParser.IsSupported(event.SQL) {
		p.logger.Warn("Unsupported DDL statement, skipping",
			zap.String("sql", event.SQL))
		return nil
	}

	mysqlDDL, err := p.ddlParser.Parse(event.SQL)
	if err != nil {
		return fmt.Errorf("failed to parse DDL: %w", err)
	}

	if mysqlDDL.Type == schema.DDLTypeUnknown {
		p.logger.Warn("Unknown DDL statement type, skipping",
			zap.String("sql", event.SQL))
		return nil
	}

	// Set database and table if not specified
	if mysqlDDL.Database == "" {
		mysqlDDL.Database = event.Database
	}
	if mysqlDDL.Table == "" {
		mysqlDDL.Table = event.Table
	}

	// Translate to ClickHouse DDL
	defaultEngine := clickhouse.EngineReplacingMergeTree
	chDDL, err := p.ddlTranslator.Translate(mysqlDDL, defaultEngine, p.chClient.GetDatabase())
	if err != nil {
		return fmt.Errorf("failed to translate DDL: %w", err)
	}

	// Validate schema consistency before execution using ClickHouse database/table names
	if err := p.validateSchemaConsistency(chDDL.Database, chDDL.Table, mysqlDDL); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}

	// Execute DDL statements (now using native SQL idempotency with IF NOT EXISTS / IF EXISTS)
	return p.executeDDLStatements(ctx, chDDL, mysqlDDL, event)
}

// validateSchemaConsistency validates that the DDL won't break existing data
// With SQL-level idempotency, we only check configuration-based blocking of destructive operations
func (p *Processor) validateSchemaConsistency(database, table string, mysqlDDL *schema.DDLStatement) error {
	// Check if destructive operations are allowed
	if !p.schemaCfg.AllowDestructiveDDL {
		if mysqlDDL.Type == schema.DDLTypeDropTable {
			return fmt.Errorf("destructive DDL blocked by configuration: DROP TABLE %s.%s", database, table)
		}
		// Check for DROP COLUMN in ALTER operations
		if mysqlDDL.Type == schema.DDLTypeAlterTable {
			for _, op := range mysqlDDL.Operations {
				if op.Action == schema.DDLActionDropColumn {
					return fmt.Errorf("destructive DDL blocked by configuration: DROP COLUMN %s in table %s.%s",
						op.Column.Name, database, table)
				}
			}
		}
	}

	// Log destructive operations for visibility
	if mysqlDDL.Type == schema.DDLTypeDropTable {
		p.logger.Warn("Destructive DDL operation detected",
			zap.String("database", database),
			zap.String("table", table),
			zap.String("type", string(mysqlDDL.Type)))
	}

	// Log DROP COLUMN operations
	if mysqlDDL.Type == schema.DDLTypeAlterTable {
		for _, operation := range mysqlDDL.Operations {
			if operation.Action == schema.DDLActionDropColumn && operation.Column != nil {
				p.logger.Warn("Destructive operation: dropping column",
					zap.String("database", database),
					zap.String("table", table),
					zap.String("column", operation.Column.Name))
			}
		}
	}

	return nil
}

// executeDDLStatements executes DDL statements sequentially
// No rollback is attempted - in CDC systems, MySQL is the source of truth.
// Idempotent operations allow safe retry after manual intervention.
func (p *Processor) executeDDLStatements(ctx context.Context, chDDL *schema.ClickHouseDDL, mysqlDDL *schema.DDLStatement, event *common.Event) error {
	if len(chDDL.Statements) == 0 {
		p.logger.Debug("No ClickHouse statements to execute")
		return nil
	}

	// For single statement, execute directly
	if len(chDDL.Statements) == 1 {
		return p.executeSingleStatement(ctx, chDDL.Statements[0], chDDL, mysqlDDL)
	}

	// For multiple statements, execute sequentially
	// Note: No rollback is attempted. In CDC systems, MySQL is source of truth.
	// If DDL fails, the system stops and requires manual intervention to reconcile schemas.
	// Idempotent operations (via validation/filtering) allow safe retry after fixing issues.
	executedCount := 0

	for i, stmt := range chDDL.Statements {
		if stmt == "" {
			continue
		}

		p.logger.Info("Executing ClickHouse DDL statement",
			zap.String("database", chDDL.Database),
			zap.String("table", chDDL.Table),
			zap.Int("statement_index", i),
			zap.String("statement", stmt))

		if err := p.chClient.ExecuteDDL(ctx, stmt); err != nil {
			p.logger.Error("DDL statement failed - manual intervention required",
				zap.String("statement", stmt),
				zap.Int("failed_at_index", i),
				zap.Int("statements_executed", executedCount),
				zap.Int("total_statements", len(chDDL.Statements)),
				zap.Error(err))

			return fmt.Errorf("DDL statement %d of %d failed (executed %d): %w. "+
				"Manual intervention required to reconcile schemas. "+
				"System uses idempotent DDL operations - safe to retry after fixing the issue",
				i, len(chDDL.Statements), executedCount, err)
		}

		executedCount++
	}

	p.logger.Info("Multi-statement DDL executed successfully",
		zap.String("database", event.Database),
		zap.String("table", event.Table),
		zap.Int("statements_executed", executedCount))

	// Update cache after successful multi-statement DDL execution using ClickHouse database/table
	if err := p.cacheUpdater.UpdateCacheAfterDDL(mysqlDDL, chDDL.Database, chDDL.Table); err != nil {
		p.logger.Error("CRITICAL: Schema cache update failed after multi-statement DDL execution",
			zap.String("database", chDDL.Database),
			zap.String("table", chDDL.Table),
			zap.String("ddl_type", string(chDDL.Type)),
			zap.Error(err))

		// Force cache invalidation to trigger database query on next access
		p.cacheUpdater.InvalidateTable(chDDL.Database, chDDL.Table)

		p.logger.Error("CRITICAL: Cache invalidated - next access will query database directly",
			zap.String("table", fmt.Sprintf("%s.%s", chDDL.Database, chDDL.Table)))
	}

	return nil
}

// executeSingleStatement executes a single DDL statement
func (p *Processor) executeSingleStatement(ctx context.Context, stmt string, chDDL *schema.ClickHouseDDL, mysqlDDL *schema.DDLStatement) error {
	p.logger.Info("Executing single ClickHouse DDL statement",
		zap.String("database", chDDL.Database),
		zap.String("table", chDDL.Table),
		zap.String("statement", stmt))

	if err := p.chClient.ExecuteDDL(ctx, stmt); err != nil {
		return err
	}

	// Update cache after successful DDL execution using ClickHouse database/table
	if err := p.cacheUpdater.UpdateCacheAfterDDL(mysqlDDL, chDDL.Database, chDDL.Table); err != nil {
		p.logger.Error("CRITICAL: Schema cache update failed after DDL execution",
			zap.String("database", chDDL.Database),
			zap.String("table", chDDL.Table),
			zap.String("ddl_type", string(chDDL.Type)),
			zap.Error(err))

		// Force cache invalidation to trigger database query on next access
		p.cacheUpdater.InvalidateTable(chDDL.Database, chDDL.Table)

		p.logger.Error("CRITICAL: Cache invalidated - next access will query database directly",
			zap.String("table", fmt.Sprintf("%s.%s", chDDL.Database, chDDL.Table)))
	}

	return nil
}

// executeDDLWithRetry executes DDL with retry logic and exponential backoff
func (p *Processor) executeDDLWithRetry(ctx context.Context, event *common.Event, tableKey string) error {
	maxRetries := p.cfg.MaxRetries
	baseDelay := p.cfg.RetryDelay
	maxDelay := 30 * time.Second

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		p.logger.Debug("Attempting DDL execution",
			zap.String("table", tableKey),
			zap.Int("attempt", attempt),
			zap.Int("max_retries", maxRetries))

		if err := p.executeDDLTransaction(ctx, event); err != nil {
			lastErr = err

			// Check if this is a retryable error
			if !p.isRetryableDDLError(err) {
				p.logger.Error("Non-retryable DDL error",
					zap.String("table", tableKey),
					zap.Error(err))
				return err
			}

			if attempt < maxRetries {
				// Calculate delay with exponential backoff
				delay := time.Duration(attempt) * baseDelay
				if delay > maxDelay {
					delay = maxDelay
				}

				p.logger.Warn("DDL execution failed, retrying",
					zap.String("table", tableKey),
					zap.Int("attempt", attempt),
					zap.Duration("retry_delay", delay),
					zap.Error(err))

				// Wait before retry
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
					// Continue to next attempt
				}
			}
		} else {
			// Success
			if attempt > 1 {
				p.logger.Info("DDL execution succeeded after retry",
					zap.String("table", tableKey),
					zap.Int("successful_attempt", attempt))
			}
			return nil
		}
	}

	return fmt.Errorf("DDL execution failed after %d attempts: %w", maxRetries, lastErr)
}

// isRetryableDDLError determines if a DDL error is retryable
func (p *Processor) isRetryableDDLError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Network-related errors are retryable
	retryableErrors := []string{
		"connection refused",
		"connection reset",
		"timeout",
		"network is unreachable",
		"temporary failure",
		"connection lost",
		"connection broken",
	}

	for _, retryableErr := range retryableErrors {
		if strings.Contains(errStr, retryableErr) {
			return true
		}
	}

	// Syntax errors, constraint violations, etc. are not retryable
	nonRetryableErrors := []string{
		"syntax error",
		"table already exists",
		"table doesn't exist",
		"column already exists",
		"column doesn't exist",
		"constraint violation",
		"permission denied",
	}

	for _, nonRetryableErr := range nonRetryableErrors {
		if strings.Contains(errStr, nonRetryableErr) {
			return false
		}
	}

	// If we can't classify the error, assume it's retryable
	return true
}

// attemptDDLRecovery attempts to recover from DDL failures
// CRITICAL: Only creates a recovery checkpoint if schema state is validated as consistent
// This prevents checkpointing half-completed DDL states that could cause schema divergence
func (p *Processor) attemptDDLRecovery(ctx context.Context, event *common.Event, originalErr error) error {
	tableKey := fmt.Sprintf("%s.%s", event.Database, event.Table)

	p.logger.Info("Attempting DDL recovery",
		zap.String("table", tableKey),
		zap.String("original_error", originalErr.Error()))

	// Recovery strategy 1: Validate table state consistency
	// CRITICAL: Block checkpoint creation if validation fails to prevent schema divergence
	if err := p.validateTableStateConsistency(ctx, event.Database, event.Table); err != nil {
		p.logger.Error("Table state consistency validation failed - cannot create recovery checkpoint",
			zap.String("table", tableKey),
			zap.Error(err))
		return fmt.Errorf("schema inconsistency detected, refusing to checkpoint: %w", err)
	}

	// Recovery strategy 2: Validate schema consistency between cache and actual ClickHouse state
	// CRITICAL: Ensures we don't checkpoint a half-completed DDL state
	if err := p.validateSchemaMatchesDatabase(ctx, event.Database, event.Table); err != nil {
		p.logger.Error("Schema consistency validation failed - cannot create recovery checkpoint",
			zap.String("table", tableKey),
			zap.Error(err))
		return fmt.Errorf("schema mismatch detected, refusing to checkpoint: %w", err)
	}

	// Recovery strategy 3: Create recovery checkpoint only after validation passes
	if p.stateManager != nil {
		if err := p.stateManager.CreateCheckpoint(ctx, event.Position, event.Timestamp); err != nil {
			p.logger.Error("Failed to create recovery checkpoint",
				zap.String("table", tableKey),
				zap.Error(err))
			return err
		}
		p.logger.Info("Created recovery checkpoint for failed DDL after schema validation",
			zap.String("table", tableKey),
			zap.String("failed_sql", event.SQL),
			zap.String("error_message", originalErr.Error()))
	}

	// Recovery strategy 4: Notify monitoring systems
	if p.metricsManager != nil {
		// Use existing metrics interface - increment failed events
		p.metricsManager.IncEventsFailed()
		p.logger.Info("Incremented DDL failure metrics",
			zap.String("table", tableKey),
			zap.String("error_type", p.classifyDDLError(originalErr)))
	}

	return nil
}

// validateTableStateConsistency validates table state after DDL failure
func (p *Processor) validateTableStateConsistency(ctx context.Context, database, table string) error {
	// Check if table exists in ClickHouse
	exists, err := p.chClient.TableExists(ctx, database, table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if exists {
		p.logger.Debug("Table exists in ClickHouse after DDL failure",
			zap.String("database", database),
			zap.String("table", table))
	} else {
		p.logger.Debug("Table does not exist in ClickHouse after DDL failure",
			zap.String("database", database),
			zap.String("table", table))
	}

	return nil
}

// validateSchemaMatchesDatabase compares cached schema with actual ClickHouse schema
// This detects half-completed DDL states where the cache and database are out of sync
func (p *Processor) validateSchemaMatchesDatabase(ctx context.Context, database, table string) error {
	// First check if table exists
	exists, err := p.chClient.TableExists(ctx, database, table)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	// If table doesn't exist in ClickHouse, we can't compare schemas
	// This is valid for DROP TABLE failures - no inconsistency possible
	if !exists {
		p.logger.Debug("Table does not exist in ClickHouse, skipping schema consistency check",
			zap.String("database", database),
			zap.String("table", table))
		return nil
	}

	// Get cached schema (if available)
	var cachedSchema *common.TableInfo
	if p.cacheUpdater != nil {
		cachedSchema, _ = p.cacheUpdater.GetTableSchema(database, table)
	}

	// If no cached schema, we can't compare - but this is potentially dangerous
	// We should be conservative and not allow checkpoint in this case
	if cachedSchema == nil {
		p.logger.Warn("No cached schema available for consistency check",
			zap.String("database", database),
			zap.String("table", table))
		// Allow checkpoint if cache is empty - this happens during initial setup
		// The actual schema will be loaded on next access
		return nil
	}

	// Query actual schema from ClickHouse
	actualSchema, err := p.chClient.QueryTableInfoFromDatabase(ctx, database, table)
	if err != nil {
		return fmt.Errorf("failed to query actual schema from ClickHouse: %w", err)
	}

	// Compare column counts
	if len(cachedSchema.Columns) != len(actualSchema.Columns) {
		return fmt.Errorf("column count mismatch: cached=%d, actual=%d",
			len(cachedSchema.Columns), len(actualSchema.Columns))
	}

	// Compare column names and types
	for colName, cachedCol := range cachedSchema.Columns {
		actualCol, exists := actualSchema.Columns[colName]
		if !exists {
			return fmt.Errorf("column %q exists in cache but not in ClickHouse", colName)
		}

		// Normalize types for comparison (ClickHouse types may have slight variations)
		cachedType := strings.ToLower(strings.TrimSpace(cachedCol.Type))
		actualType := strings.ToLower(strings.TrimSpace(actualCol.Type))

		if cachedType != actualType {
			return fmt.Errorf("type mismatch for column %q: cached=%q, actual=%q",
				colName, cachedCol.Type, actualCol.Type)
		}
	}

	// Check for columns in ClickHouse that aren't in cache
	for colName := range actualSchema.Columns {
		if _, exists := cachedSchema.Columns[colName]; !exists {
			return fmt.Errorf("column %q exists in ClickHouse but not in cache", colName)
		}
	}

	p.logger.Debug("Schema consistency validated successfully",
		zap.String("database", database),
		zap.String("table", table),
		zap.Int("column_count", len(actualSchema.Columns)))

	return nil
}

// classifyDDLError classifies DDL errors for metrics and monitoring
func (p *Processor) classifyDDLError(err error) string {
	if err == nil {
		return "unknown"
	}

	errStr := strings.ToLower(err.Error())

	if strings.Contains(errStr, "syntax") {
		return "syntax_error"
	}
	if strings.Contains(errStr, "timeout") {
		return "timeout"
	}
	if strings.Contains(errStr, "connection") {
		return "connection_error"
	}
	if strings.Contains(errStr, "permission") {
		return "permission_error"
	}
	if strings.Contains(errStr, "exists") {
		return "existence_error"
	}

	return "other"
}

// FlushAndWaitForCompletion flushes all pending events and waits for completion
// This is critical for ensuring data consistency before checkpoints
func (p *Processor) FlushAndWaitForCompletion(ctx context.Context, timeout time.Duration) error {
	if !p.IsRunning() {
		return fmt.Errorf("processor is not running")
	}

	p.logger.Info("Starting flush and wait for completion", zap.Duration("timeout", timeout))
	start := time.Now()

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Step 1: Flush all pending batches
	p.logger.Debug("Flushing all pending batches")
	p.batcher.flushAll()

	// Step 2: Wait for all active batches to complete processing
	p.logger.Debug("Waiting for all active batches to complete")
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.batcher.activeBatches.Wait()
	}()

	// Wait with timeout
	select {
	case <-done:
		elapsed := time.Since(start)
		p.logger.Info("Flush and wait completed successfully",
			zap.Duration("elapsed", elapsed))
		return nil
	case <-timeoutCtx.Done():
		elapsed := time.Since(start)
		return fmt.Errorf("timeout waiting for events to flush after %v (elapsed: %v)", timeout, elapsed)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// safeSendError safely sends an error to the errorChan, checking shutdown state
func (p *Processor) safeSendError(err error) {
	select {
	case <-p.shutdown:
		p.logger.Debug("Error during shutdown", zap.Error(err))
	case p.errorChan <- err:
		// Successfully sent
	default:
		p.logger.Error("Error channel full, dropping error", zap.Error(err))
	}
}

// Table-based event queue methods for maintaining binlog order

// enqueueEventToTable adds an event to the appropriate table queue
func (p *Processor) enqueueEventToTable(tableKey string, event *common.Event) {
	queue := p.getOrCreateTableQueue(tableKey)

	// Send event to worker via channel
	// Use select with context to handle shutdown gracefully
	select {
	case <-p.ctx.Done():
		p.logger.Warn("Cannot enqueue event, processor shutting down",
			zap.String("table", tableKey),
			zap.String("event_id", event.ID))
		return
	case queue.eventChan <- event:
		// Event sent successfully
	}
}

// getOrCreateTableQueue gets or creates a queue for the given table
func (p *Processor) getOrCreateTableQueue(tableKey string) *tableEventQueue {
	p.tableQueuesMu.Lock()
	defer p.tableQueuesMu.Unlock()

	queue, exists := p.tableQueues[tableKey]
	if !exists {
		queue = &tableEventQueue{
			table:     tableKey,
			eventChan: make(chan *common.Event, 1000), // Buffered channel for events
		}
		p.tableQueues[tableKey] = queue

		// Start worker for this table
		p.startTableWorker(tableKey, queue)
	}

	return queue
}

// startTableWorker starts a dedicated worker for a table
func (p *Processor) startTableWorker(tableKey string, queue *tableEventQueue) {
	ctx, cancel := context.WithCancel(p.ctx)

	worker := &tableWorker{
		id:        len(p.tableWorkers),
		table:     tableKey,
		processor: p,
		queue:     queue,
		ctx:       ctx,
		cancel:    cancel,
	}

	p.tableWorkers[tableKey] = worker

	p.wg.Add(1)
	go worker.run()
}

// run processes events sequentially for a specific table
func (tw *tableWorker) run() {
	defer tw.processor.wg.Done()
	defer tw.cancel()

	tw.processor.logger.Info("Table worker started",
		zap.String("table", tw.table),
		zap.Int("worker_id", tw.id))

	tw.mu.Lock()
	tw.running = true
	tw.mu.Unlock()

	for {
		event := tw.getNextEvent()
		if event == nil {
			// nil means context was cancelled - time to exit
			tw.processor.logger.Info("Table worker stopped",
				zap.String("table", tw.table),
				zap.Int("worker_id", tw.id))
			return
		}

		tw.processEvent(event)
	}
}

// getNextEvent gets the next event from the queue, waiting if necessary
func (tw *tableWorker) getNextEvent() *common.Event {
	select {
	case <-tw.ctx.Done():
		return nil
	case event := <-tw.queue.eventChan:
		return event
	}
}

// processEvent processes a single event maintaining binlog order
func (tw *tableWorker) processEvent(event *common.Event) {
	switch event.Type {
	case common.EventTypeDDL:
		tw.processDDLEvent(event)
	default:
		// DML events - batch them for efficiency
		tw.processor.batcher.addEvent(event)
	}
}

// processDDLEvent processes DDL events synchronously
func (tw *tableWorker) processDDLEvent(event *common.Event) {
	tableKey := fmt.Sprintf("%s.%s", event.Database, event.Table)

	tw.processor.logger.Info("Processing DDL event",
		zap.String("table", tableKey),
		zap.String("sql", event.SQL))

	// Wait for any pending batches to complete for this table
	tw.waitForTableBatches(tableKey)

	// Execute DDL synchronously
	if err := tw.processor.executeDDLWithRetry(tw.ctx, event, tableKey); err != nil {
		tw.processor.logger.Error("DDL execution failed",
			zap.String("table", tableKey),
			zap.String("sql", event.SQL),
			zap.Error(err))

		// Attempt recovery
		if recoveryErr := tw.processor.attemptDDLRecovery(tw.ctx, event, err); recoveryErr != nil {
			tw.processor.logger.Error("DDL recovery failed",
				zap.String("table", tableKey),
				zap.Error(recoveryErr))
		}

		tw.processor.safeSendError(fmt.Errorf("DDL failed for table %s: %w", tableKey, err))
	} else {
		tw.processor.logger.Info("DDL executed successfully",
			zap.String("table", tableKey),
			zap.String("sql", event.SQL))

		// Update metrics
		atomic.AddUint64(&tw.processor.metrics.eventsSuccessful, 1)
	}
}

// waitForTableBatches waits for pending batches to complete for a specific table
func (tw *tableWorker) waitForTableBatches(tableKey string) {
	timeout := tw.processor.cfg.DDLFlushTimeout
	if timeout <= 0 {
		timeout = 1 * time.Minute
	}

	ctx, cancel := context.WithTimeout(tw.ctx, timeout)
	defer cancel()

	// Get table-specific batch sync
	tableBatchSync := tw.processor.batcher.getTableBatchSync(tableKey)

	done := make(chan struct{})
	go func() {
		defer close(done)
		tableBatchSync.Wait()
	}()

	select {
	case <-done:
		tw.processor.logger.Debug("All table batches completed before DDL",
			zap.String("table", tableKey))
	case <-ctx.Done():
		tw.processor.logger.Warn("Timeout waiting for table batches before DDL",
			zap.String("table", tableKey),
			zap.Duration("timeout", timeout))
	}
}

// getTableBatchSync gets or creates a WaitGroup for table-specific batch synchronization
func (b *batcher) getTableBatchSync(tableKey string) *sync.WaitGroup {
	b.tableBatchesMu.Lock()
	defer b.tableBatchesMu.Unlock()

	wg, exists := b.tableBatches[tableKey]
	if !exists {
		wg = &sync.WaitGroup{}
		b.tableBatches[tableKey] = wg
	}

	return wg
}
