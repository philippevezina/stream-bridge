package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/config"
	"github.com/philippevezina/stream-bridge/internal/metrics"
	"github.com/philippevezina/stream-bridge/internal/mysql"
	"github.com/philippevezina/stream-bridge/internal/observability"
	"github.com/philippevezina/stream-bridge/internal/pipeline"
	"github.com/philippevezina/stream-bridge/internal/schema"
	"github.com/philippevezina/stream-bridge/internal/snapshot"
	"github.com/philippevezina/stream-bridge/internal/state"
)

type Application struct {
	cfg                  *config.Config
	logger               *zap.Logger
	mysqlCDC             *mysql.CDC
	clickhouseClient     *clickhouse.Client
	processor            *pipeline.Processor
	metricsManager       *metrics.Manager
	stateManager         *state.Manager
	schemaDiscovery      *schema.Discovery
	snapshotManager      *snapshot.Manager
	observabilityManager *observability.Manager
	forceSnapshot        bool
	running              bool
	mu                   sync.RWMutex
}

func main() {
	var (
		configPath    = flag.String("config", "configs/example.yaml", "Path to configuration file")
		version       = flag.Bool("version", false, "Show version information")
		snapshot      = flag.Bool("snapshot", false, "Enable initial snapshot")
		forceSnapshot = flag.Bool("force-snapshot", false, "Force a new snapshot even if one was completed")
		testSentry    = flag.Bool("test-sentry", false, "Send a test error to Sentry and exit")
		testNewRelic  = flag.Bool("test-newrelic", false, "Send test logs to NewRelic and exit")
	)
	flag.Parse()

	if *version {
		fmt.Printf("Stream Bridge %s\n", common.GetVersion())
		os.Exit(0)
	}

	if *testSentry {
		if err := runSentryTest(*configPath); err != nil {
			fmt.Fprintf(os.Stderr, "Sentry test failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if *testNewRelic {
		if err := runNewRelicTest(*configPath); err != nil {
			fmt.Fprintf(os.Stderr, "NewRelic test failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if err := run(*configPath, *snapshot, *forceSnapshot); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runSentryTest(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Force enable error reporting for the test
	cfg.Observability.ErrorReporting.Enabled = true

	logger, err := common.NewLogger(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	defer logger.Sync()

	logger.Info("Initializing observability manager for Sentry test...")

	obsManager, err := observability.NewManager(
		&cfg.Observability,
		common.LoggerWithComponent(logger, "observability"),
	)
	if err != nil {
		return fmt.Errorf("failed to create observability manager: %w", err)
	}

	// Create a test error
	testErr := fmt.Errorf("test error from stream-bridge: this is a test error to verify Sentry integration at %s", time.Now().Format(time.RFC3339))

	logger.Info("Sending test error to Sentry...",
		zap.String("error", testErr.Error()))

	// Capture the test error
	ctx := context.Background()
	obsManager.GetErrorReporter().CaptureError(ctx, testErr,
		observability.NewErrorContext("test", "sentry_verification").
			WithDatabase("test_database").
			WithTable("test_table").
			WithExtra("test_key", "test_value"))

	// Also send a test message
	obsManager.GetErrorReporter().CaptureMessage(ctx,
		"Test message from stream-bridge Sentry verification",
		observability.SeverityInfo,
		observability.NewErrorContext("test", "sentry_verification"))

	logger.Info("Flushing events to Sentry...")

	// Give it time to send
	if !obsManager.GetErrorReporter().Flush(10 * time.Second) {
		logger.Warn("Flush timed out, some events may not have been sent")
	}

	if err := obsManager.Stop(); err != nil {
		logger.Warn("Error stopping observability manager", zap.Error(err))
	}

	logger.Info("Sentry test completed successfully. Check your Sentry dashboard for the test error.")
	return nil
}

func runNewRelicTest(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Force enable log exporting for the test
	cfg.Observability.LogExporting.Enabled = true

	// Create logger core
	loggerCore, err := common.NewLoggerCore(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to create logger core: %w", err)
	}

	// Build initial logger for observability manager initialization
	initialLogger := loggerCore.BuildLogger(loggerCore.Core)

	initialLogger.Info("Initializing observability manager for NewRelic test...")

	obsManager, err := observability.NewManager(
		&cfg.Observability,
		common.LoggerWithComponent(initialLogger, "observability"),
	)
	if err != nil {
		return fmt.Errorf("failed to create observability manager: %w", err)
	}

	// Wrap the logger core for NewRelic
	wrappedCore := obsManager.WrapZapCore(loggerCore.Core)

	// Build final logger with wrapped core
	logger := loggerCore.BuildLogger(wrappedCore)
	defer logger.Sync()

	logger.Info("NewRelic log forwarding test started")

	// Send various log levels
	logger.Debug("Test DEBUG message from stream-bridge NewRelic verification",
		zap.String("test_key", "debug_value"),
		zap.Int("test_number", 1))

	logger.Info("Test INFO message from stream-bridge NewRelic verification",
		zap.String("test_key", "info_value"),
		zap.Int("test_number", 2),
		zap.String("component", "test"))

	logger.Warn("Test WARN message from stream-bridge NewRelic verification",
		zap.String("test_key", "warn_value"),
		zap.Int("test_number", 3))

	logger.Error("Test ERROR message from stream-bridge NewRelic verification",
		zap.String("test_key", "error_value"),
		zap.Int("test_number", 4),
		zap.Error(fmt.Errorf("simulated error for testing")))

	logger.Info("Flushing logs to NewRelic...")

	// Give it time to send
	time.Sleep(2 * time.Second)

	if !obsManager.GetLogExporter().Flush(15 * time.Second) {
		logger.Warn("Flush timed out, some logs may not have been sent")
	}

	if err := obsManager.Stop(); err != nil {
		initialLogger.Warn("Error stopping observability manager", zap.Error(err))
	}

	fmt.Println("NewRelic test completed. Check your NewRelic Logs UI for test messages.")
	fmt.Println("Look for logs with message containing 'stream-bridge NewRelic verification'")
	return nil
}

func run(configPath string, enableSnapshot bool, forceSnapshot bool) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Override snapshot setting if CLI flag is provided
	if enableSnapshot {
		cfg.Snapshot.Enabled = true
	}

	// Create logger core (allows wrapping for NewRelic)
	loggerCore, err := common.NewLoggerCore(&cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to create logger core: %w", err)
	}

	// Build initial logger for observability manager initialization
	initialLogger := loggerCore.BuildLogger(loggerCore.Core)

	// Create observability manager first (needs logger for its own initialization)
	observabilityManager, err := observability.NewManager(
		&cfg.Observability,
		common.LoggerWithComponent(initialLogger, "observability"),
	)
	if err != nil {
		return fmt.Errorf("failed to create observability manager: %w", err)
	}

	// Wrap the logger core for NewRelic if enabled
	wrappedCore := observabilityManager.WrapZapCore(loggerCore.Core)

	// Build final logger with potentially wrapped core
	logger := loggerCore.BuildLogger(wrappedCore)
	defer logger.Sync()

	app := &Application{
		cfg:                  cfg,
		logger:               logger,
		forceSnapshot:        forceSnapshot,
		observabilityManager: observabilityManager,
	}

	if err := app.initialize(); err != nil {
		return fmt.Errorf("failed to initialize application: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := app.start(ctx); err != nil {
		return fmt.Errorf("failed to start application: %w", err)
	}

	app.logger.Info("Stream Bridge started successfully",
		zap.String("version", common.GetVersion()))

	app.waitForShutdown()

	// Cancel context to signal all goroutines to stop
	cancel()
	app.logger.Info("Context cancelled, waiting for components to stop...")

	if err := app.stop(); err != nil {
		app.logger.Error("Error during shutdown", zap.Error(err))
		return err
	}

	app.logger.Info("Stream Bridge stopped gracefully")
	return nil
}

func (a *Application) initialize() error {
	a.logger.Info("Initializing components...")

	// Note: observabilityManager is already initialized in run() before logger wrapping

	metricsManager := metrics.NewManager(&a.cfg.Monitoring, common.LoggerWithComponent(a.logger, "metrics"))
	a.metricsManager = metricsManager

	// Create schema cache manager
	cacheManager := schema.NewCacheManager(common.LoggerWithComponent(a.logger, "schema-cache"))

	clickhouseClient, err := clickhouse.NewClient(&a.cfg.ClickHouse, cacheManager, common.LoggerWithComponent(a.logger, "clickhouse"))
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse client: %w", err)
	}
	a.clickhouseClient = clickhouseClient

	stateManager, err := state.NewManager(a.cfg.State, clickhouseClient, common.LoggerWithComponent(a.logger, "state"))
	if err != nil {
		return fmt.Errorf("failed to create state manager: %w", err)
	}
	a.stateManager = stateManager

	translationOptions := &schema.TranslationOptions{
		Engine:              clickhouse.TableEngine(a.cfg.Schema.DefaultEngine),
		PreserveNullability: a.cfg.Schema.PreserveNullable,
		TimestampPrecision:  a.cfg.Schema.TimestampPrecision,
		DefaultStringLength: 255,
		CustomTypeMappings:  make(map[string]string),
	}

	translator := schema.NewTranslator(common.LoggerWithComponent(a.logger, "schema-translator"), translationOptions)

	schemaDiscovery := schema.NewDiscovery(&a.cfg.MySQL, common.LoggerWithComponent(a.logger, "schema-discovery"))
	a.schemaDiscovery = schemaDiscovery

	processor := pipeline.NewProcessor(&a.cfg.Pipeline, &a.cfg.Schema, clickhouseClient, stateManager, translator, cacheManager, a.metricsManager.GetMetrics(), common.LoggerWithComponent(a.logger, "pipeline"))
	a.processor = processor

	mysqlCDC, err := mysql.NewCDC(&a.cfg.MySQL, stateManager, processor, common.LoggerWithComponent(a.logger, "mysql"))
	if err != nil {
		return fmt.Errorf("failed to create MySQL CDC: %w", err)
	}
	a.mysqlCDC = mysqlCDC

	// Initialize snapshot manager
	snapshotManager, err := snapshot.NewManager(
		&a.cfg.Snapshot,
		&a.cfg.MySQL,
		&a.cfg.ClickHouse,
		stateManager,
		schemaDiscovery,
		clickhouseClient,
		translator,
		common.LoggerWithComponent(a.logger, "snapshot"),
	)
	if err != nil {
		return fmt.Errorf("failed to create snapshot manager: %w", err)
	}
	a.snapshotManager = snapshotManager

	a.logger.Info("Components initialized successfully")
	return nil
}

func (a *Application) start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return fmt.Errorf("application is already running")
	}

	a.logger.Info("Starting components...")

	if err := a.metricsManager.Start(); err != nil {
		return fmt.Errorf("failed to start metrics manager: %w", err)
	}

	if err := a.stateManager.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize state manager: %w", err)
	}

	// Populate ClickHouse schema cache from existing tables
	if err := a.clickhouseClient.PopulateCacheFromClickHouse(ctx); err != nil {
		return fmt.Errorf("failed to populate ClickHouse schema cache: %w", err)
	}

	// Execute snapshot if enabled
	if a.cfg.Snapshot.Enabled {
		// Check if we already have a completed snapshot
		hasCompletedSnapshot, err := a.snapshotManager.HasCompletedSnapshot(ctx)
		if err != nil {
			return fmt.Errorf("failed to check for completed snapshot: %w", err)
		}

		if hasCompletedSnapshot && !a.forceSnapshot {
			// Use existing completed snapshot - no need to load schema
			completedSnapshot, err := a.snapshotManager.GetLastCompletedSnapshot(ctx)
			if err != nil {
				return fmt.Errorf("failed to get completed snapshot: %w", err)
			}

			if completedSnapshot != nil {
				a.logger.Info("Using existing completed snapshot, skipping schema loading",
					zap.String("snapshot_id", completedSnapshot.ID),
					zap.Time("completed_at", *completedSnapshot.EndTime))
			}
		} else {
			// Need to create a new snapshot - start schema discovery temporarily
			a.logger.Info("Creating new snapshot, loading schema discovery temporarily")

			if err := a.schemaDiscovery.Start(ctx); err != nil {
				return fmt.Errorf("failed to start schema discovery for snapshot: %w", err)
			}

			// Execute a new snapshot
			if a.forceSnapshot && hasCompletedSnapshot {
				a.logger.Info("Force snapshot flag set, creating new snapshot despite existing completed snapshot")
			} else {
				a.logger.Info("No completed snapshot found, executing new snapshot")
			}

			if err := a.snapshotManager.Start(ctx); err != nil {
				return fmt.Errorf("failed to start snapshot manager: %w", err)
			}

			if err := a.snapshotManager.ExecuteSnapshot(ctx); err != nil {
				return fmt.Errorf("failed to execute snapshot: %w", err)
			}

			// Checkpoint created automatically during snapshot execution
			a.logger.Info("Snapshot execution completed, checkpoint created automatically")

			// Stop and dispose schema discovery after snapshot is complete
			// Schema synchronization will now be handled via CDC DDL events only
			if err := a.schemaDiscovery.Stop(); err != nil {
				a.logger.Warn("Failed to stop schema discovery after snapshot", zap.Error(err))
			}
			a.schemaDiscovery.Dispose()
			a.schemaDiscovery = nil

			a.logger.Info("Schema discovery disposed after snapshot completion")
		}
	} else {
		a.logger.Info("Snapshots disabled, skipping schema loading entirely")
	}

	if err := a.processor.Start(); err != nil {
		return fmt.Errorf("failed to start processor: %w", err)
	}

	if err := a.mysqlCDC.Start(ctx); err != nil {
		return fmt.Errorf("failed to start MySQL CDC: %w", err)
	}

	go a.runEventForwarder(ctx)
	go a.runMetricsUpdater(ctx)

	a.running = true
	return nil
}

func (a *Application) stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.running {
		return nil
	}

	a.logger.Info("Stopping components...")

	var errors []error

	if err := a.mysqlCDC.Stop(); err != nil {
		errors = append(errors, fmt.Errorf("MySQL CDC stop error: %w", err))
	}

	if err := a.processor.Stop(); err != nil {
		errors = append(errors, fmt.Errorf("processor stop error: %w", err))
	}

	// Schema discovery is stopped after snapshot completion if it was started
	// No need to stop it here as it's not running during normal operation

	if a.snapshotManager != nil {
		if err := a.snapshotManager.Stop(); err != nil {
			errors = append(errors, fmt.Errorf("snapshot manager stop error: %w", err))
		}
	}

	if err := a.clickhouseClient.Close(); err != nil {
		errors = append(errors, fmt.Errorf("ClickHouse client close error: %w", err))
	}

	if err := a.metricsManager.Stop(); err != nil {
		errors = append(errors, fmt.Errorf("metrics manager stop error: %w", err))
	}

	if err := a.stateManager.Close(); err != nil {
		errors = append(errors, fmt.Errorf("state manager close error: %w", err))
	}

	// Stop observability manager last to capture any shutdown errors
	if a.observabilityManager != nil {
		if err := a.observabilityManager.Stop(); err != nil {
			errors = append(errors, fmt.Errorf("observability manager stop error: %w", err))
		}
	}

	a.running = false

	if len(errors) > 0 {
		return fmt.Errorf("shutdown errors: %v", errors)
	}

	return nil
}

func (a *Application) runEventForwarder(ctx context.Context) {
	a.logger.Info("Starting event forwarder")

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("Event forwarder stopped")
			return
		case event := <-a.mysqlCDC.EventChan():
			if event == nil {
				continue
			}

			a.metricsManager.GetMetrics().IncEventsProcessed()
			a.metricsManager.GetMetrics().SetLastEventTime(event.Timestamp)

			// Check processor queue length for backpressure
			processorMetrics := a.processor.GetMetrics()
			queueLength := processorMetrics.QueueLength

			// Apply backpressure if queue is getting full (80% threshold)
			queueCapacity := a.processor.EventChanCapacity()
			if queueLength > int(float64(queueCapacity)*0.8) {
				a.logger.Warn("Processor queue nearly full, applying backpressure",
					zap.Int("queue_length", queueLength),
					zap.Int("queue_capacity", queueCapacity))

				// Use timeout to prevent indefinite blocking while still applying backpressure
				timeoutCtx, cancel := context.WithTimeout(ctx, time.Second*30)
				select {
				case a.processor.EventChan() <- event:
					cancel()
				case <-timeoutCtx.Done():
					cancel()
					a.logger.Error("Failed to forward event after backpressure timeout",
						zap.String("event_id", event.ID))
					a.metricsManager.GetMetrics().IncEventsFailed()
				case <-ctx.Done():
					cancel()
					return
				}
			} else {
				// Normal operation - no backpressure needed
				select {
				case a.processor.EventChan() <- event:
				case <-ctx.Done():
					return
				}
			}
		case err := <-a.mysqlCDC.ErrorChan():
			if err != nil {
				a.logger.Error("MySQL CDC error", zap.Error(err))
				a.metricsManager.GetMetrics().IncEventsFailed()
				a.observabilityManager.GetErrorReporter().CaptureError(ctx, err,
					observability.NewErrorContext("mysql", "cdc_streaming"))
			}
		case err := <-a.processor.ErrorChan():
			if err != nil {
				a.logger.Error("Processor error", zap.Error(err))
				a.metricsManager.GetMetrics().IncEventsFailed()
				a.observabilityManager.GetErrorReporter().CaptureError(ctx, err,
					observability.NewErrorContext("pipeline", "event_processing"))
			}
		}
	}
}

func (a *Application) runMetricsUpdater(ctx context.Context) {
	a.logger.Info("Starting metrics updater")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("Metrics updater stopped")
			return
		case <-ticker.C:
			a.updateConnectionStatus()
			a.updateReplicationLag()
			a.updateQueueMetrics()
			a.updateCheckpointMetrics()
		}
	}
}

func (a *Application) updateConnectionStatus() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mysqlConnected := a.mysqlCDC.IsRunning()
	a.metricsManager.GetMetrics().SetConnectionStatus("mysql", mysqlConnected)

	clickhouseConnected := true
	if err := a.clickhouseClient.Ping(ctx); err != nil {
		clickhouseConnected = false
		a.logger.Warn("ClickHouse ping failed", zap.Error(err))
	}
	a.metricsManager.GetMetrics().SetConnectionStatus("clickhouse", clickhouseConnected)
}

func (a *Application) updateReplicationLag() {
	// Get the last event timestamp from metrics
	lastEventTime := a.metricsManager.GetMetrics().GetLastEventTime()
	if !lastEventTime.IsZero() {
		lag := time.Since(lastEventTime)
		a.metricsManager.GetMetrics().SetReplicationLag(lag)
	} else {
		// If no events processed yet, lag is 0
		a.metricsManager.GetMetrics().SetReplicationLag(0)
	}
}

func (a *Application) updateQueueMetrics() {
	if a.processor.IsRunning() {
		metrics := a.processor.GetMetrics()
		a.metricsManager.GetMetrics().SetQueueLength(metrics.QueueLength)

		workerQueueLengths := a.processor.GetWorkerQueueLengths()
		a.metricsManager.GetMetrics().SetWorkerQueueLengths(workerQueueLengths)

		batcherBufferSize := a.processor.GetBatcherBufferSize()
		a.metricsManager.GetMetrics().SetBatcherBufferSize(batcherBufferSize)
	}
}

func (a *Application) updateCheckpointMetrics() {
	if checkpoint := a.stateManager.GetCurrentCheckpoint(); checkpoint != nil {
		age := time.Since(checkpoint.CreatedAt)
		a.metricsManager.GetMetrics().SetCheckpointAge(age)
	}
}

func (a *Application) waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	a.logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
}
