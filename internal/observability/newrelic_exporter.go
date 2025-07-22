package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/newrelic/go-agent/v3/integrations/logcontext-v2/nrzap"
	"github.com/newrelic/go-agent/v3/newrelic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/philippevezina/stream-bridge/internal/config"
)

// NewRelicExporter is a NewRelic implementation that provides log forwarding
// through the nrzap integration with Zap logging.
type NewRelicExporter struct {
	app    *newrelic.Application
	logger *zap.Logger
}

// NewNewRelicExporter creates a new NewRelic log exporter.
func NewNewRelicExporter(cfg *config.NewRelicConfig, logger *zap.Logger) (*NewRelicExporter, error) {
	if cfg.LicenseKey == "" {
		return nil, fmt.Errorf("NewRelic license key is required")
	}

	if cfg.AppName == "" {
		return nil, fmt.Errorf("NewRelic app name is required")
	}

	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName(cfg.AppName),
		newrelic.ConfigLicense(cfg.LicenseKey),
		newrelic.ConfigAppLogForwardingEnabled(cfg.LogForwarding),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create NewRelic application: %w", err)
	}

	// Wait for connection with a timeout
	if err := app.WaitForConnection(10 * time.Second); err != nil {
		logger.Warn("NewRelic connection timeout, will continue in background", zap.Error(err))
	}

	return &NewRelicExporter{
		app:    app,
		logger: logger,
	}, nil
}

// GetApplication returns the NewRelic application for use with nrzap wrapping.
func (e *NewRelicExporter) GetApplication() *newrelic.Application {
	return e.app
}

// WrapZapCore wraps a Zap core for background logging (non-transactional).
// This enables automatic log forwarding to NewRelic Logs.
func (e *NewRelicExporter) WrapZapCore(core zapcore.Core) (zapcore.Core, error) {
	wrappedCore, err := nrzap.WrapBackgroundCore(core, e.app)
	if err != nil {
		return core, fmt.Errorf("failed to wrap zap core for NewRelic: %w", err)
	}
	return wrappedCore, nil
}

// Export sends a single log entry to NewRelic (legacy interface, logs are now auto-forwarded).
func (e *NewRelicExporter) Export(ctx context.Context, entry *LogEntry) error {
	// Logs are automatically forwarded through the wrapped Zap core
	return nil
}

// ExportBatch sends multiple log entries (legacy interface, logs are now auto-forwarded).
func (e *NewRelicExporter) ExportBatch(ctx context.Context, entries []*LogEntry) error {
	// Logs are automatically forwarded through the wrapped Zap core
	return nil
}

// Flush waits for all pending log entries to be sent up to the given timeout.
func (e *NewRelicExporter) Flush(timeout time.Duration) bool {
	e.app.Shutdown(timeout)
	return true
}

// Close cleanly shuts down the NewRelic exporter.
func (e *NewRelicExporter) Close() error {
	e.app.Shutdown(5 * time.Second)
	return nil
}

// Compile-time interface compliance check
var _ LogExporter = (*NewRelicExporter)(nil)
