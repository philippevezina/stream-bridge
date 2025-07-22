package observability

import (
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/philippevezina/stream-bridge/internal/config"
)

// Manager orchestrates observability providers (error reporters and log exporters).
type Manager struct {
	cfg           *config.ObservabilityConfig
	logger        *zap.Logger
	errorReporter ErrorReporter
	logExporter   LogExporter
}

// NewManager creates a new observability manager with the given configuration.
func NewManager(cfg *config.ObservabilityConfig, logger *zap.Logger) (*Manager, error) {
	m := &Manager{
		cfg:    cfg,
		logger: logger,
	}

	if err := m.initErrorReporter(); err != nil {
		return nil, fmt.Errorf("failed to initialize error reporter: %w", err)
	}

	if err := m.initLogExporter(); err != nil {
		// Clean up error reporter if log exporter fails
		if m.errorReporter != nil {
			m.errorReporter.Close()
		}
		return nil, fmt.Errorf("failed to initialize log exporter: %w", err)
	}

	return m, nil
}

// initErrorReporter initializes the error reporter based on configuration.
func (m *Manager) initErrorReporter() error {
	if !m.cfg.ErrorReporting.Enabled {
		m.logger.Info("Error reporting disabled, using noop reporter")
		m.errorReporter = NewNoopErrorReporter()
		return nil
	}

	switch m.cfg.ErrorReporting.Provider {
	case "sentry":
		reporter, err := NewSentryReporter(&m.cfg.ErrorReporting.Sentry, m.logger)
		if err != nil {
			return fmt.Errorf("failed to create Sentry reporter: %w", err)
		}
		m.errorReporter = reporter
		m.logger.Info("Sentry error reporter initialized",
			zap.String("environment", m.cfg.ErrorReporting.Sentry.Environment))

	case "noop", "":
		m.errorReporter = NewNoopErrorReporter()
		m.logger.Info("Using noop error reporter")

	default:
		return fmt.Errorf("unknown error reporting provider: %s", m.cfg.ErrorReporting.Provider)
	}

	return nil
}

// initLogExporter initializes the log exporter based on configuration.
func (m *Manager) initLogExporter() error {
	if !m.cfg.LogExporting.Enabled {
		m.logger.Info("Log exporting disabled, using noop exporter")
		m.logExporter = NewNoopLogExporter()
		return nil
	}

	switch m.cfg.LogExporting.Provider {
	case "newrelic":
		exporter, err := NewNewRelicExporter(&m.cfg.LogExporting.NewRelic, m.logger)
		if err != nil {
			return fmt.Errorf("failed to create NewRelic exporter: %w", err)
		}
		m.logExporter = exporter
		m.logger.Info("NewRelic log exporter initialized",
			zap.String("app_name", m.cfg.LogExporting.NewRelic.AppName))

	case "noop", "":
		m.logExporter = NewNoopLogExporter()
		m.logger.Info("Using noop log exporter")

	default:
		return fmt.Errorf("unknown log exporting provider: %s", m.cfg.LogExporting.Provider)
	}

	return nil
}

// GetErrorReporter returns the configured error reporter.
func (m *Manager) GetErrorReporter() ErrorReporter {
	return m.errorReporter
}

// GetLogExporter returns the configured log exporter.
func (m *Manager) GetLogExporter() LogExporter {
	return m.logExporter
}

// WrapZapCore wraps a Zap core for NewRelic log forwarding if enabled.
// If NewRelic is not enabled or not configured, returns the original core unchanged.
func (m *Manager) WrapZapCore(core zapcore.Core) zapcore.Core {
	if !m.cfg.LogExporting.Enabled {
		return core
	}

	if nrExporter, ok := m.logExporter.(*NewRelicExporter); ok {
		wrappedCore, err := nrExporter.WrapZapCore(core)
		if err != nil {
			m.logger.Warn("Failed to wrap Zap core for NewRelic, using original core",
				zap.Error(err))
			return core
		}
		m.logger.Info("Zap core wrapped for NewRelic log forwarding")
		return wrappedCore
	}

	return core
}

// Stop gracefully shuts down all observability providers.
func (m *Manager) Stop() error {
	var errs []error

	// Flush and close error reporter
	if m.errorReporter != nil {
		flushTimeout := 5 * time.Second
		if m.cfg.ErrorReporting.Sentry.FlushTimeout > 0 {
			flushTimeout = m.cfg.ErrorReporting.Sentry.FlushTimeout
		}

		if !m.errorReporter.Flush(flushTimeout) {
			m.logger.Warn("Error reporter flush timed out")
		}

		if err := m.errorReporter.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error reporter close error: %w", err))
		}
	}

	// Flush and close log exporter
	if m.logExporter != nil {
		flushTimeout := 5 * time.Second
		if m.cfg.LogExporting.NewRelic.FlushTimeout > 0 {
			flushTimeout = m.cfg.LogExporting.NewRelic.FlushTimeout
		}

		if !m.logExporter.Flush(flushTimeout) {
			m.logger.Warn("Log exporter flush timed out")
		}

		if err := m.logExporter.Close(); err != nil {
			errs = append(errs, fmt.Errorf("log exporter close error: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("observability shutdown errors: %v", errs)
	}

	m.logger.Info("Observability manager stopped")
	return nil
}
