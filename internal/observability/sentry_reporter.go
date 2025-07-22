package observability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/config"
)

// SentryReporter is a Sentry implementation of the ErrorReporter interface.
type SentryReporter struct {
	logger *zap.Logger
	hub    *sentry.Hub
	mu     sync.RWMutex
	tags   map[string]string
}

// NewSentryReporter creates a new Sentry error reporter.
func NewSentryReporter(cfg *config.SentryConfig, logger *zap.Logger) (*SentryReporter, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("Sentry DSN is required")
	}

	err := sentry.Init(sentry.ClientOptions{
		Dsn:              cfg.DSN,
		Environment:      cfg.Environment,
		Release:          cfg.Release,
		SampleRate:       cfg.SampleRate,
		Debug:            cfg.Debug,
		AttachStacktrace: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Sentry: %w", err)
	}

	return &SentryReporter{
		logger: logger,
		hub:    sentry.CurrentHub(),
		tags:   make(map[string]string),
	}, nil
}

// CaptureError captures an error with optional context information.
func (r *SentryReporter) CaptureError(ctx context.Context, err error, errCtx *ErrorContext) error {
	if err == nil {
		return nil
	}

	r.hub.WithScope(func(scope *sentry.Scope) {
		r.applyContext(scope, errCtx)
		r.applyTags(scope)
		r.hub.CaptureException(err)
	})

	return nil
}

// CaptureMessage captures a message with severity and optional context.
func (r *SentryReporter) CaptureMessage(ctx context.Context, msg string, severity Severity, errCtx *ErrorContext) error {
	r.hub.WithScope(func(scope *sentry.Scope) {
		scope.SetLevel(mapSeverityToSentryLevel(severity))
		r.applyContext(scope, errCtx)
		r.applyTags(scope)
		r.hub.CaptureMessage(msg)
	})

	return nil
}

// AddBreadcrumb adds a breadcrumb for debugging trails.
func (r *SentryReporter) AddBreadcrumb(category, message string, data map[string]interface{}) {
	r.hub.AddBreadcrumb(&sentry.Breadcrumb{
		Category:  category,
		Message:   message,
		Data:      data,
		Timestamp: time.Now(),
	}, nil)
}

// SetTag sets a global tag that will be included in all subsequent events.
func (r *SentryReporter) SetTag(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tags[key] = value
}

// Flush waits for all pending events to be sent up to the given timeout.
func (r *SentryReporter) Flush(timeout time.Duration) bool {
	return sentry.Flush(timeout)
}

// Close cleanly shuts down the Sentry reporter.
func (r *SentryReporter) Close() error {
	sentry.Flush(2 * time.Second)
	return nil
}

// applyContext applies ErrorContext to a Sentry scope.
func (r *SentryReporter) applyContext(scope *sentry.Scope, errCtx *ErrorContext) {
	if errCtx == nil {
		return
	}

	if errCtx.Component != "" {
		scope.SetTag("component", errCtx.Component)
	}
	if errCtx.Operation != "" {
		scope.SetTag("operation", errCtx.Operation)
	}
	if errCtx.Database != "" {
		scope.SetTag("database", errCtx.Database)
	}
	if errCtx.Table != "" {
		scope.SetTag("table", errCtx.Table)
	}
	if errCtx.Position != "" {
		scope.SetExtra("binlog_position", errCtx.Position)
	}

	for k, v := range errCtx.Extra {
		scope.SetExtra(k, v)
	}
}

// applyTags applies global tags to a Sentry scope.
func (r *SentryReporter) applyTags(scope *sentry.Scope) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for k, v := range r.tags {
		scope.SetTag(k, v)
	}
}

// mapSeverityToSentryLevel maps observability severity to Sentry level.
func mapSeverityToSentryLevel(severity Severity) sentry.Level {
	switch severity {
	case SeverityDebug:
		return sentry.LevelDebug
	case SeverityInfo:
		return sentry.LevelInfo
	case SeverityWarning:
		return sentry.LevelWarning
	case SeverityError:
		return sentry.LevelError
	case SeverityCritical, SeverityFatal:
		return sentry.LevelFatal
	default:
		return sentry.LevelError
	}
}

// Compile-time interface compliance check
var _ ErrorReporter = (*SentryReporter)(nil)
