package observability

import (
	"context"
	"time"
)

// ErrorReporter defines the interface for error reporting providers.
// Implementations include Sentry, Rollbar, Datadog, etc.
type ErrorReporter interface {
	// CaptureError captures an error with optional context information.
	CaptureError(ctx context.Context, err error, errCtx *ErrorContext) error

	// CaptureMessage captures a message with severity and optional context.
	CaptureMessage(ctx context.Context, msg string, severity Severity, errCtx *ErrorContext) error

	// AddBreadcrumb adds a breadcrumb for debugging trails.
	AddBreadcrumb(category, message string, data map[string]interface{})

	// SetTag sets a global tag that will be included in all subsequent events.
	SetTag(key, value string)

	// Flush waits for all pending events to be sent up to the given timeout.
	// Returns true if all events were sent, false if the timeout was reached.
	Flush(timeout time.Duration) bool

	// Close cleanly shuts down the error reporter.
	Close() error
}

// LogExporter defines the interface for log exporting providers.
// Implementations include NewRelic, Datadog, CloudWatch, etc.
type LogExporter interface {
	// Export sends a single log entry to the provider.
	Export(ctx context.Context, entry *LogEntry) error

	// ExportBatch sends multiple log entries in a batch.
	ExportBatch(ctx context.Context, entries []*LogEntry) error

	// Flush waits for all pending log entries to be sent up to the given timeout.
	// Returns true if all entries were sent, false if the timeout was reached.
	Flush(timeout time.Duration) bool

	// Close cleanly shuts down the log exporter.
	Close() error
}
