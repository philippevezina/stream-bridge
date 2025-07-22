package observability

import (
	"context"
	"time"
)

// NoopErrorReporter is a no-op implementation of ErrorReporter.
// It provides zero-cost operation when error reporting is disabled.
type NoopErrorReporter struct{}

// NewNoopErrorReporter creates a new no-op error reporter.
func NewNoopErrorReporter() *NoopErrorReporter {
	return &NoopErrorReporter{}
}

// CaptureError is a no-op that always succeeds.
func (n *NoopErrorReporter) CaptureError(_ context.Context, _ error, _ *ErrorContext) error {
	return nil
}

// CaptureMessage is a no-op that always succeeds.
func (n *NoopErrorReporter) CaptureMessage(_ context.Context, _ string, _ Severity, _ *ErrorContext) error {
	return nil
}

// AddBreadcrumb is a no-op.
func (n *NoopErrorReporter) AddBreadcrumb(_, _ string, _ map[string]interface{}) {}

// SetTag is a no-op.
func (n *NoopErrorReporter) SetTag(_, _ string) {}

// Flush is a no-op that always returns true.
func (n *NoopErrorReporter) Flush(_ time.Duration) bool {
	return true
}

// Close is a no-op that always succeeds.
func (n *NoopErrorReporter) Close() error {
	return nil
}

// NoopLogExporter is a no-op implementation of LogExporter.
// It provides zero-cost operation when log exporting is disabled.
type NoopLogExporter struct{}

// NewNoopLogExporter creates a new no-op log exporter.
func NewNoopLogExporter() *NoopLogExporter {
	return &NoopLogExporter{}
}

// Export is a no-op that always succeeds.
func (n *NoopLogExporter) Export(_ context.Context, _ *LogEntry) error {
	return nil
}

// ExportBatch is a no-op that always succeeds.
func (n *NoopLogExporter) ExportBatch(_ context.Context, _ []*LogEntry) error {
	return nil
}

// Flush is a no-op that always returns true.
func (n *NoopLogExporter) Flush(_ time.Duration) bool {
	return true
}

// Close is a no-op that always succeeds.
func (n *NoopLogExporter) Close() error {
	return nil
}

// Compile-time interface compliance checks
var (
	_ ErrorReporter = (*NoopErrorReporter)(nil)
	_ LogExporter   = (*NoopLogExporter)(nil)
)
