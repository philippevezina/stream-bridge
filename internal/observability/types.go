package observability

import (
	"time"
)

// Severity represents the severity level of an error or message.
type Severity int

const (
	SeverityDebug Severity = iota
	SeverityInfo
	SeverityWarning
	SeverityError
	SeverityCritical
	SeverityFatal
)

// String returns the string representation of the severity level.
func (s Severity) String() string {
	switch s {
	case SeverityDebug:
		return "debug"
	case SeverityInfo:
		return "info"
	case SeverityWarning:
		return "warning"
	case SeverityError:
		return "error"
	case SeverityCritical:
		return "critical"
	case SeverityFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// ErrorContext provides contextual information for error reporting.
type ErrorContext struct {
	// Component identifies the application component where the error occurred.
	// Examples: "mysql", "clickhouse", "pipeline", "state"
	Component string

	// Operation describes the specific operation that failed.
	// Examples: "cdc_streaming", "batch_write", "checkpoint_save"
	Operation string

	// Database is the database name involved, if applicable.
	Database string

	// Table is the table name involved, if applicable.
	Table string

	// Position contains binlog position information, if applicable.
	Position string

	// Extra contains any additional key-value pairs to include.
	Extra map[string]interface{}
}

// LogEntry represents a log entry to be exported.
type LogEntry struct {
	// Level is the log level (debug, info, warn, error, etc.)
	Level string

	// Timestamp when the log entry was created.
	Timestamp time.Time

	// Message is the log message.
	Message string

	// Fields contains structured log fields.
	Fields map[string]interface{}

	// Caller contains the file:line of the log call, if available.
	Caller string

	// Component identifies the application component, if available.
	Component string
}

// NewErrorContext creates a new ErrorContext with the given component and operation.
func NewErrorContext(component, operation string) *ErrorContext {
	return &ErrorContext{
		Component: component,
		Operation: operation,
		Extra:     make(map[string]interface{}),
	}
}

// WithDatabase adds a database to the error context.
func (ec *ErrorContext) WithDatabase(database string) *ErrorContext {
	ec.Database = database
	return ec
}

// WithTable adds a table to the error context.
func (ec *ErrorContext) WithTable(table string) *ErrorContext {
	ec.Table = table
	return ec
}

// WithPosition adds a binlog position to the error context.
func (ec *ErrorContext) WithPosition(position string) *ErrorContext {
	ec.Position = position
	return ec
}

// WithExtra adds extra key-value pairs to the error context.
func (ec *ErrorContext) WithExtra(key string, value interface{}) *ErrorContext {
	if ec.Extra == nil {
		ec.Extra = make(map[string]interface{})
	}
	ec.Extra[key] = value
	return ec
}

// ToMap converts the ErrorContext to a map for use with error reporting providers.
func (ec *ErrorContext) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	if ec.Component != "" {
		result["component"] = ec.Component
	}
	if ec.Operation != "" {
		result["operation"] = ec.Operation
	}
	if ec.Database != "" {
		result["database"] = ec.Database
	}
	if ec.Table != "" {
		result["table"] = ec.Table
	}
	if ec.Position != "" {
		result["position"] = ec.Position
	}

	for k, v := range ec.Extra {
		result[k] = v
	}

	return result
}
