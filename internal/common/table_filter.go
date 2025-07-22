package common

import (
	"fmt"
	"regexp"

	"github.com/philippevezina/stream-bridge/internal/config"
)

// TableFilter provides table filtering functionality based on configuration
type TableFilter struct {
	includeRegex  []*regexp.Regexp
	excludeRegex  []*regexp.Regexp
	includeTables map[string]bool
	excludeTables map[string]bool
}

// NewTableFilter creates a new table filter from configuration
func NewTableFilter(cfg config.TableFilterConfig) (*TableFilter, error) {
	tf := &TableFilter{
		includeTables: make(map[string]bool),
		excludeTables: make(map[string]bool),
	}

	// Compile include patterns
	for _, pattern := range cfg.IncludePatterns {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid include pattern '%s': %w", pattern, err)
		}
		tf.includeRegex = append(tf.includeRegex, regex)
	}

	// Compile exclude patterns
	for _, pattern := range cfg.ExcludePatterns {
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid exclude pattern '%s': %w", pattern, err)
		}
		tf.excludeRegex = append(tf.excludeRegex, regex)
	}

	// Build include tables map
	for _, table := range cfg.IncludeTables {
		tf.includeTables[table] = true
	}

	// Build exclude tables map
	for _, table := range cfg.ExcludeTables {
		tf.excludeTables[table] = true
	}

	return tf, nil
}

// ShouldProcessTable determines if a table should be processed based on the filter configuration
func (tf *TableFilter) ShouldProcessTable(database, table string) bool {
	fullName := fmt.Sprintf("%s.%s", database, table)

	// Check exclude tables first (exact match)
	if tf.excludeTables[fullName] || tf.excludeTables[table] {
		return false
	}

	// Check exclude patterns
	for _, regex := range tf.excludeRegex {
		if regex.MatchString(fullName) || regex.MatchString(table) {
			return false
		}
	}

	// If we have include filters, the table must match at least one
	if len(tf.includeTables) > 0 || len(tf.includeRegex) > 0 {
		// Check include tables (exact match)
		if tf.includeTables[fullName] || tf.includeTables[table] {
			return true
		}

		// Check include patterns
		for _, regex := range tf.includeRegex {
			if regex.MatchString(fullName) || regex.MatchString(table) {
				return true
			}
		}

		// No include filter matched
		return false
	}

	// No include filters defined, so include all non-excluded tables
	return true
}
