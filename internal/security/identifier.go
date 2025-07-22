package security

import (
	"fmt"
	"regexp"
	"strings"
)

// identifierRegex matches valid SQL identifiers (alphanumeric + underscore, must start with letter or underscore)
var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ValidateIdentifier validates that an identifier (table name, database name, column name) is safe for SQL interpolation.
// This is critical for preventing SQL injection attacks since SQL databases (including ClickHouse) do not support
// parameterized identifiers - only parameterized values.
//
// Validation rules:
// 1. Length: 1-255 characters (ClickHouse limit is 255)
// 2. Format: Must match ^[a-zA-Z_][a-zA-Z0-9_]*$ (alphanumeric + underscore, starts with letter or underscore)
//
// Note: Reserved words (like SELECT, TABLE, etc.) are allowed because we always escape identifiers with backticks.
// When properly quoted, reserved words are safe to use as identifiers in ClickHouse.
//
// Parameters:
//   - identifier: The identifier to validate (e.g., "users", "order_id", "mydb", "test")
//   - identifierType: Human-readable type for error messages (e.g., "table name", "column name", "database name")
//
// Returns:
//   - nil if valid
//   - error describing the validation failure
func ValidateIdentifier(identifier string, identifierType string) error {
	// Length check
	if len(identifier) == 0 {
		return fmt.Errorf("%s cannot be empty", identifierType)
	}
	if len(identifier) > 255 {
		return fmt.Errorf("%s too long (%d characters, max 255): %s", identifierType, len(identifier), identifier)
	}

	// Format check: must be alphanumeric + underscore, starting with letter or underscore
	if !identifierRegex.MatchString(identifier) {
		return fmt.Errorf("%s contains invalid characters (only alphanumeric and underscore allowed, must start with letter or underscore): %s", identifierType, identifier)
	}

	// Reserved word check removed: Since we always escape identifiers with backticks,
	// reserved words are safe to use. This allows databases named "test", "data", etc.

	return nil
}

// EscapeIdentifier properly escapes a ClickHouse identifier by:
// 1. Doubling any backticks in the identifier (ClickHouse backtick escape sequence)
// 2. Wrapping the result in backticks
//
// This provides defense-in-depth even when ValidateIdentifier is used,
// as it handles edge cases where backticks might be present in valid identifiers.
//
// Example:
//   - Input: "my_table"     -> Output: "`my_table`"
//   - Input: "my`table"     -> Output: "`my“table`"
//   - Input: "table“name"  -> Output: "`table````name`"
//
// Note: This should ALWAYS be used in combination with ValidateIdentifier, not as a replacement.
func EscapeIdentifier(identifier string) string {
	// Escape backticks by doubling them (ClickHouse escape sequence)
	escaped := strings.ReplaceAll(identifier, "`", "``")
	// Wrap in backticks
	return fmt.Sprintf("`%s`", escaped)
}

// ValidateAndEscapeIdentifier combines validation and escaping in a single operation.
// This is the recommended function to use for all SQL identifier interpolation.
//
// Parameters:
//   - identifier: The identifier to validate and escape
//   - identifierType: Human-readable type for error messages
//
// Returns:
//   - Escaped identifier ready for SQL interpolation (e.g., "`my_table`")
//   - Error if validation fails
func ValidateAndEscapeIdentifier(identifier string, identifierType string) (string, error) {
	if err := ValidateIdentifier(identifier, identifierType); err != nil {
		return "", err
	}
	return EscapeIdentifier(identifier), nil
}

// ValidateIdentifiers validates multiple identifiers at once.
// Returns an error on the first validation failure.
//
// Parameters:
//   - identifiers: Map of identifier name -> identifier value (e.g., {"table": "users", "column": "id"})
//
// Returns:
//   - nil if all identifiers are valid
//   - error on first validation failure
func ValidateIdentifiers(identifiers map[string]string) error {
	for identifierType, identifier := range identifiers {
		if err := ValidateIdentifier(identifier, identifierType); err != nil {
			return err
		}
	}
	return nil
}
