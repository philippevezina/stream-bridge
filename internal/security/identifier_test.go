package security

import (
	"strings"
	"testing"
)

func TestValidateIdentifier(t *testing.T) {
	tests := []struct {
		name           string
		identifier     string
		identifierType string
		wantErr        bool
		errContains    string
	}{
		// Valid identifiers
		{
			name:           "valid simple name",
			identifier:     "users",
			identifierType: "table name",
			wantErr:        false,
		},
		{
			name:           "valid with underscores",
			identifier:     "user_accounts",
			identifierType: "table name",
			wantErr:        false,
		},
		{
			name:           "valid with numbers",
			identifier:     "table123",
			identifierType: "table name",
			wantErr:        false,
		},
		{
			name:           "valid starting with underscore",
			identifier:     "_internal",
			identifierType: "table name",
			wantErr:        false,
		},
		{
			name:           "valid mixed case",
			identifier:     "MyTable_123",
			identifierType: "table name",
			wantErr:        false,
		},
		{
			name:           "valid max length (255 chars)",
			identifier:     strings.Repeat("a", 255),
			identifierType: "table name",
			wantErr:        false,
		},

		// Invalid: Empty
		{
			name:           "empty identifier",
			identifier:     "",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "cannot be empty",
		},

		// Invalid: Too long
		{
			name:           "too long (256 chars)",
			identifier:     strings.Repeat("a", 256),
			identifierType: "table name",
			wantErr:        true,
			errContains:    "too long",
		},

		// Invalid: Starting with number
		{
			name:           "starts with number",
			identifier:     "123table",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},

		// Invalid: Special characters
		{
			name:           "contains hyphen",
			identifier:     "user-table",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "contains space",
			identifier:     "user table",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "contains dot",
			identifier:     "db.table",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "contains backtick",
			identifier:     "table`name",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "contains semicolon",
			identifier:     "table;DROP",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},

		// SQL Injection attempts
		{
			name:           "SQL injection with backtick escape",
			identifier:     "table` DROP TABLE users--",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "SQL injection with comment",
			identifier:     "table--comment",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "SQL injection with UNION",
			identifier:     "table UNION SELECT",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "SQL injection with quotes",
			identifier:     "table'OR'1'='1",
			identifierType: "table name",
			wantErr:        true,
			errContains:    "invalid characters",
		},

		// Reserved words are now ALLOWED because we always escape with backticks
		{
			name:           "reserved word SELECT (now allowed)",
			identifier:     "SELECT",
			identifierType: "table name",
			wantErr:        false,
		},
		{
			name:           "reserved word select lowercase (now allowed)",
			identifier:     "select",
			identifierType: "column name",
			wantErr:        false,
		},
		{
			name:           "reserved word TABLE (now allowed)",
			identifier:     "TABLE",
			identifierType: "column name",
			wantErr:        false,
		},
		{
			name:           "reserved word test (common database name)",
			identifier:     "test",
			identifierType: "database name",
			wantErr:        false,
		},
		{
			name:           "reserved word data (common database name)",
			identifier:     "data",
			identifierType: "database name",
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifier(tt.identifier, tt.identifierType)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateIdentifier() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateIdentifier() error = %v, expected to contain %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateIdentifier() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestEscapeIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		identifier string
		want       string
	}{
		{
			name:       "simple identifier",
			identifier: "users",
			want:       "`users`",
		},
		{
			name:       "identifier with underscore",
			identifier: "user_accounts",
			want:       "`user_accounts`",
		},
		{
			name:       "identifier with numbers",
			identifier: "table123",
			want:       "`table123`",
		},
		{
			name:       "identifier with single backtick",
			identifier: "my`table",
			want:       "`my``table`",
		},
		{
			name:       "identifier with double backticks",
			identifier: "my``table",
			want:       "`my````table`",
		},
		{
			name:       "identifier with backtick at start",
			identifier: "`table",
			want:       "```table`",
		},
		{
			name:       "identifier with backtick at end",
			identifier: "table`",
			want:       "`table```",
		},
		{
			name:       "identifier with multiple backticks",
			identifier: "a`b`c`d",
			want:       "`a``b``c``d`",
		},
		{
			name:       "empty identifier (edge case)",
			identifier: "",
			want:       "``",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EscapeIdentifier(tt.identifier)
			if got != tt.want {
				t.Errorf("EscapeIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateAndEscapeIdentifier(t *testing.T) {
	tests := []struct {
		name           string
		identifier     string
		identifierType string
		want           string
		wantErr        bool
		errContains    string
	}{
		{
			name:           "valid simple identifier",
			identifier:     "users",
			identifierType: "table name",
			want:           "`users`",
			wantErr:        false,
		},
		{
			name:           "valid with underscores",
			identifier:     "user_accounts",
			identifierType: "table name",
			want:           "`user_accounts`",
			wantErr:        false,
		},
		{
			name:           "invalid: contains space",
			identifier:     "user table",
			identifierType: "table name",
			want:           "",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "invalid: SQL injection attempt",
			identifier:     "table` DROP TABLE users--",
			identifierType: "table name",
			want:           "",
			wantErr:        true,
			errContains:    "invalid characters",
		},
		{
			name:           "valid: reserved word SELECT (now allowed with escaping)",
			identifier:     "SELECT",
			identifierType: "table name",
			want:           "`SELECT`",
			wantErr:        false,
		},
		{
			name:           "invalid: empty",
			identifier:     "",
			identifierType: "table name",
			want:           "",
			wantErr:        true,
			errContains:    "cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateAndEscapeIdentifier(tt.identifier, tt.identifierType)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateAndEscapeIdentifier() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateAndEscapeIdentifier() error = %v, expected to contain %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateAndEscapeIdentifier() unexpected error = %v", err)
					return
				}
				if got != tt.want {
					t.Errorf("ValidateAndEscapeIdentifier() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestValidateIdentifiers(t *testing.T) {
	tests := []struct {
		name        string
		identifiers map[string]string
		wantErr     bool
		errContains string
	}{
		{
			name: "all valid identifiers",
			identifiers: map[string]string{
				"database": "mydb",
				"table":    "users",
				"column":   "user_id",
			},
			wantErr: false,
		},
		{
			name: "one invalid identifier",
			identifiers: map[string]string{
				"database": "mydb",
				"table":    "users",
				"column":   "user-id", // Invalid: contains hyphen
			},
			wantErr:     true,
			errContains: "invalid characters",
		},
		{
			name: "reserved word in identifiers (now allowed)",
			identifiers: map[string]string{
				"database": "test",   // Reserved word, now allowed
				"table":    "SELECT", // Reserved word, now allowed
				"column":   "id",
			},
			wantErr: false,
		},
		{
			name: "empty identifier",
			identifiers: map[string]string{
				"database": "mydb",
				"table":    "",
				"column":   "id",
			},
			wantErr:     true,
			errContains: "cannot be empty",
		},
		{
			name:        "empty map",
			identifiers: map[string]string{},
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateIdentifiers(tt.identifiers)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateIdentifiers() expected error but got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("ValidateIdentifiers() error = %v, expected to contain %q", err, tt.errContains)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateIdentifiers() unexpected error = %v", err)
				}
			}
		})
	}
}

// Benchmark tests
func BenchmarkValidateIdentifier(b *testing.B) {
	identifier := "user_accounts_table_123"
	identifierType := "table name"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateIdentifier(identifier, identifierType)
	}
}

func BenchmarkEscapeIdentifier(b *testing.B) {
	identifier := "user_accounts_table"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EscapeIdentifier(identifier)
	}
}

func BenchmarkValidateAndEscapeIdentifier(b *testing.B) {
	identifier := "user_accounts_table"
	identifierType := "table name"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = ValidateAndEscapeIdentifier(identifier, identifierType)
	}
}
