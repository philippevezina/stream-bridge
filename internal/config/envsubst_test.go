package config

import (
	"os"
	"strings"
	"testing"
)

func TestExpandEnvWithDefaults(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		envVars     map[string]string
		expected    string
		expectError bool
		errorMsg    string
	}{
		{
			name:     "standard ${VAR} expansion",
			input:    "host: ${TEST_HOST}",
			envVars:  map[string]string{"TEST_HOST": "localhost"},
			expected: "host: localhost",
		},
		{
			name:     "shorthand $VAR expansion",
			input:    "host: $TEST_HOST",
			envVars:  map[string]string{"TEST_HOST": "localhost"},
			expected: "host: localhost",
		},
		{
			name:     "unset variable expands to empty string",
			input:    "host: ${UNSET_VAR}",
			envVars:  map[string]string{},
			expected: "host: ",
		},
		{
			name:     "default value when var is unset",
			input:    "host: ${TEST_HOST:-localhost}",
			envVars:  map[string]string{},
			expected: "host: localhost",
		},
		{
			name:     "default value when var is empty",
			input:    "host: ${TEST_HOST:-localhost}",
			envVars:  map[string]string{"TEST_HOST": ""},
			expected: "host: localhost",
		},
		{
			name:     "default value not used when var is set",
			input:    "host: ${TEST_HOST:-localhost}",
			envVars:  map[string]string{"TEST_HOST": "remotehost"},
			expected: "host: remotehost",
		},
		{
			name:     "empty default value",
			input:    "dsn: ${SENTRY_DSN:-}",
			envVars:  map[string]string{},
			expected: "dsn: ",
		},
		{
			name:     "required variable when set",
			input:    "password: ${DB_PASSWORD:?Database password is required}",
			envVars:  map[string]string{"DB_PASSWORD": "secret123"},
			expected: "password: secret123",
		},
		{
			name:        "required variable when unset with message",
			input:       "password: ${DB_PASSWORD:?Database password is required}",
			envVars:     map[string]string{},
			expectError: true,
			errorMsg:    "Database password is required",
		},
		{
			name:        "required variable when empty with message",
			input:       "password: ${DB_PASSWORD:?Database password is required}",
			envVars:     map[string]string{"DB_PASSWORD": ""},
			expectError: true,
			errorMsg:    "Database password is required",
		},
		{
			name:        "required variable when unset without message",
			input:       "password: ${DB_PASSWORD:?}",
			envVars:     map[string]string{},
			expectError: true,
			errorMsg:    "required but not set",
		},
		{
			name:  "multiple variables in one string",
			input: "mysql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}",
			envVars: map[string]string{
				"DB_USER": "admin",
				"DB_PASS": "secret",
				"DB_HOST": "localhost",
				"DB_PORT": "3306",
				"DB_NAME": "mydb",
			},
			expected: "mysql://admin:secret@localhost:3306/mydb",
		},
		{
			name:     "no variables passthrough",
			input:    "host: localhost\nport: 3306",
			envVars:  map[string]string{},
			expected: "host: localhost\nport: 3306",
		},
		{
			name:     "mixed variables and text",
			input:    "Connection to ${HOST} on port 3306",
			envVars:  map[string]string{"HOST": "myserver"},
			expected: "Connection to myserver on port 3306",
		},
		{
			name:     "variable with underscore in name",
			input:    "value: ${MY_LONG_VAR_NAME}",
			envVars:  map[string]string{"MY_LONG_VAR_NAME": "test"},
			expected: "value: test",
		},
		{
			name:     "variable with numbers in name",
			input:    "value: ${VAR123}",
			envVars:  map[string]string{"VAR123": "test123"},
			expected: "value: test123",
		},
		{
			name:     "default value with special characters",
			input:    "url: ${URL:-https://example.com/path?query=1&other=2}",
			envVars:  map[string]string{},
			expected: "url: https://example.com/path?query=1&other=2",
		},
		{
			name:     "default value with spaces",
			input:    "message: ${MSG:-Hello World}",
			envVars:  map[string]string{},
			expected: "message: Hello World",
		},
		{
			name: "yaml config with env vars",
			input: `mysql:
  host: "${MYSQL_HOST:-localhost}"
  port: 3306
  username: "${MYSQL_USER:-root}"
  password: "${MYSQL_PASSWORD}"`,
			envVars: map[string]string{
				"MYSQL_HOST":     "db.example.com",
				"MYSQL_PASSWORD": "secret",
			},
			expected: `mysql:
  host: "db.example.com"
  port: 3306
  username: "root"
  password: "secret"`,
		},
		{
			name:     "shorthand var at end of line",
			input:    "host: $HOST",
			envVars:  map[string]string{"HOST": "myhost"},
			expected: "host: myhost",
		},
		{
			name:     "multiple shorthand vars",
			input:    "$USER@$HOST",
			envVars:  map[string]string{"USER": "admin", "HOST": "server"},
			expected: "admin@server",
		},
		{
			name:     "adjacent variables",
			input:    "${PREFIX}${SUFFIX}",
			envVars:  map[string]string{"PREFIX": "hello", "SUFFIX": "world"},
			expected: "helloworld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all test environment variables first
			for key := range tt.envVars {
				os.Unsetenv(key)
			}
			// Also clear commonly used test variables
			os.Unsetenv("TEST_HOST")
			os.Unsetenv("UNSET_VAR")
			os.Unsetenv("DB_PASSWORD")
			os.Unsetenv("SENTRY_DSN")

			// Set environment variables for this test
			for key, value := range tt.envVars {
				os.Setenv(key, value)
			}

			result, err := expandEnvWithDefaults(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}

			// Clean up environment variables
			for key := range tt.envVars {
				os.Unsetenv(key)
			}
		})
	}
}

func TestExpandEnvWithDefaults_MultipleRequiredErrors(t *testing.T) {
	// When multiple required variables are missing, only the first error is returned
	os.Unsetenv("VAR1")
	os.Unsetenv("VAR2")

	input := "${VAR1:?first error}${VAR2:?second error}"
	_, err := expandEnvWithDefaults(input)

	if err == nil {
		t.Error("expected error but got none")
		return
	}

	// Should contain the first error
	if !strings.Contains(err.Error(), "VAR1") {
		t.Errorf("expected error for VAR1, got: %v", err)
	}
}

func TestExpandEnvWithDefaults_PartialMatch(t *testing.T) {
	// Test that partial matches don't incorrectly expand
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "dollar sign without valid var name",
			input:    "price: $100",
			expected: "price: $100",
		},
		{
			name:     "unclosed brace",
			input:    "value: ${UNCLOSED",
			expected: "value: ${UNCLOSED",
		},
		{
			name:     "empty braces",
			input:    "value: ${}",
			expected: "value: ${}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := expandEnvWithDefaults(tt.input)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}
