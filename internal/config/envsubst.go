package config

import (
	"fmt"
	"os"
	"regexp"
)

// envVarPattern matches environment variable references in the following formats:
// - ${VAR} - standard syntax
// - $VAR - shorthand syntax (word characters only)
// - ${VAR:-default} - with default value if unset or empty
// - ${VAR:?error message} - required variable with error message
var envVarPattern = regexp.MustCompile(`\$\{([a-zA-Z_][a-zA-Z0-9_]*)(?:(:[-?])([^}]*))?\}|\$([a-zA-Z_][a-zA-Z0-9_]*)`)

// expandEnvWithDefaults expands environment variables in the input string.
// It supports the following syntax:
//   - ${VAR} or $VAR: Substitute with the value of VAR (empty string if unset)
//   - ${VAR:-default}: Use "default" if VAR is unset or empty
//   - ${VAR:?error message}: Fail with error if VAR is unset or empty
func expandEnvWithDefaults(input string) (string, error) {
	var expansionErr error

	result := envVarPattern.ReplaceAllStringFunc(input, func(match string) string {
		// If we already have an error, skip further processing
		if expansionErr != nil {
			return match
		}

		submatches := envVarPattern.FindStringSubmatch(match)
		if submatches == nil {
			return match
		}

		// submatches[1] = variable name for ${VAR...} syntax
		// submatches[2] = operator (":- " or ":?")
		// submatches[3] = default value or error message
		// submatches[4] = variable name for $VAR syntax

		var varName string
		var operator string
		var operand string

		if submatches[4] != "" {
			// $VAR syntax (shorthand)
			varName = submatches[4]
		} else {
			// ${VAR...} syntax
			varName = submatches[1]
			operator = submatches[2]
			operand = submatches[3]
		}

		value := os.Getenv(varName)

		switch operator {
		case ":-":
			// Use default if unset or empty
			if value == "" {
				return operand
			}
			return value
		case ":?":
			// Require variable to be set and non-empty
			if value == "" {
				if operand != "" {
					expansionErr = fmt.Errorf("environment variable %s is required: %s", varName, operand)
				} else {
					expansionErr = fmt.Errorf("environment variable %s is required but not set", varName)
				}
				return match
			}
			return value
		default:
			// Standard substitution (empty string if unset)
			return value
		}
	})

	if expansionErr != nil {
		return "", expansionErr
	}

	return result, nil
}
