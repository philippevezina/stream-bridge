package schema

import (
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

type DDLParser struct {
	logger *zap.Logger
}

type DDLStatement struct {
	Type        DDLType           `json:"type"`
	Database    string            `json:"database"`
	Table       string            `json:"table"`
	Columns     []DDLColumn       `json:"columns,omitempty"`
	Operations  []DDLOperation    `json:"operations,omitempty"`
	RenamePairs []RenamePair      `json:"rename_pairs,omitempty"`
	Engine      string            `json:"engine,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
	RawSQL      string            `json:"raw_sql"`
}

type RenamePair struct {
	FromDatabase string `json:"from_database"`
	FromTable    string `json:"from_table"`
	ToDatabase   string `json:"to_database"`
	ToTable      string `json:"to_table"`
}

type DDLType string

const (
	DDLTypeCreateTable DDLType = "CREATE_TABLE"
	DDLTypeAlterTable  DDLType = "ALTER_TABLE"
	DDLTypeDropTable   DDLType = "DROP_TABLE"
	DDLTypeRenameTable DDLType = "RENAME_TABLE"
	DDLTypeCreateIndex DDLType = "CREATE_INDEX"
	DDLTypeDropIndex   DDLType = "DROP_INDEX"
	DDLTypeUnknown     DDLType = "UNKNOWN"
)

type DDLColumn struct {
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	Nullable     bool              `json:"nullable"`
	DefaultValue string            `json:"default_value"`
	Comment      string            `json:"comment"`
	Attributes   map[string]string `json:"attributes"`
}

type DDLOperation struct {
	Action    DDLAction         `json:"action"`
	Column    *DDLColumn        `json:"column,omitempty"`
	OldName   string            `json:"old_name,omitempty"`
	NewName   string            `json:"new_name,omitempty"`
	IndexName string            `json:"index_name,omitempty"`
	Options   map[string]string `json:"options,omitempty"`
}

type DDLAction string

const (
	DDLActionAddColumn    DDLAction = "ADD_COLUMN"
	DDLActionDropColumn   DDLAction = "DROP_COLUMN"
	DDLActionModifyColumn DDLAction = "MODIFY_COLUMN"
	DDLActionChangeColumn DDLAction = "CHANGE_COLUMN"
	DDLActionRenameColumn DDLAction = "RENAME_COLUMN"
	DDLActionAddIndex     DDLAction = "ADD_INDEX"
	DDLActionDropIndex    DDLAction = "DROP_INDEX"
	DDLActionRename       DDLAction = "RENAME"
)

var (
	createTableRegex = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)" + `\s*\(([\s\S]+)\)(?:\s*ENGINE\s*=\s*(\w+))?`)
	alterTableRegex  = regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)" + `\s+([\s\S]+)`)
	dropTableRegex   = regexp.MustCompile(`(?i)^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)" + `(?:\s*/\*.*?\*/\s*)?$`)
	renameTableRegex = regexp.MustCompile(`(?i)^\s*RENAME\s+TABLE\s+([\s\S]+)`)
	renamePairRegex  = regexp.MustCompile(`(?i)(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)" + `\s+TO\s+(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)")

	addColumnRegex    = regexp.MustCompile(`(?i)ADD\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+([\s\S]*?)(?:\s+AFTER\s+(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `))?(?:\s+FIRST)?\s*$`)
	dropColumnRegex   = regexp.MustCompile(`(?i)DROP\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)`)
	modifyColumnRegex = regexp.MustCompile(`(?i)MODIFY\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+([\s\S]*?)\s*$`)
	changeColumnRegex = regexp.MustCompile(`(?i)CHANGE\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+([\s\S]*?)\s*$`)
	renameColumnRegex = regexp.MustCompile(`(?i)RENAME\s+COLUMN\s+(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+TO\s+(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s*$`)

	columnDefRegex = regexp.MustCompile(`(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+([\s\S]+?)(?:\s+DEFAULT\s+([^,\)]+?))?(?:\s+COMMENT\s+'([^']*)')?$`)
)

func NewDDLParser(logger *zap.Logger) *DDLParser {
	return &DDLParser{
		logger: logger,
	}
}

func (p *DDLParser) Parse(ddlSQL string) (*DDLStatement, error) {
	ddlSQL = strings.TrimSpace(ddlSQL)
	if ddlSQL == "" {
		return nil, fmt.Errorf("empty DDL statement")
	}

	stmt := &DDLStatement{
		RawSQL:  ddlSQL,
		Options: make(map[string]string),
	}

	if matches := createTableRegex.FindStringSubmatch(ddlSQL); matches != nil {
		return p.parseCreateTable(stmt, matches)
	}

	if matches := alterTableRegex.FindStringSubmatch(ddlSQL); matches != nil {
		return p.parseAlterTable(stmt, matches)
	}

	if matches := dropTableRegex.FindStringSubmatch(ddlSQL); matches != nil {
		return p.parseDropTable(stmt, matches)
	}

	if matches := renameTableRegex.FindStringSubmatch(ddlSQL); matches != nil {
		return p.parseRenameTable(stmt, matches[1])
	}

	p.logger.Warn("Unknown DDL statement type", zap.String("sql", ddlSQL))
	stmt.Type = DDLTypeUnknown
	return stmt, nil
}

func (p *DDLParser) parseCreateTable(stmt *DDLStatement, matches []string) (*DDLStatement, error) {
	stmt.Type = DDLTypeCreateTable

	// Database: group 1 (unquoted) or group 2 (quoted)
	if matches[1] != "" {
		stmt.Database = matches[1]
	} else if matches[2] != "" {
		stmt.Database = matches[2]
	}

	// Table: group 3 (unquoted) or group 4 (quoted)
	if matches[3] != "" {
		stmt.Table = matches[3]
	} else if matches[4] != "" {
		stmt.Table = matches[4]
	}

	columnDefs := matches[5]
	stmt.Engine = matches[6]

	columns, err := p.parseColumnDefinitions(columnDefs)
	if err != nil {
		return nil, fmt.Errorf("failed to parse column definitions: %w", err)
	}

	stmt.Columns = columns

	p.logger.Debug("Parsed CREATE TABLE statement",
		zap.String("database", stmt.Database),
		zap.String("table", stmt.Table),
		zap.String("engine", stmt.Engine),
		zap.Int("column_count", len(stmt.Columns)))

	return stmt, nil
}

func (p *DDLParser) parseAlterTable(stmt *DDLStatement, matches []string) (*DDLStatement, error) {
	stmt.Type = DDLTypeAlterTable

	// Database: group 1 (unquoted) or group 2 (quoted)
	if matches[1] != "" {
		stmt.Database = matches[1]
	} else if matches[2] != "" {
		stmt.Database = matches[2]
	}

	// Table: group 3 (unquoted) or group 4 (quoted)
	if matches[3] != "" {
		stmt.Table = matches[3]
	} else if matches[4] != "" {
		stmt.Table = matches[4]
	}

	alterClause := matches[5]

	operations, err := p.parseAlterOperations(alterClause)
	if err != nil {
		return nil, fmt.Errorf("failed to parse alter operations: %w", err)
	}

	stmt.Operations = operations

	p.logger.Debug("Parsed ALTER TABLE statement",
		zap.String("database", stmt.Database),
		zap.String("table", stmt.Table),
		zap.Int("operation_count", len(stmt.Operations)))

	return stmt, nil
}

func (p *DDLParser) parseDropTable(stmt *DDLStatement, matches []string) (*DDLStatement, error) {
	stmt.Type = DDLTypeDropTable

	// Database: group 1 (unquoted) or group 2 (quoted)
	if matches[1] != "" {
		stmt.Database = matches[1]
	} else if matches[2] != "" {
		stmt.Database = matches[2]
	}

	// Table: group 3 (unquoted) or group 4 (quoted)
	if matches[3] != "" {
		stmt.Table = matches[3]
	} else if matches[4] != "" {
		stmt.Table = matches[4]
	}

	p.logger.Debug("Parsed DROP TABLE statement",
		zap.String("database", stmt.Database),
		zap.String("table", stmt.Table))

	return stmt, nil
}

func (p *DDLParser) parseRenameTable(stmt *DDLStatement, pairsClause string) (*DDLStatement, error) {
	stmt.Type = DDLTypeRenameTable

	pairMatches := renamePairRegex.FindAllStringSubmatch(pairsClause, -1)
	if len(pairMatches) == 0 {
		return nil, fmt.Errorf("no valid rename pairs found in RENAME TABLE statement")
	}

	for _, match := range pairMatches {
		fromDB := match[1]
		if fromDB == "" {
			fromDB = match[2]
		}
		fromTable := match[3]
		if fromTable == "" {
			fromTable = match[4]
		}
		toDB := match[5]
		if toDB == "" {
			toDB = match[6]
		}
		toTable := match[7]
		if toTable == "" {
			toTable = match[8]
		}

		stmt.RenamePairs = append(stmt.RenamePairs, RenamePair{
			FromDatabase: fromDB,
			FromTable:    fromTable,
			ToDatabase:   toDB,
			ToTable:      toTable,
		})
	}

	// Set Database/Table from first pair for compatibility with existing DDL flow
	if len(stmt.RenamePairs) > 0 {
		stmt.Database = stmt.RenamePairs[0].FromDatabase
		stmt.Table = stmt.RenamePairs[0].FromTable
	}

	p.logger.Debug("Parsed RENAME TABLE statement",
		zap.Int("pairs", len(stmt.RenamePairs)))

	return stmt, nil
}

func (p *DDLParser) parseColumnDefinitions(columnDefs string) ([]DDLColumn, error) {
	var columns []DDLColumn

	depth := 0
	start := 0

	for i, char := range columnDefs {
		switch char {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				colDef := strings.TrimSpace(columnDefs[start:i])
				if colDef != "" {
					column, err := p.parseColumnDefinition(colDef)
					if err != nil {
						if isConstraintDefinition(colDef) {
							p.logger.Debug("Skipping constraint/index definition",
								zap.String("column_def", colDef))
						} else {
							p.logger.Warn("Failed to parse column definition",
								zap.String("column_def", colDef),
								zap.Error(err))
						}
					} else {
						columns = append(columns, *column)
					}
				}
				start = i + 1
			}
		}
	}

	colDef := strings.TrimSpace(columnDefs[start:])
	if colDef != "" {
		column, err := p.parseColumnDefinition(colDef)
		if err != nil {
			if isConstraintDefinition(colDef) {
				p.logger.Debug("Skipping constraint/index definition",
					zap.String("column_def", colDef))
			} else {
				p.logger.Warn("Failed to parse final column definition",
					zap.String("column_def", colDef),
					zap.Error(err))
			}
		} else {
			columns = append(columns, *column)
		}
	}

	return columns, nil
}

func (p *DDLParser) parseColumnDefinition(colDef string) (*DDLColumn, error) {
	colDef = strings.TrimSpace(colDef)

	if strings.HasPrefix(strings.ToUpper(colDef), "PRIMARY KEY") ||
		strings.HasPrefix(strings.ToUpper(colDef), "KEY") ||
		strings.HasPrefix(strings.ToUpper(colDef), "INDEX") ||
		strings.HasPrefix(strings.ToUpper(colDef), "UNIQUE") ||
		strings.HasPrefix(strings.ToUpper(colDef), "FOREIGN KEY") ||
		strings.HasPrefix(strings.ToUpper(colDef), "CONSTRAINT") {
		return nil, fmt.Errorf("not a column definition: %s", colDef)
	}

	matches := columnDefRegex.FindStringSubmatch(colDef)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid column definition: %s", colDef)
	}

	// Extract column name from either unquoted (group 1) or backtick-quoted (group 2)
	columnName := matches[1]
	if columnName == "" {
		columnName = matches[2]
	}

	// Clean up the type by removing SQL attributes like NULL, DEFAULT, etc.
	rawType := strings.TrimSpace(matches[3])
	cleanType := p.extractBaseType(rawType)

	column := &DDLColumn{
		Name:       columnName,
		Type:       cleanType,
		Nullable:   !strings.Contains(strings.ToUpper(colDef), "NOT NULL"),
		Attributes: make(map[string]string),
	}

	if len(matches) > 4 && matches[4] != "" {
		column.DefaultValue = p.cleanDefaultValue(matches[4])
	}

	if len(matches) > 5 && matches[5] != "" {
		column.Comment = matches[5]
	}

	if strings.Contains(strings.ToUpper(colDef), "AUTO_INCREMENT") {
		column.Attributes["auto_increment"] = "true"
	}

	if strings.Contains(strings.ToUpper(colDef), "PRIMARY KEY") {
		column.Attributes["primary_key"] = "true"
	}

	return column, nil
}

// cleanDefaultValue extracts the actual default value from a captured string,
// removing any trailing SQL keywords (NOT NULL, NULL, etc.) that may have been
// captured by the regex.
func (p *DDLParser) cleanDefaultValue(rawDefault string) string {
	rawDefault = strings.TrimSpace(rawDefault)
	if rawDefault == "" {
		return ""
	}

	// If the value starts with a single quote, extract the quoted string
	if strings.HasPrefix(rawDefault, "'") {
		// Find the closing quote (handling escaped quotes)
		i := 1
		for i < len(rawDefault) {
			if rawDefault[i] == '\'' {
				// Check if it's an escaped quote ('')
				if i+1 < len(rawDefault) && rawDefault[i+1] == '\'' {
					i += 2
					continue
				}
				// Found the closing quote - return content without quotes
				return rawDefault[1:i]
			}
			i++
		}
		// No closing quote found, return as-is without leading quote
		return rawDefault[1:]
	}

	// If the value starts with a double quote, extract the quoted string
	if strings.HasPrefix(rawDefault, "\"") {
		// Find the closing quote
		i := 1
		for i < len(rawDefault) {
			if rawDefault[i] == '"' {
				// Check if it's an escaped quote
				if i+1 < len(rawDefault) && rawDefault[i+1] == '"' {
					i += 2
					continue
				}
				// Found the closing quote - return content without quotes
				return rawDefault[1:i]
			}
			i++
		}
		// No closing quote found, return as-is without leading quote
		return rawDefault[1:]
	}

	// For unquoted values, take only the first token (stop at whitespace)
	// This handles cases like "0 NOT NULL" -> "0"
	parts := strings.Fields(rawDefault)
	if len(parts) > 0 {
		return parts[0]
	}

	return rawDefault
}

func (p *DDLParser) extractBaseType(rawType string) string {
	// Remove common SQL attributes from the type string
	typeStr := strings.TrimSpace(rawType)

	// Split on whitespace and take the first part (the actual type)
	parts := strings.Fields(typeStr)
	if len(parts) == 0 {
		return typeStr
	}

	baseType := parts[0]

	// Handle parentheses for types like VARCHAR(255), DECIMAL(10,2)
	for i := 1; i < len(parts); i++ {
		part := strings.ToUpper(parts[i])
		// Stop when we hit SQL keywords
		if part == "NULL" || part == "NOT" || part == "DEFAULT" ||
			part == "AUTO_INCREMENT" || part == "PRIMARY" ||
			part == "UNIQUE" || part == "COMMENT" {
			break
		}
		// Include parentheses and size specifiers
		if strings.Contains(parts[i], "(") || strings.Contains(parts[i], ")") {
			baseType += " " + parts[i]
		}
	}

	return baseType
}

func (p *DDLParser) parseAlterOperations(alterClause string) ([]DDLOperation, error) {
	var operations []DDLOperation

	parts := p.splitAlterOperations(alterClause)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		operation, err := p.parseAlterOperation(part)
		if err != nil {
			p.logger.Warn("Failed to parse alter operation",
				zap.String("operation", part),
				zap.Error(err))
			continue
		}

		if operation != nil {
			operations = append(operations, *operation)
		}
	}

	return operations, nil
}

func (p *DDLParser) splitAlterOperations(alterClause string) []string {
	var parts []string
	depth := 0
	start := 0

	for i, char := range alterClause {
		switch char {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, alterClause[start:i])
				start = i + 1
			}
		}
	}

	parts = append(parts, alterClause[start:])
	return parts
}

func (p *DDLParser) parseAlterOperation(operation string) (*DDLOperation, error) {
	operation = strings.TrimSpace(operation)
	upperOp := strings.ToUpper(operation)

	// Skip index/key operations - not relevant to ClickHouse
	if isIndexOperation(upperOp) {
		p.logger.Debug("Skipping index/key operation", zap.String("operation", operation))
		return nil, nil
	}

	if matches := addColumnRegex.FindStringSubmatch(operation); matches != nil {
		// Extract column name from either unquoted (group 1) or backtick-quoted (group 2)
		columnName := matches[1]
		if columnName == "" {
			columnName = matches[2]
		}

		// Group 3 contains the column definition (potentially multiline)
		columnDef := strings.TrimSpace(matches[3])

		column, err := p.parseColumnDefinition(fmt.Sprintf("%s %s", columnName, columnDef))
		if err != nil {
			return nil, err
		}
		return &DDLOperation{
			Action: DDLActionAddColumn,
			Column: column,
		}, nil
	}

	if matches := dropColumnRegex.FindStringSubmatch(operation); matches != nil {
		// Extract column name from either unquoted (group 1) or backtick-quoted (group 2)
		columnName := matches[1]
		if columnName == "" {
			columnName = matches[2]
		}

		return &DDLOperation{
			Action: DDLActionDropColumn,
			Column: &DDLColumn{Name: columnName},
		}, nil
	}

	if matches := modifyColumnRegex.FindStringSubmatch(operation); matches != nil {
		// Extract column name from either unquoted (group 1) or backtick-quoted (group 2)
		columnName := matches[1]
		if columnName == "" {
			columnName = matches[2]
		}

		// Group 3 contains the column definition (potentially multiline)
		columnDef := strings.TrimSpace(matches[3])

		column, err := p.parseColumnDefinition(fmt.Sprintf("%s %s", columnName, columnDef))
		if err != nil {
			return nil, err
		}
		return &DDLOperation{
			Action: DDLActionModifyColumn,
			Column: column,
		}, nil
	}

	if matches := changeColumnRegex.FindStringSubmatch(operation); matches != nil {
		// Extract old column name from either unquoted (group 1) or backtick-quoted (group 2)
		oldColumnName := matches[1]
		if oldColumnName == "" {
			oldColumnName = matches[2]
		}

		// Extract new column name from either unquoted (group 3) or backtick-quoted (group 4)
		newColumnName := matches[3]
		if newColumnName == "" {
			newColumnName = matches[4]
		}

		// Group 5 contains the column definition (potentially multiline)
		columnDef := strings.TrimSpace(matches[5])

		column, err := p.parseColumnDefinition(fmt.Sprintf("%s %s", newColumnName, columnDef))
		if err != nil {
			return nil, err
		}
		return &DDLOperation{
			Action:  DDLActionChangeColumn,
			OldName: oldColumnName,
			Column:  column,
		}, nil
	}

	if matches := renameColumnRegex.FindStringSubmatch(operation); matches != nil {
		oldColumnName := matches[1]
		if oldColumnName == "" {
			oldColumnName = matches[2]
		}

		newColumnName := matches[3]
		if newColumnName == "" {
			newColumnName = matches[4]
		}

		return &DDLOperation{
			Action:  DDLActionRenameColumn,
			OldName: oldColumnName,
			NewName: newColumnName,
		}, nil
	}

	if strings.Contains(upperOp, "RENAME") && strings.Contains(upperOp, "TO") {
		return &DDLOperation{
			Action: DDLActionRename,
		}, nil
	}

	p.logger.Debug("Unhandled alter operation", zap.String("operation", operation))
	return nil, nil
}

// isConstraintDefinition checks if a column definition string is actually
// a constraint/index definition (PRIMARY KEY, KEY, INDEX, UNIQUE, etc.)
// which should be silently skipped during CREATE TABLE parsing.
func isConstraintDefinition(colDef string) bool {
	upper := strings.ToUpper(strings.TrimSpace(colDef))
	return strings.HasPrefix(upper, "PRIMARY KEY") ||
		strings.HasPrefix(upper, "KEY") ||
		strings.HasPrefix(upper, "INDEX") ||
		strings.HasPrefix(upper, "UNIQUE") ||
		strings.HasPrefix(upper, "FOREIGN KEY") ||
		strings.HasPrefix(upper, "CONSTRAINT")
}

// isIndexOperation checks if an ALTER TABLE operation is an index/key operation
// (ADD INDEX, ADD KEY, ADD UNIQUE, ADD PRIMARY KEY, DROP INDEX, DROP KEY, etc.)
// which should be silently skipped since ClickHouse doesn't use MySQL indexes.
func isIndexOperation(upperOp string) bool {
	prefixes := []string{
		"ADD INDEX",
		"ADD KEY",
		"ADD UNIQUE KEY",
		"ADD UNIQUE INDEX",
		"ADD PRIMARY KEY",
		"ADD FULLTEXT",
		"ADD SPATIAL",
		"DROP INDEX",
		"DROP KEY",
		"DROP PRIMARY KEY",
		"DROP FOREIGN KEY",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(upperOp, prefix) {
			return true
		}
	}
	return false
}

// IsRenameTable checks if a DDL statement is a RENAME TABLE statement
func (p *DDLParser) IsRenameTable(ddlSQL string) bool {
	return strings.HasPrefix(strings.TrimSpace(strings.ToUpper(ddlSQL)), "RENAME TABLE")
}

func (p *DDLParser) IsSupported(ddlSQL string) bool {
	ddlSQL = strings.TrimSpace(strings.ToUpper(ddlSQL))

	supportedPrefixes := []string{
		"CREATE TABLE",
		"ALTER TABLE",
		"DROP TABLE",
		"RENAME TABLE",
	}

	for _, prefix := range supportedPrefixes {
		if strings.HasPrefix(ddlSQL, prefix) {
			return true
		}
	}

	return false
}
