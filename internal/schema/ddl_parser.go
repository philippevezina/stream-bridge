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
	Type       DDLType           `json:"type"`
	Database   string            `json:"database"`
	Table      string            `json:"table"`
	Columns    []DDLColumn       `json:"columns,omitempty"`
	Operations []DDLOperation    `json:"operations,omitempty"`
	Engine     string            `json:"engine,omitempty"`
	Options    map[string]string `json:"options,omitempty"`
	RawSQL     string            `json:"raw_sql"`
}

type DDLType string

const (
	DDLTypeCreateTable DDLType = "CREATE_TABLE"
	DDLTypeAlterTable  DDLType = "ALTER_TABLE"
	DDLTypeDropTable   DDLType = "DROP_TABLE"
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
	DDLActionAddIndex     DDLAction = "ADD_INDEX"
	DDLActionDropIndex    DDLAction = "DROP_INDEX"
	DDLActionRename       DDLAction = "RENAME"
)

var (
	createTableRegex = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)" + `\s*\((.+)\)(?:\s*ENGINE\s*=\s*(\w+))?`)
	alterTableRegex  = regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\s+(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)" + `\s+([\s\S]+)`)
	dropTableRegex   = regexp.MustCompile(`(?i)^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(?:(\w+)|` + "`([^`]+)`)" + `\.)?(?:(\w+)|` + "`([^`]+)`)" + `(?:\s*/\*.*?\*/\s*)?$`)

	addColumnRegex    = regexp.MustCompile(`(?i)ADD\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+([\s\S]*?)(?:\s+AFTER\s+(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `))?(?:\s+FIRST)?\s*$`)
	dropColumnRegex   = regexp.MustCompile(`(?i)DROP\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)`)
	modifyColumnRegex = regexp.MustCompile(`(?i)MODIFY\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+([\s\S]*?)\s*$`)
	changeColumnRegex = regexp.MustCompile(`(?i)CHANGE\s+(?:COLUMN\s+)?(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+(?:(\w+)|` + "`" + `([^` + "`" + `]+)` + "`" + `)\s+([\s\S]*?)\s*$`)

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
						p.logger.Warn("Failed to parse column definition",
							zap.String("column_def", colDef),
							zap.Error(err))
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
			p.logger.Warn("Failed to parse final column definition",
				zap.String("column_def", colDef),
				zap.Error(err))
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
		column.DefaultValue = strings.Trim(matches[4], "'\"")
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

	if strings.Contains(upperOp, "RENAME") && strings.Contains(upperOp, "TO") {
		return &DDLOperation{
			Action: DDLActionRename,
		}, nil
	}

	p.logger.Debug("Unhandled alter operation", zap.String("operation", operation))
	return nil, nil
}

func (p *DDLParser) IsSupported(ddlSQL string) bool {
	ddlSQL = strings.TrimSpace(strings.ToUpper(ddlSQL))

	supportedPrefixes := []string{
		"CREATE TABLE",
		"ALTER TABLE",
		"DROP TABLE",
	}

	for _, prefix := range supportedPrefixes {
		if strings.HasPrefix(ddlSQL, prefix) {
			return true
		}
	}

	return false
}
