package schema

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
	"github.com/philippevezina/stream-bridge/internal/security"
)

type Translator struct {
	logger              *zap.Logger
	defaultEngine       clickhouse.TableEngine
	preserveNullability bool
	timestampPrecision  int
	defaultStringLength int
	customTypeMappings  map[string]string
}

type TranslationOptions struct {
	Engine              clickhouse.TableEngine `json:"engine"`
	PreserveNullability bool                   `json:"preserve_nullability"`
	TimestampPrecision  int                    `json:"timestamp_precision"`
	DefaultStringLength int                    `json:"default_string_length"`
	CustomTypeMappings  map[string]string      `json:"custom_type_mappings"`
}

type ClickHouseSchema struct {
	Database    string                 `json:"database"`
	Table       string                 `json:"table"`
	Columns     []ClickHouseColumn     `json:"columns"`
	Engine      clickhouse.TableEngine `json:"engine"`
	OrderBy     []string               `json:"order_by"`
	PartitionBy string                 `json:"partition_by,omitempty"`
	PrimaryKey  []string               `json:"primary_key,omitempty"`
	Settings    map[string]string      `json:"settings,omitempty"`
}

type ClickHouseColumn struct {
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	DefaultValue string            `json:"default_value,omitempty"`
	Codec        string            `json:"codec,omitempty"`
	Comment      string            `json:"comment,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

var (
	numericTypeRegex = regexp.MustCompile(`^(\w+)\((\d+)(?:,(\d+))?\)$`)
	charTypeRegex    = regexp.MustCompile(`^(?:VAR)?CHAR\((\d+)\)$`)
	decimalTypeRegex = regexp.MustCompile(`^DECIMAL\((\d+),(\d+)\)$`)
)

// validateAndGetEngine ensures only supported engines are used
func validateAndGetEngine(engine clickhouse.TableEngine) clickhouse.TableEngine {
	if engine == clickhouse.EngineReplacingMergeTree || engine == clickhouse.EngineReplicatedReplacingMergeTree {
		return engine
	}
	// Default to ReplacingMergeTree for any unsupported engine
	return clickhouse.EngineReplacingMergeTree
}

func NewTranslator(logger *zap.Logger, options *TranslationOptions) *Translator {
	if options == nil {
		options = &TranslationOptions{
			Engine:              clickhouse.EngineReplacingMergeTree,
			PreserveNullability: true,
			TimestampPrecision:  3,
			DefaultStringLength: 255,
			CustomTypeMappings:  make(map[string]string),
		}
	}

	return &Translator{
		logger:              logger,
		defaultEngine:       validateAndGetEngine(options.Engine),
		preserveNullability: options.PreserveNullability,
		timestampPrecision:  options.TimestampPrecision,
		defaultStringLength: options.DefaultStringLength,
		customTypeMappings:  options.CustomTypeMappings,
	}
}

func (t *Translator) TranslateTable(mysqlSchema *common.TableInfo, targetEngine clickhouse.TableEngine, targetDatabase string) (*ClickHouseSchema, error) {
	if mysqlSchema == nil {
		return nil, fmt.Errorf("mysql schema cannot be nil")
	}
	if targetDatabase == "" {
		return nil, fmt.Errorf("target database cannot be empty")
	}

	chSchema := &ClickHouseSchema{
		Database: targetDatabase,
		Table:    mysqlSchema.Name,
		Engine:   targetEngine,
		Settings: make(map[string]string),
	}

	var primaryKeys []string
	var orderByColumns []string

	for _, columnName := range mysqlSchema.ColumnOrder {
		mysqlCol, exists := mysqlSchema.Columns[columnName]
		if !exists {
			continue
		}

		chCol, err := t.translateColumn(mysqlCol)
		if err != nil {
			t.logger.Warn("Failed to translate column, using String fallback",
				zap.String("column", mysqlCol.Name),
				zap.String("mysql_type", mysqlCol.Type),
				zap.Error(err))

			chCol = &ClickHouseColumn{
				Name: mysqlCol.Name,
				Type: "String",
			}
		}

		if mysqlCol.IsPrimaryKey {
			primaryKeys = append(primaryKeys, mysqlCol.Name)
		}

		chSchema.Columns = append(chSchema.Columns, *chCol)
	}

	// Add required columns for ReplacingMergeTree engines
	switch targetEngine {
	case clickhouse.EngineReplacingMergeTree, clickhouse.EngineReplicatedReplacingMergeTree:
		// Check if _is_deleted column already exists
		hasIsDeleted := false
		hasVersion := false
		for _, col := range chSchema.Columns {
			if col.Name == "_is_deleted" {
				hasIsDeleted = true
			}
			if col.Name == "_version" {
				hasVersion = true
			}
		}

		// Add _is_deleted column if missing (for soft deletes)
		if !hasIsDeleted {
			chSchema.Columns = append(chSchema.Columns, ClickHouseColumn{
				Name:         "_is_deleted",
				Type:         "UInt8",
				DefaultValue: "0",
			})
		}

		// Add _version column if missing (for ReplacingMergeTree version tracking)
		if !hasVersion {
			chSchema.Columns = append(chSchema.Columns, ClickHouseColumn{
				Name:         "_version",
				Type:         "UInt64",
				DefaultValue: "0",
			})
		}

		chSchema.Settings["allow_nullable_key"] = "1"
		if len(primaryKeys) > 0 {
			chSchema.Settings["index_granularity"] = "8192"
		}
	default:
		return nil, fmt.Errorf("unsupported table engine: %s. Only ReplacingMergeTree and ReplicatedReplacingMergeTree are supported", targetEngine)
	}

	chSchema.PrimaryKey = primaryKeys

	if len(primaryKeys) > 0 {
		orderByColumns = primaryKeys
	} else {
		for _, col := range chSchema.Columns {
			if !strings.Contains(col.Type, "Array") &&
				!strings.Contains(col.Type, "Map") &&
				!strings.Contains(col.Type, "JSON") {
				orderByColumns = append(orderByColumns, col.Name)
				break
			}
		}
	}

	chSchema.OrderBy = orderByColumns

	t.logger.Debug("Translated MySQL table to ClickHouse schema",
		zap.String("mysql_database", mysqlSchema.Database),
		zap.String("clickhouse_database", chSchema.Database),
		zap.String("table", chSchema.Table),
		zap.String("engine", string(chSchema.Engine)),
		zap.Int("column_count", len(chSchema.Columns)))

	return chSchema, nil
}

func (t *Translator) translateColumn(mysqlCol common.Column) (*ClickHouseColumn, error) {
	mysqlType := strings.ToUpper(strings.TrimSpace(mysqlCol.Type))

	if customType, exists := t.customTypeMappings[mysqlType]; exists {
		return &ClickHouseColumn{
			Name:         mysqlCol.Name,
			Type:         customType,
			DefaultValue: t.translateDefaultValue(mysqlCol.DefaultValue, customType),
		}, nil
	}

	clickhouseType, err := t.mapMySQLTypeToClickHouse(mysqlType)
	if err != nil {
		return nil, err
	}

	if t.preserveNullability && mysqlCol.Nullable && !mysqlCol.IsPrimaryKey {
		clickhouseType = fmt.Sprintf("Nullable(%s)", clickhouseType)
	}

	chCol := &ClickHouseColumn{
		Name:         mysqlCol.Name,
		Type:         clickhouseType,
		DefaultValue: t.translateDefaultValue(mysqlCol.DefaultValue, clickhouseType),
		Attributes:   make(map[string]string),
	}

	if mysqlCol.AutoIncrement {
		chCol.Attributes["auto_increment"] = "true"
	}

	return chCol, nil
}

func (t *Translator) mapMySQLTypeToClickHouse(mysqlType string) (string, error) {
	mysqlType = strings.ToUpper(mysqlType)

	switch {
	case strings.HasPrefix(mysqlType, "TINYINT(1)"):
		return "UInt8", nil
	case strings.HasPrefix(mysqlType, "TINYINT"):
		return "Int8", nil
	case strings.HasPrefix(mysqlType, "SMALLINT"):
		return "Int16", nil
	case strings.HasPrefix(mysqlType, "MEDIUMINT"):
		return "Int32", nil
	case strings.HasPrefix(mysqlType, "BIGINT"):
		return "Int64", nil
	case strings.HasPrefix(mysqlType, "INT"):
		return "Int32", nil

	case strings.HasPrefix(mysqlType, "FLOAT"):
		return "Float32", nil
	case strings.HasPrefix(mysqlType, "DOUBLE"):
		return "Float64", nil

	case strings.HasPrefix(mysqlType, "DECIMAL"):
		return t.translateDecimalType(mysqlType)

	case strings.HasPrefix(mysqlType, "CHAR"):
		return t.translateCharType(mysqlType)
	case strings.HasPrefix(mysqlType, "VARCHAR"):
		return "String", nil
	case mysqlType == "TEXT":
		return "String", nil
	case mysqlType == "MEDIUMTEXT":
		return "String", nil
	case mysqlType == "LONGTEXT":
		return "String", nil

	case mysqlType == "DATE":
		return "Date", nil
	case strings.HasPrefix(mysqlType, "DATETIME"):
		return fmt.Sprintf("DateTime64(%d)", t.timestampPrecision), nil
	case strings.HasPrefix(mysqlType, "TIMESTAMP"):
		return fmt.Sprintf("DateTime64(%d)", t.timestampPrecision), nil
	case strings.HasPrefix(mysqlType, "TIME"):
		return "String", nil
	case strings.HasPrefix(mysqlType, "YEAR"):
		return "UInt16", nil

	case mysqlType == "JSON":
		return "String", nil
	case strings.HasPrefix(mysqlType, "ENUM"):
		return t.translateEnumType(mysqlType)
	case strings.HasPrefix(mysqlType, "SET"):
		return "Array(String)", nil

	case strings.HasPrefix(mysqlType, "BINARY"):
		return "FixedString(255)", nil
	case strings.HasPrefix(mysqlType, "VARBINARY"):
		return "String", nil
	case mysqlType == "BLOB":
		return "String", nil
	case mysqlType == "MEDIUMBLOB":
		return "String", nil
	case mysqlType == "LONGBLOB":
		return "String", nil

	case mysqlType == "BIT":
		return "UInt64", nil

	case mysqlType == "GEOMETRY":
		return "String", nil
	case mysqlType == "POINT":
		return "String", nil
	case mysqlType == "LINESTRING":
		return "String", nil
	case mysqlType == "POLYGON":
		return "String", nil

	default:
		t.logger.Warn("Unknown MySQL type, defaulting to String",
			zap.String("mysql_type", mysqlType))
		return "String", nil
	}
}

func (t *Translator) translateDecimalType(mysqlType string) (string, error) {
	matches := decimalTypeRegex.FindStringSubmatch(mysqlType)
	if len(matches) != 3 {
		return "Decimal64(4)", nil
	}

	precision, err := strconv.Atoi(matches[1])
	if err != nil {
		return "Decimal64(4)", nil
	}

	scale, err := strconv.Atoi(matches[2])
	if err != nil {
		return "Decimal64(4)", nil
	}

	switch {
	case precision <= 9:
		return fmt.Sprintf("Decimal32(%d)", scale), nil
	case precision <= 18:
		return fmt.Sprintf("Decimal64(%d)", scale), nil
	case precision <= 38:
		return fmt.Sprintf("Decimal128(%d)", scale), nil
	default:
		return fmt.Sprintf("Decimal256(%d)", scale), nil
	}
}

func (t *Translator) translateCharType(mysqlType string) (string, error) {
	matches := charTypeRegex.FindStringSubmatch(mysqlType)
	if len(matches) != 2 {
		return fmt.Sprintf("FixedString(%d)", t.defaultStringLength), nil
	}

	length, err := strconv.Atoi(matches[1])
	if err != nil {
		return fmt.Sprintf("FixedString(%d)", t.defaultStringLength), nil
	}

	if length > 1000 {
		return "String", nil
	}

	return fmt.Sprintf("FixedString(%d)", length), nil
}

func (t *Translator) translateEnumType(mysqlType string) (string, error) {
	start := strings.Index(mysqlType, "(")
	end := strings.LastIndex(mysqlType, ")")

	if start == -1 || end == -1 || start >= end {
		return "String", nil
	}

	enumValues := mysqlType[start+1 : end]

	values := strings.Split(enumValues, ",")
	for i, value := range values {
		value = strings.TrimSpace(value)
		value = strings.Trim(value, "'\"")
		values[i] = fmt.Sprintf("'%s'", value)
	}

	return fmt.Sprintf("Enum8(%s)", strings.Join(values, ", ")), nil
}

func (t *Translator) translateDefaultValue(mysqlDefault, clickhouseType string) string {
	// Empty default means no default specified
	if mysqlDefault == "" {
		return ""
	}

	// Handle NULL default for Nullable types
	if mysqlDefault == "NULL" {
		if strings.HasPrefix(clickhouseType, "Nullable(") {
			return "NULL"
		}
		// NULL default for non-nullable type doesn't make sense, return empty
		return ""
	}

	// Unwrap Nullable types for other (non-NULL) defaults
	if strings.HasPrefix(clickhouseType, "Nullable(") {
		innerType := strings.TrimSuffix(strings.TrimPrefix(clickhouseType, "Nullable("), ")")
		return t.translateDefaultValue(mysqlDefault, innerType)
	}

	switch {
	case strings.Contains(clickhouseType, "Int") || strings.Contains(clickhouseType, "UInt"):
		if _, err := strconv.ParseInt(mysqlDefault, 10, 64); err == nil {
			return mysqlDefault
		}
		return "0"

	case strings.Contains(clickhouseType, "Float"):
		if _, err := strconv.ParseFloat(mysqlDefault, 64); err == nil {
			return mysqlDefault
		}
		return "0.0"

	case strings.Contains(clickhouseType, "String"):
		if mysqlDefault == "CURRENT_TIMESTAMP" {
			return "''"
		}
		return fmt.Sprintf("'%s'", mysqlDefault)

	case strings.Contains(clickhouseType, "Date"):
		if mysqlDefault == "CURRENT_DATE" || mysqlDefault == "CURDATE()" {
			return "today()"
		}
		if mysqlDefault == "0000-00-00" {
			return "'1970-01-01'"
		}
		return fmt.Sprintf("'%s'", mysqlDefault)

	case strings.Contains(clickhouseType, "DateTime"):
		if mysqlDefault == "CURRENT_TIMESTAMP" || strings.Contains(mysqlDefault, "NOW()") {
			return "now()"
		}
		if mysqlDefault == "0000-00-00 00:00:00" {
			return "'1970-01-01 00:00:00'"
		}
		return fmt.Sprintf("'%s'", mysqlDefault)

	case strings.Contains(clickhouseType, "Enum"):
		return fmt.Sprintf("'%s'", mysqlDefault)

	default:
		return ""
	}
}

func (t *Translator) GenerateCreateTableDDL(schema *ClickHouseSchema) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("schema cannot be nil")
	}

	if len(schema.Columns) == 0 {
		return "", fmt.Errorf("schema must have at least one column")
	}

	// SECURITY: Validate database and table names
	if err := security.ValidateIdentifier(schema.Database, "database name"); err != nil {
		return "", fmt.Errorf("invalid database name: %w", err)
	}
	if err := security.ValidateIdentifier(schema.Table, "table name"); err != nil {
		return "", fmt.Errorf("invalid table name: %w", err)
	}

	var columnDefs []string
	for _, col := range schema.Columns {
		// SECURITY: Validate column name
		if err := security.ValidateIdentifier(col.Name, "column name"); err != nil {
			return "", fmt.Errorf("invalid column name %q: %w", col.Name, err)
		}

		escapedCol := security.EscapeIdentifier(col.Name)
		colDef := fmt.Sprintf("%s %s", escapedCol, col.Type)

		if col.DefaultValue != "" {
			colDef += fmt.Sprintf(" DEFAULT %s", col.DefaultValue)
		}

		if col.Codec != "" {
			colDef += fmt.Sprintf(" CODEC(%s)", col.Codec)
		}

		columnDefs = append(columnDefs, colDef)
	}

	escapedDB := security.EscapeIdentifier(schema.Database)
	escapedTable := security.EscapeIdentifier(schema.Table)
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (\n  %s\n)",
		escapedDB, escapedTable, strings.Join(columnDefs, ",\n  "))

	// For ReplacingMergeTree engines, specify both version and sign columns
	engineClause := fmt.Sprintf("ENGINE = %s", schema.Engine)
	if schema.Engine == clickhouse.EngineReplacingMergeTree || schema.Engine == clickhouse.EngineReplicatedReplacingMergeTree {
		// Use _version as version column and _is_deleted as sign column for ReplacingMergeTree
		engineClause = fmt.Sprintf("ENGINE = %s(_version, _is_deleted)", schema.Engine)
	}

	if len(schema.OrderBy) > 0 {
		engineClause += fmt.Sprintf("\nORDER BY (%s)", strings.Join(wrapColumns(schema.OrderBy), ", "))
	}

	if schema.PartitionBy != "" {
		engineClause += fmt.Sprintf("\nPARTITION BY %s", schema.PartitionBy)
	}

	// ReplacingMergeTree engines use ORDER BY for primary key
	if len(schema.PrimaryKey) > 0 {
		// For ReplacingMergeTree, the ORDER BY clause already defines the sort key
		// No additional PRIMARY KEY clause needed
	}

	ddl += fmt.Sprintf("\n%s", engineClause)

	if len(schema.Settings) > 0 {
		var settings []string
		for key, value := range schema.Settings {
			settings = append(settings, fmt.Sprintf("%s = %s", key, value))
		}
		ddl += fmt.Sprintf("\nSETTINGS %s", strings.Join(settings, ", "))
	}

	return ddl, nil
}

func wrapColumns(columns []string) []string {
	wrapped := make([]string, 0, len(columns))
	for _, col := range columns {
		// SECURITY: Validate and escape column names for ORDER BY clause
		if err := security.ValidateIdentifier(col, "column name"); err != nil {
			// Skip invalid column names with error logged
			// Note: This function is called during DDL generation, validation errors
			// will have already been caught earlier in GenerateCreateTableDDL
			continue
		}
		wrapped = append(wrapped, security.EscapeIdentifier(col))
	}
	return wrapped
}
