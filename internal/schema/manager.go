package schema

import (
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/clickhouse"
	"github.com/philippevezina/stream-bridge/internal/common"
)

type Manager struct {
	translator *Translator
	logger     *zap.Logger
}

type ManagerConfig struct {
	Translation TranslationOptions `json:"translation"`
}

func NewManager(cfg ManagerConfig, logger *zap.Logger) (*Manager, error) {
	translator := NewTranslator(common.LoggerWithComponent(logger, "translator"), &cfg.Translation)

	return &Manager{
		translator: translator,
		logger:     logger,
	}, nil
}

// TranslateTable translates a MySQL table schema to ClickHouse format
func (m *Manager) TranslateTable(mysqlSchema *common.TableInfo, engine clickhouse.TableEngine, targetDatabase string) (*ClickHouseSchema, error) {
	return m.translator.TranslateTable(mysqlSchema, engine, targetDatabase)
}

// GenerateCreateTableDDL generates the CREATE TABLE DDL for a ClickHouse schema
func (m *Manager) GenerateCreateTableDDL(schema *ClickHouseSchema) (string, error) {
	return m.translator.GenerateCreateTableDDL(schema)
}
