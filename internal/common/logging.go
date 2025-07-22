package common

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/philippevezina/stream-bridge/internal/config"
)

func NewLogger(cfg *config.LoggingConfig) (*zap.Logger, error) {
	var zapCfg zap.Config

	switch cfg.Format {
	case "json":
		zapCfg = zap.NewProductionConfig()
	case "console":
		zapCfg = zap.NewDevelopmentConfig()
	default:
		zapCfg = zap.NewProductionConfig()
	}

	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level '%s': %w", cfg.Level, err)
	}
	zapCfg.Level = zap.NewAtomicLevelAt(level)

	if cfg.OutputPath != "" && cfg.OutputPath != "stdout" {
		if cfg.MaxSize > 0 || cfg.MaxBackups > 0 || cfg.MaxAge > 0 {
			zapCfg.EncoderConfig.TimeKey = "timestamp"
			zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
			zapCfg.EncoderConfig.MessageKey = "message"
			zapCfg.EncoderConfig.LevelKey = "level"
			zapCfg.EncoderConfig.CallerKey = "caller"

			logWriter := &lumberjack.Logger{
				Filename:   cfg.OutputPath,
				MaxSize:    cfg.MaxSize,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAge,
				Compress:   cfg.Compress,
				LocalTime:  cfg.LocalTime,
			}

			var encoder zapcore.Encoder
			if cfg.Format == "console" {
				encoder = zapcore.NewConsoleEncoder(zapCfg.EncoderConfig)
			} else {
				encoder = zapcore.NewJSONEncoder(zapCfg.EncoderConfig)
			}

			core := zapcore.NewCore(
				encoder,
				zapcore.AddSync(logWriter),
				level,
			)

			logger := zap.New(core)
			logger = logger.With(
				zap.String("service", "stream-bridge"),
				zap.String("version", GetVersion()),
			)

			return logger, nil
		}

		zapCfg.OutputPaths = []string{cfg.OutputPath}
		zapCfg.ErrorOutputPaths = []string{cfg.OutputPath}
	}

	zapCfg.EncoderConfig.TimeKey = "timestamp"
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapCfg.EncoderConfig.MessageKey = "message"
	zapCfg.EncoderConfig.LevelKey = "level"
	zapCfg.EncoderConfig.CallerKey = "caller"

	zapCfg.InitialFields = map[string]interface{}{
		"service": "stream-bridge",
		"version": GetVersion(),
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build logger: %w", err)
	}

	return logger, nil
}

func GetVersion() string {
	if version := os.Getenv("STREAM_BRIDGE_VERSION"); version != "" {
		return version
	}
	return "dev"
}

func LoggerWithComponent(logger *zap.Logger, component string) *zap.Logger {
	return logger.With(zap.String("component", component))
}

// LoggerCore holds the components needed to create a logger.
// This allows wrapping the core before creating the final logger.
type LoggerCore struct {
	Core          zapcore.Core
	EncoderCfg    zapcore.EncoderConfig
	Level         zapcore.Level
	InitialFields map[string]interface{}
}

// NewLoggerCore creates a Zap core and configuration without building the final logger.
// This allows the core to be wrapped (e.g., for NewRelic) before creating the logger.
func NewLoggerCore(cfg *config.LoggingConfig) (*LoggerCore, error) {
	var encoderCfg zapcore.EncoderConfig

	switch cfg.Format {
	case "console":
		encoderCfg = zap.NewDevelopmentEncoderConfig()
	default:
		encoderCfg = zap.NewProductionEncoderConfig()
	}

	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level '%s': %w", cfg.Level, err)
	}

	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.MessageKey = "message"
	encoderCfg.LevelKey = "level"
	encoderCfg.CallerKey = "caller"

	var encoder zapcore.Encoder
	if cfg.Format == "console" {
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	} else {
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	}

	var writeSyncer zapcore.WriteSyncer
	if cfg.OutputPath != "" && cfg.OutputPath != "stdout" && cfg.OutputPath != "stderr" {
		if cfg.MaxSize > 0 || cfg.MaxBackups > 0 || cfg.MaxAge > 0 {
			logWriter := &lumberjack.Logger{
				Filename:   cfg.OutputPath,
				MaxSize:    cfg.MaxSize,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAge,
				Compress:   cfg.Compress,
				LocalTime:  cfg.LocalTime,
			}
			writeSyncer = zapcore.AddSync(logWriter)
		} else {
			file, err := os.OpenFile(cfg.OutputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, fmt.Errorf("failed to open log file: %w", err)
			}
			writeSyncer = zapcore.AddSync(file)
		}
	} else {
		writeSyncer = zapcore.AddSync(os.Stdout)
	}

	core := zapcore.NewCore(encoder, writeSyncer, level)

	return &LoggerCore{
		Core:       core,
		EncoderCfg: encoderCfg,
		Level:      level,
		InitialFields: map[string]interface{}{
			"service": "stream-bridge",
			"version": GetVersion(),
		},
	}, nil
}

// BuildLogger creates a zap.Logger from a LoggerCore.
// The core can be wrapped before calling this function.
func (lc *LoggerCore) BuildLogger(core zapcore.Core) *zap.Logger {
	logger := zap.New(core, zap.AddCaller())

	for k, v := range lc.InitialFields {
		logger = logger.With(zap.Any(k, v))
	}

	return logger
}
