package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/philippevezina/stream-bridge/internal/config"
)

type Metrics interface {
	IncEventsProcessed()
	IncEventsSuccessful()
	IncEventsFailed()
	IncEventsSkippedMissingMetadata(database, table string)
	IncBatchesProcessed()
	ObserveProcessingDuration(duration time.Duration)
	ObserveClickHouseOperation(operationType string, duration time.Duration)
	ObserveBatcherOperation(operationType string, duration time.Duration)
	SetBatcherBufferSize(size int)
	SetReplicationLag(lag time.Duration)
	SetQueueLength(length int)
	SetWorkerQueueLengths(lengths []int)
	SetConnectionStatus(dbType string, connected bool)
	SetLastEventTime(timestamp time.Time)
	GetLastEventTime() time.Time
	AddEventsProcessed(count int64)
	AddEventsSuccessful(count int64)
	AddEventsFailed(count int64)
	IncCheckpointsCreated()
	SetCheckpointAge(age time.Duration)
}

type Manager struct {
	cfg     *config.MonitoringConfig
	logger  *zap.Logger
	metrics Metrics
	server  *http.Server
}

func NewManager(cfg *config.MonitoringConfig, logger *zap.Logger) *Manager {
	var metrics Metrics

	if cfg.Enabled {
		metrics = NewPrometheusMetrics()
	} else {
		metrics = &NoopMetrics{}
	}

	return &Manager{
		cfg:     cfg,
		logger:  logger,
		metrics: metrics,
	}
}

func (m *Manager) Start() error {
	if !m.cfg.Enabled {
		m.logger.Info("Metrics disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle(m.cfg.MetricsPath, promhttp.Handler())
	mux.HandleFunc(m.cfg.HealthPath, m.healthHandler)

	m.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", m.cfg.Port),
		Handler: mux,
	}

	go func() {
		m.logger.Info("Starting metrics server",
			zap.Int("port", m.cfg.Port),
			zap.String("metrics_path", m.cfg.MetricsPath),
			zap.String("health_path", m.cfg.HealthPath))

		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Error("Metrics server error", zap.Error(err))
		}
	}()

	return nil
}

func (m *Manager) Stop() error {
	if m.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.server.Shutdown(ctx); err != nil {
		m.logger.Error("Failed to shutdown metrics server", zap.Error(err))
		return err
	}

	m.logger.Info("Metrics server stopped")
	return nil
}

func (m *Manager) GetMetrics() Metrics {
	return m.metrics
}

func (m *Manager) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok","timestamp":"%s"}`, time.Now().UTC().Format(time.RFC3339))
}

type NoopMetrics struct{}

func (n *NoopMetrics) IncEventsProcessed()                                                     {}
func (n *NoopMetrics) IncEventsSuccessful()                                                    {}
func (n *NoopMetrics) IncEventsFailed()                                                        {}
func (n *NoopMetrics) IncEventsSkippedMissingMetadata(database, table string)                  {}
func (n *NoopMetrics) IncBatchesProcessed()                                                    {}
func (n *NoopMetrics) ObserveProcessingDuration(duration time.Duration)                        {}
func (n *NoopMetrics) ObserveClickHouseOperation(operationType string, duration time.Duration) {}
func (n *NoopMetrics) ObserveBatcherOperation(operationType string, duration time.Duration)    {}
func (n *NoopMetrics) SetBatcherBufferSize(size int)                                           {}
func (n *NoopMetrics) SetReplicationLag(lag time.Duration)                                     {}
func (n *NoopMetrics) SetQueueLength(length int)                                               {}
func (n *NoopMetrics) SetWorkerQueueLengths(lengths []int)                                     {}
func (n *NoopMetrics) SetConnectionStatus(dbType string, connected bool)                       {}
func (n *NoopMetrics) SetLastEventTime(timestamp time.Time)                                    {}
func (n *NoopMetrics) GetLastEventTime() time.Time                                             { return time.Time{} }
func (n *NoopMetrics) AddEventsProcessed(count int64)                                          {}
func (n *NoopMetrics) AddEventsSuccessful(count int64)                                         {}
func (n *NoopMetrics) AddEventsFailed(count int64)                                             {}
func (n *NoopMetrics) IncCheckpointsCreated()                                                  {}
func (n *NoopMetrics) SetCheckpointAge(age time.Duration)                                      {}
