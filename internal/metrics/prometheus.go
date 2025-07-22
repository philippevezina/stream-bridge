package metrics

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusMetrics struct {
	eventsProcessed              prometheus.Counter
	eventsSuccessful             prometheus.Counter
	eventsFailed                 prometheus.Counter
	eventsSkippedMissingMetadata *prometheus.CounterVec
	batchesProcessed             prometheus.Counter
	processingDuration           prometheus.Histogram
	clickhouseOpDuration         *prometheus.HistogramVec
	batcherOpDuration            *prometheus.HistogramVec
	batcherBufferSize            prometheus.Gauge
	replicationLag               prometheus.Gauge
	queueLength                  prometheus.Gauge
	workerQueueLengths           *prometheus.GaugeVec
	connectionStatus             *prometheus.GaugeVec
	lastEventTime                prometheus.Gauge
	checkpointsCreated           prometheus.Counter
	checkpointAge                prometheus.Gauge
	lastEventTimestamp           time.Time
	mu                           sync.RWMutex
}

func NewPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		eventsProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "stream_bridge_events_processed_total",
			Help: "Total number of events processed",
		}),
		eventsSuccessful: promauto.NewCounter(prometheus.CounterOpts{
			Name: "stream_bridge_events_successful_total",
			Help: "Total number of events successfully processed",
		}),
		eventsFailed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "stream_bridge_events_failed_total",
			Help: "Total number of events that failed processing",
		}),
		eventsSkippedMissingMetadata: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "stream_bridge_events_skipped_missing_metadata_total",
			Help: "Total number of events skipped due to missing binlog metadata",
		}, []string{"database", "table"}),
		batchesProcessed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "stream_bridge_batches_processed_total",
			Help: "Total number of batches processed",
		}),
		processingDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "stream_bridge_processing_duration_seconds",
			Help:    "Duration of event processing in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		clickhouseOpDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "stream_bridge_clickhouse_operation_duration_seconds",
			Help:    "Duration of ClickHouse operations in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation_type"}),
		batcherOpDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "stream_bridge_batcher_operation_duration_seconds",
			Help:    "Duration of batcher operations in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation_type"}),
		batcherBufferSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "stream_bridge_batcher_buffer_size",
			Help: "Current number of events in batcher buffer",
		}),
		replicationLag: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "stream_bridge_replication_lag_seconds",
			Help: "Current replication lag in seconds",
		}),
		queueLength: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "stream_bridge_queue_length",
			Help: "Current length of the event processing queue",
		}),
		workerQueueLengths: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "stream_bridge_worker_queue_length",
			Help: "Current length of each worker's batch queue",
		}, []string{"worker_id"}),
		connectionStatus: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "stream_bridge_connection_status",
			Help: "Connection status (1 = connected, 0 = disconnected)",
		}, []string{"database_type"}),
		lastEventTime: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "stream_bridge_last_event_timestamp",
			Help: "Timestamp of the last processed event",
		}),
		checkpointsCreated: promauto.NewCounter(prometheus.CounterOpts{
			Name: "stream_bridge_checkpoints_created_total",
			Help: "Total number of checkpoints created",
		}),
		checkpointAge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "stream_bridge_checkpoint_age_seconds",
			Help: "Age of the last checkpoint in seconds",
		}),
	}
}

func (m *PrometheusMetrics) IncEventsProcessed() {
	m.eventsProcessed.Inc()
}

func (m *PrometheusMetrics) IncEventsSuccessful() {
	m.eventsSuccessful.Inc()
}

func (m *PrometheusMetrics) IncEventsFailed() {
	m.eventsFailed.Inc()
}

func (m *PrometheusMetrics) IncEventsSkippedMissingMetadata(database, table string) {
	m.eventsSkippedMissingMetadata.WithLabelValues(database, table).Inc()
}

func (m *PrometheusMetrics) IncBatchesProcessed() {
	m.batchesProcessed.Inc()
}

func (m *PrometheusMetrics) ObserveProcessingDuration(duration time.Duration) {
	m.processingDuration.Observe(duration.Seconds())
}

func (m *PrometheusMetrics) ObserveClickHouseOperation(operationType string, duration time.Duration) {
	m.clickhouseOpDuration.WithLabelValues(operationType).Observe(duration.Seconds())
}

func (m *PrometheusMetrics) ObserveBatcherOperation(operationType string, duration time.Duration) {
	m.batcherOpDuration.WithLabelValues(operationType).Observe(duration.Seconds())
}

func (m *PrometheusMetrics) SetBatcherBufferSize(size int) {
	m.batcherBufferSize.Set(float64(size))
}

func (m *PrometheusMetrics) SetReplicationLag(lag time.Duration) {
	m.replicationLag.Set(lag.Seconds())
}

func (m *PrometheusMetrics) SetQueueLength(length int) {
	m.queueLength.Set(float64(length))
}

func (m *PrometheusMetrics) SetWorkerQueueLengths(lengths []int) {
	for i, length := range lengths {
		m.workerQueueLengths.WithLabelValues(fmt.Sprintf("%d", i)).Set(float64(length))
	}
}

func (m *PrometheusMetrics) SetConnectionStatus(dbType string, connected bool) {
	status := 0.0
	if connected {
		status = 1.0
	}
	m.connectionStatus.WithLabelValues(dbType).Set(status)
}

func (m *PrometheusMetrics) SetLastEventTime(timestamp time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastEventTimestamp = timestamp
	m.lastEventTime.Set(float64(timestamp.Unix()))
}

func (m *PrometheusMetrics) GetLastEventTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastEventTimestamp
}

func (m *PrometheusMetrics) AddEventsProcessed(count int64) {
	m.eventsProcessed.Add(float64(count))
}

func (m *PrometheusMetrics) AddEventsSuccessful(count int64) {
	m.eventsSuccessful.Add(float64(count))
}

func (m *PrometheusMetrics) AddEventsFailed(count int64) {
	m.eventsFailed.Add(float64(count))
}

func (m *PrometheusMetrics) IncCheckpointsCreated() {
	m.checkpointsCreated.Inc()
}

func (m *PrometheusMetrics) SetCheckpointAge(age time.Duration) {
	m.checkpointAge.Set(age.Seconds())
}
