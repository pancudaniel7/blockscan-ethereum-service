package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type KafkaMetrics struct {
	ProduceAttemptsTotal prometheus.Counter
	ProduceSuccessTotal  prometheus.Counter
	ProduceErrorsTotal   *prometheus.CounterVec
	ProduceLatencyMS     prometheus.Histogram
}

var (
	kafkaOnce sync.Once
	kafka     *KafkaMetrics
)

func Kafka() *KafkaMetrics {
	kafkaOnce.Do(func() {
		r := Registerer()
		kafka = &KafkaMetrics{
			ProduceAttemptsTotal: promauto.With(r).NewCounter(prometheus.CounterOpts{
				Name: "kafka_produce_attempts_total",
				Help: "kafka produce attempts (success + error)",
			}),
			ProduceSuccessTotal: promauto.With(r).NewCounter(prometheus.CounterOpts{
				Name: "kafka_produce_success_total",
				Help: "successful kafka produce attempts",
			}),
			ProduceErrorsTotal: promauto.With(r).NewCounterVec(
				prometheus.CounterOpts{Name: "kafka_produce_errors_total", Help: "kafka produce errors by type"},
				[]string{"type"},
			),
			ProduceLatencyMS: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
				Name:    "kafka_produce_latency_ms",
				Help:    "kafka produce latency per attempt (ms)",
				Buckets: []float64{5, 10, 20, 50, 100, 200, 500, 1000, 2000},
			}),
		}
	})
	return kafka
}
