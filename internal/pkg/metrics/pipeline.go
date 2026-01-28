package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PipelineMetrics struct {
	EndToEndLatencyMS prometheus.Histogram
	ProcessedTotal    prometheus.Counter
}

var (
	pipelineOnce sync.Once
	pipeline     *PipelineMetrics
)

func Pipeline() *PipelineMetrics {
	pipelineOnce.Do(func() {
		r := Registerer()
		pipeline = &PipelineMetrics{
			EndToEndLatencyMS: promauto.With(r).NewHistogram(prometheus.HistogramOpts{
				Name:    "pipeline_end_to_end_latency_ms",
				Help:    "end-to-end latency from scan to publish+marker+ack (ms)",
				Buckets: []float64{30, 35, 40, 45, 50, 60},
			}),
			ProcessedTotal: promauto.With(r).NewCounter(prometheus.CounterOpts{
				Name: "pipeline_processed_total",
				Help: "total blocks consumed from stream and acknowledged",
			}),
		}
	})
	return pipeline
}
