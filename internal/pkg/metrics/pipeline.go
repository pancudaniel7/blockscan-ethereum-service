package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PipelineMetrics struct {
	EndToEndLatencyMS prometheus.Histogram
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
		}
	})
	return pipeline
}
