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
                Buckets: []float64{5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000},
            }),
        }
    })
    return pipeline
}

