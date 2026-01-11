package metrics

import (
	"runtime"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ProcessMetrics struct {
	Goroutines prometheus.GaugeFunc
}

var (
	processOnce sync.Once
	process     *ProcessMetrics
)

func Process() *ProcessMetrics {
    processOnce.Do(func() {
        r := Registerer()
        process = &ProcessMetrics{
            Goroutines: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
                Name: "app_goroutines",
                Help: "Number of goroutines (runtime.NumGoroutine).",
            }, func() float64 {
                return float64(runtime.NumGoroutine())
            }),
        }
    })
    return process
}
