package metrics

import (
    "sync"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

type AppMetrics struct {
    ErrorsTotal *prometheus.CounterVec
    WarningsTotal *prometheus.CounterVec
}

var (
    appOnce sync.Once
    app     *AppMetrics
)

func App() *AppMetrics {
    appOnce.Do(func() {
        r := Registerer()
        app = &AppMetrics{
            ErrorsTotal: promauto.With(r).NewCounterVec(
                prometheus.CounterOpts{
                    Name: "app_errors_total",
                    Help: "application errors by component and reason",
                },
                []string{"component", "reason"},
            ),
            WarningsTotal: promauto.With(r).NewCounterVec(
                prometheus.CounterOpts{
                    Name: "app_warnings_total",
                    Help: "application warnings (handled/transient) by component and reason",
                },
                []string{"component", "reason"},
            ),
        }
    })
    return app
}
