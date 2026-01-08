package metrics

import (
    "sync"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

type ScannerMetrics struct {
    ScannedBlocksTotal   *prometheus.CounterVec
    Connected            prometheus.Gauge
    ReconnectsTotal      *prometheus.CounterVec
    FetchErrorsTotal     *prometheus.CounterVec
    HandlerErrorsTotal   *prometheus.CounterVec
    FetchLatencyMS       *prometheus.HistogramVec
}

var (
    scannerOnce sync.Once
    scanner     *ScannerMetrics
)

func Scanner() *ScannerMetrics {
    scannerOnce.Do(func() {
        r := Registerer()
        scanner = &ScannerMetrics{
            ScannedBlocksTotal: promauto.With(r).NewCounterVec(
                prometheus.CounterOpts{
                    Name: "scanned_blocks_total",
                    Help: "unique blocks scanned successfully by the Blockchain scanner",
                },
                []string{"mode", "chain"},
            ),
            Connected: promauto.With(r).NewGauge(prometheus.GaugeOpts{
                Name: "scanner_connected",
                Help: "scanner connectivity/subscription status (1=connected,0=disconnected)",
            }),
            ReconnectsTotal: promauto.With(r).NewCounterVec(
                prometheus.CounterOpts{
                    Name: "scanner_reconnects_total",
                    Help: "scanner reconnect attempts by reason",
                },
                []string{"reason"},
            ),
            FetchErrorsTotal: promauto.With(r).NewCounterVec(
                prometheus.CounterOpts{
                    Name: "scanner_fetch_errors_total",
                    Help: "scanner block fetch errors by mode and code",
                },
                []string{"mode", "code"},
            ),
            HandlerErrorsTotal: promauto.With(r).NewCounterVec(
                prometheus.CounterOpts{
                    Name: "scanner_handler_errors_total",
                    Help: "scanner handler errors by mode",
                },
                []string{"mode"},
            ),
            FetchLatencyMS: promauto.With(r).NewHistogramVec(
                prometheus.HistogramOpts{
                    Name:    "scanner_fetch_latency_ms",
                    Help:    "scanner block fetch latency (ms)",
                    Buckets: []float64{5, 10, 20, 50, 100, 200, 500, 1000, 2000},
                },
                []string{"mode"},
            ),
        }
    })
    return scanner
}
