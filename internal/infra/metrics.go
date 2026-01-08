package infra

import (
    "github.com/gofiber/fiber/v3"
    "github.com/gofiber/fiber/v3/middleware/adaptor"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/collectors"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/spf13/viper"

    imetrics "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/metrics"
)

var promRegistry *prometheus.Registry

func InitMetrics(app *fiber.App) {
    if app == nil {
        return
    }
    if promRegistry == nil {
        promRegistry = prometheus.NewRegistry()
        promRegistry.MustRegister(collectors.NewGoCollector())
        promRegistry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
        svc := viper.GetString("service.name")
        inst := viper.GetString("service.instance")
        bi := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "service_build_info", Help: "build info", ConstLabels: prometheus.Labels{"service": svc, "instance": inst}}, []string{"version", "rev"})
        promRegistry.MustRegister(bi)
        bi.WithLabelValues("dev", "unknown").Set(1)
        imetrics.UseRegisterer(promRegistry)
        _ = imetrics.Kafka()
        _ = imetrics.Scanner()
    }
    h := promhttp.InstrumentMetricHandler(promRegistry, promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
    app.Get("/metrics", adaptor.HTTPHandler(h))
}
