package infra

import (
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
    "regexp"

	imetrics "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/metrics"
)

var promRegistry *prometheus.Registry

func InitMetrics(app *fiber.App) {
	if app == nil {
		return
	}
    if promRegistry == nil {
        promRegistry = prometheus.NewRegistry()
        promRegistry.MustRegister(collectors.NewGoCollector(
            collectors.WithGoCollectorRuntimeMetrics(
                collectors.MetricsGC,
                collectors.MetricsMemory,
                collectors.MetricsScheduler,
                collectors.GoRuntimeMetricsRule{Matcher: regexp.MustCompile("^/cpu/classes/.*")},
            ),
        ))
        promRegistry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{ReportErrors: true}))
		svc := viper.GetString("service.name")
		inst := viper.GetString("service.instance")

		prefix := svc
		prefixed := prometheus.WrapRegistererWithPrefix(prefix+"_", promRegistry)
		bi := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: prefix + "_service_build_info", Help: "build info", ConstLabels: prometheus.Labels{"service": svc, "instance": inst}}, []string{"version", "rev"})
		promRegistry.MustRegister(bi)
		bi.WithLabelValues("dev", "unknown").Set(1)
		imetrics.UseRegisterer(prefixed)
		_ = imetrics.Kafka()
		_ = imetrics.Scanner()
		_ = imetrics.Process()
		_ = imetrics.Pipeline()
		_ = imetrics.App()
	}
	h := promhttp.InstrumentMetricHandler(promRegistry, promhttp.HandlerFor(promRegistry, promhttp.HandlerOpts{}))
	app.Get("/metrics", adaptor.HTTPHandler(h))
}
