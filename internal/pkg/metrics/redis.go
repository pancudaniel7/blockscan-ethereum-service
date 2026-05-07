package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type RedisMetrics struct {
	StreamLength             *prometheus.GaugeVec
	StreamPendingMessages    *prometheus.GaugeVec
	StreamLagMessages        *prometheus.GaugeVec
	StreamOldestPendingAgeMS *prometheus.GaugeVec
}

var (
	redisOnce sync.Once
	redisM    *RedisMetrics
)

func Redis() *RedisMetrics {
	redisOnce.Do(func() {
		r := Registerer()
		redisM = &RedisMetrics{
			StreamLength: promauto.With(r).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "redis_stream_length",
					Help: "redis stream length by stream",
				},
				[]string{"stream"},
			),
			StreamPendingMessages: promauto.With(r).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "redis_stream_pending_messages",
					Help: "redis stream pending messages by stream and consumer group",
				},
				[]string{"stream", "group"},
			),
			StreamLagMessages: promauto.With(r).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "redis_stream_lag_messages",
					Help: "redis stream undelivered messages by stream and consumer group",
				},
				[]string{"stream", "group"},
			),
			StreamOldestPendingAgeMS: promauto.With(r).NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "redis_stream_oldest_pending_age_ms",
					Help: "age in milliseconds of the oldest pending redis stream message",
				},
				[]string{"stream", "group"},
			),
		}
	})
	return redisM
}
