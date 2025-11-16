package infra

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/adapter/publish"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/spf13/viper"
)

// InitBlockPublisher wires the Kafka publisher using configuration sourced from Viper.
func InitBlockPublisher(logger applog.AppLogger, v *validator.Validate) (port.Publisher, error) {
	if logger == nil {
		return nil, fmt.Errorf("infra: logger is required to init block publisher")
	}
	if v == nil {
		v = validator.New()
	}

	cfg := publish.Config{
		Brokers:               viper.GetStringSlice("kafka.brokers"),
		Topic:                 viper.GetString("kafka.topic"),
		ClientID:              viper.GetString("kafka.client_id"),
		TransactionalID:       viper.GetString("kafka.transactional_id"),
		MaxRetryAttempts:      viper.GetInt("kafka.max_retry_attempts"),
		RetryInitialBackoffMS: viper.GetInt("kafka.retry_initial_backoff_ms"),
		RetryMaxBackoffMS:     viper.GetInt("kafka.retry_max_backoff_ms"),
		RetryJitter:           viper.GetFloat64("kafka.retry_jitter"),
		WriteTimeoutSeconds:   viper.GetInt("kafka.write_timeout_seconds"),
	}

	publisher, err := publish.NewKafkaPublisher(logger, cfg, v)
	if err != nil {
		return nil, fmt.Errorf("infra: failed to init block publisher: %w", err)
	}
	return publisher, nil
}
