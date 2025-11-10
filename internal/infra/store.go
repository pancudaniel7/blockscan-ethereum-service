package infra

import (
	"fmt"
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/adapter/store"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/spf13/viper"
)

// InitStoreLogger creates a Redis-backed store logger using configuration loaded
// via Viper. It wires the nested Stream and Lock configs and returns the
// port-facing interface so callers remain decoupled from the adapter.
func InitStoreLogger(log applog.AppLogger, wg *sync.WaitGroup, v *validator.Validate) (port.StoreLogger, error) {
	cfg := loadStoreConfig()
	storeLogger, err := store.NewBlockLogger(log, wg, v, &cfg)
	if err != nil {
		return nil, fmt.Errorf("infra: failed to init store logger: %w", err)
	}
	return storeLogger, nil
}

// InitBlockStreamReader prepares a Redis-backed stream reader using the same
// configuration block as the logger.
func InitBlockStreamReader(log applog.AppLogger, wg *sync.WaitGroup, v *validator.Validate) (port.StoreStreamReader, error) {
	cfg := loadStoreConfig()
	stream, err := store.NewBlockStream(log, wg, v, cfg)
	if err != nil {
		return nil, fmt.Errorf("infra: failed to init block stream: %w", err)
	}
	return stream, nil
}

func loadStoreConfig() store.Config {
	return store.Config{
		Host:               viper.GetString("redis.host"),
		Port:               viper.GetString("redis.port"),
		Password:           viper.GetString("redis.password"),
		DB:                 viper.GetInt("redis.db"),
		UseTLS:             viper.GetBool("redis.use_tls"),
		PoolSize:           viper.GetInt("redis.pool_size"),
		MaxRetries:         viper.GetInt("redis.max_retries"),
		DialTimeoutSeconds: viper.GetInt("redis.dial_timeout_seconds"),
		Streams: store.StreamConfig{
			Key:                     viper.GetString("redis.streams.key"),
			ConsumerGroup:           viper.GetString("redis.streams.consumer_group"),
			ConsumerName:            viper.GetString("redis.streams.consumer_name"),
			ReadCount:               viper.GetInt("redis.streams.read_count"),
			ReadBlockTimeoutSeconds: viper.GetInt("redis.streams.read_block_timeout_seconds"),
			ClaimIdleSeconds:        viper.GetInt("redis.streams.claim_idle_seconds"),
		},
		Lock: store.LockConfig{
			DedupPrefix:     viper.GetString("redis.lock.dedup_prefix"),
			BlockTTLSeconds: viper.GetInt("redis.lock.block_ttl_seconds"),
		},
	}
}
