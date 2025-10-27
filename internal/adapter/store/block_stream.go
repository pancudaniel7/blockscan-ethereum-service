package store

import (
    "context"
    "crypto/tls"
    "net"
    "sync"
    "time"

    "github.com/go-playground/validator/v10"
    "github.com/go-redis/redis/v8"
    "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

// BlockStream encapsulates Redis stream publishing state and dependencies.
// It mirrors the cache.Config options and uses the shared validator instance.
type BlockStream struct {
    rdb       *redis.Client
    logger    applog.AppLogger
    wg        *sync.WaitGroup
    cancel    context.CancelFunc
    validator *validator.Validate
    cfg       Config
}

// NewBlockStream validates the Config, constructs a Redis client with optional
// TLS, and returns an initialized BlockStream.
func NewBlockStream(logger applog.AppLogger, wg *sync.WaitGroup, v *validator.Validate, cfg Config) (*BlockStream, error) {
    if v == nil {
        v = validator.New()
    }
    if err := v.Struct(cfg); err != nil {
        logger.Error("invalid redis config: %v", err)
        return nil, err
    }

	addr := net.JoinHostPort(cfg.Host, cfg.Port)
	opts := &redis.Options{
		Addr:        addr,
		Password:    cfg.Password,
		DB:          cfg.DB,
		PoolSize:    cfg.PoolSize,
		MaxRetries:  cfg.MaxRetries,
		DialTimeout: time.Duration(cfg.DialTimeoutSeconds) * time.Second,
	}
	if cfg.UseTLS {
		opts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

    rdb := redis.NewClient(opts)
    // Prepare a cancel function for future background operations.
    _, cancel := context.WithCancel(context.Background())

    return &BlockStream{
        rdb:       rdb,
        logger:    logger,
        wg:        wg,
        cancel:    cancel,
        validator: v,
        cfg:       cfg,
    }, nil
}

func (bs *BlockStream) StartStreaming(ctx context.Context) {

}
