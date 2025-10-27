package block

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/go-redis/redis/v8"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

// BlockLogger is a thin Redis-based store for publishing block events to a
// stream with idempotency enforced by a SET NX key.
//
// Concurrency: BlockLogger is safe for concurrent use. It relies on the
// concurrency-safe go-redis client and atomicity of the Redis function
// "add_block" to avoid races.
type BlockLogger struct {
	rdb       *redis.Client
	logger    applog.AppLogger
	wg        *sync.WaitGroup
	validator *validator.Validate
	cfg       Config
}

// NewBlockLogger creates a Redis client from the provided Config, validates the
// configuration, optionally enables TLS, and returns an initialized BlockLogger.
func NewBlockLogger(logger applog.AppLogger, wg *sync.WaitGroup, v *validator.Validate, cfg Config) (*BlockLogger, error) {
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
	return &BlockLogger{
		rdb:       rdb,
		logger:    logger,
		wg:        wg,
		validator: v,
		cfg:       cfg,
	}, nil
}

// Store publishes a block to the configured Redis stream using the `add_block`
// library function and returns true when the dedup key was set and the message
// enqueued. Returns false when the dedup key already existed.
func (bs *BlockLogger) Store(ctx context.Context, block *entity.Block) (bool, error) {
	if err := bs.validator.Struct(block); err != nil {
		return false, &apperr.BlockStoreErr{Msg: "invalid block", Cause: err}
	}
	setKey := fmt.Sprintf("%s:%s", bs.cfg.DedupPrefix, block.Hash.Hex())
	ttlMs := strconv.FormatInt(int64(bs.cfg.BlockTTLSeconds*1000), 10)
	id := "*"

	// Minimal field set; extend as needed
	fields := []string{
		"hash", block.Hash.Hex(),
		"number", strconv.FormatUint(block.Header.Number, 10),
	}

	// FCALL add_block 2 <setKey> <streamKey> <ttl> <id> <fields...>
	args := make([]interface{}, 0, 7+len(fields))
	args = append(args, "FCALL", "add_block", 2, setKey, bs.cfg.StreamKey, ttlMs, id)
	for _, f := range fields {
		args = append(args, f)
	}

	res, err := bs.rdb.Do(ctx, args...).Result()
	if err != nil {
		return false, &apperr.BlockStoreErr{Msg: "redis FCALL add_block failed", Cause: err}
	}

	arr, ok := res.([]interface{})
	if !ok || len(arr) < 1 {
		return false, &apperr.BlockStoreErr{Msg: fmt.Sprintf("unexpected FCALL response: %T", res)}
	}

	switch v := arr[0].(type) {
	case int64:
		return v == 1, nil
	default:
		return false, &apperr.BlockStoreErr{Msg: fmt.Sprintf("unexpected status type: %T", v)}
	}
}
