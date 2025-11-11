package store

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/usecase"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/go-redis/redis/v8"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
)

// BlockLogger is a thin Redis-based store for publishing block events to a
// stream with idempotency enforced by a SET NX key.
//
// Concurrency: BlockLogger is safe for concurrent use. It relies on the
// concurrency-safe go-redis client and atomicity of the Redis function
// "add_block" to avoid races.
type BlockLogger struct {
	rdb       *redis.Client
	log       applog.AppLogger
	validator *validator.Validate
	cfg       Config
}

// NewBlockLogger creates a Redis client from the provided Config, validates the
// configuration, optionally enables TLS, and returns an initialized BlockLogger.
func NewBlockLogger(log applog.AppLogger, _ *sync.WaitGroup, v *validator.Validate, cfg *Config) (*BlockLogger, error) {
	if err := v.Struct(cfg); err != nil {
		log.Error("invalid redis config: %v", err)
		return nil, apperr.NewBlockStoreErr("invalid redis config", err)
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
		log:       log,
		validator: v,
		cfg:       *cfg,
	}, nil
}

// Store publishes a block to the configured Redis stream using the `add_block`
// library function and returns true when the dedup key was set and the message
// enqueued. Returns false when the dedup key already existed.
func (bs *BlockLogger) Store(ctx context.Context, block *entity.Block) (bool, error) {
	if err := bs.validator.Struct(block); err != nil {
		return false, apperr.NewBlockStoreErr("invalid block", err)
	}

	payload, err := json.Marshal(usecase.ToDTO(block))
	if err != nil {
		return false, apperr.NewBlockStoreErr("failed to marshal block payload", err)
	}

	setKey := fmt.Sprintf("%s:%s", bs.cfg.Lock.DedupPrefix, block.Hash.Hex())
	ttlMs := strconv.FormatInt(int64(bs.cfg.Lock.BlockTTLSeconds*1000), 10)
	id := "*"

	// Minimal field set; extend as needed
	fields := []string{
		"hash", block.Hash.Hex(),
		"number", strconv.FormatUint(block.Header.Number, 10),
		"payload", string(payload),
	}

	// FCALL add_block 2 <setKey> <streamKey> <ttl> <id> <fields...>
	args := make([]interface{}, 0, 7+len(fields))
	args = append(args, "FCALL", "add_block", 2, setKey, bs.cfg.Streams.Key, ttlMs, id)
	for _, f := range fields {
		args = append(args, f)
	}

	var stored bool
	err = pattern.Retry(
		ctx,
		func(attempt int) error {
			res, err := bs.rdb.Do(ctx, args...).Result()
			if err != nil {
				bs.log.Warn("redis FCALL add_block failed", "attempt", attempt, "err", err)
				return apperr.NewBlockStoreErr("redis FCALL add_block failed", err)
			}

			arr, ok := res.([]interface{})
			if !ok || len(arr) < 1 {
				return apperr.NewBlockStoreErr("unexpected FCALL response", fmt.Errorf("type=%T", res))
			}

			status, ok := arr[0].(int64)
			if !ok {
				return apperr.NewBlockStoreErr("unexpected FCALL status type", fmt.Errorf("type=%T", arr[0]))
			}

			if status == 1 {
				stored = true
				return nil
			}

			stored = false
			if len(arr) > 1 {
				if reason, ok := arr[1].(string); ok {
					switch strings.ToUpper(reason) {
					case "EXISTS":
						bs.log.Trace("Block already logged; skipping", "hash", block.Hash.Hex(), "number", block.Header.Number)
						return nil
					case "XADD_ERR":
						bs.log.Warn("Redis XADD failed while adding block", "hash", block.Hash.Hex(), "number", block.Header.Number)
						return apperr.NewBlockStoreErr("redis add_block XADD failed", nil)
					}
				}
			}
			return apperr.NewBlockStoreErr("redis add_block failed with unknown reason", nil)
		},
		pattern.WithMaxAttempts(3),
		pattern.WithInitialDelay(200*time.Millisecond),
		pattern.WithMaxDelay(1*time.Second),
	)
	if err != nil {
		return false, apperr.NewBlockStoreErr("redis FCALL add_block failed", err)
	}

	return stored, nil
}
