package store

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/usecase"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
    imetrics "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/metrics"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
	"github.com/redis/go-redis/v9"
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
		log.Error("invalid redis config", "err", err)
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

// StoreBlock publishes a block to the configured Redis stream using the `add_block`
// library function and returns true when the dedup key was set and the message
// enqueued. Returns false when the dedup key already existed.
func (bs *BlockLogger) StoreBlock(ctx context.Context, block *entity.Block) (bool, error) {
	if err := bs.validator.Struct(block); err != nil {
		return false, apperr.NewBlockStoreErr("invalid block", err)
	}

	payload, err := usecase.MarshalBlockJSON(block)
	if err != nil {
		return false, apperr.NewBlockStoreErr("failed to marshal block payload", err)
	}

	tag := clusterHashTag(bs.cfg.Streams.Key)
	setKey := fmt.Sprintf("{%s}:%s:%s", tag, bs.cfg.Lock.DedupPrefix, block.Hash.Hex())
	ttlMs := strconv.FormatInt(int64(bs.cfg.Lock.BlockTTLSeconds*1000), 10)
	id := "*"

    fields := []string{
        "hash", block.Hash.Hex(),
        "number", strconv.FormatUint(block.Header.Number, 10),
        "payload", string(payload),
        "scanned_at_ms", strconv.FormatInt(time.Now().UnixMilli(), 10),
    }
	args := make([]interface{}, 0, 7+len(fields))
	args = append(args, "FCALL", "add_block", 2, setKey, bs.cfg.Streams.Key, ttlMs, id)
	for _, f := range fields {
		args = append(args, f)
	}

	var stored bool
    var lastReason string
    err = pattern.Retry(
        ctx,
        func(attempt int) error {
            res, err := bs.rdb.Do(ctx, args...).Result()
            if err != nil {
                bs.log.Warn("redis FCALL add_block failed", "attempt", attempt, "err", err)
                lastReason = "fcall"
                return apperr.NewBlockStoreErr("redis FCALL add_block failed", err)
            }

            arr, ok := res.([]interface{})
            if !ok || len(arr) < 1 {
                lastReason = "fcall_resp"
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
                        lastReason = "xadd_err"
                        return apperr.NewBlockStoreErr("redis add_block XADD failed", nil)
                    }
                }
            }
            lastReason = "unknown"
            return apperr.NewBlockStoreErr("redis add_block failed with unknown reason", nil)
        },
        pattern.WithMaxAttempts(3),
        pattern.WithInitialDelay(200*time.Millisecond),
        pattern.WithMaxDelay(1*time.Second),
    )
    if err != nil {
        if lastReason == "" {
            lastReason = "unknown"
        }
        imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentRedis, lastReason).Inc()
        return false, apperr.NewBlockStoreErr("redis FCALL add_block failed", err)
    }

    return stored, nil
}

func clusterHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start >= 0 {
		end := strings.IndexByte(key[start+1:], '}')
		if end >= 0 {
			tag := key[start+1 : start+1+end]
			if tag != "" {
				return tag
			}
		}
	}
	return key
}

// StorePublishedBlockHash writes a durable marker in Redis indicating that a
// block with the given hash has been successfully published to Kafka. Call this
// after a successful publication and before acknowledging the Redis stream message
// to achieve effectively once the semantics across a process crashes.
//
// It stores the marker under a key co-located with the stream's hash slot
// (e.g., "{blocks}:published:<hash>") using SET NX with an optional TTL. The
// return value is true when the key was created (first time) and false when the
// key already existed.
func (bs *BlockLogger) StorePublishedBlockHash(ctx context.Context, blockHash string) (bool, error) {
	if strings.TrimSpace(blockHash) == "" {
		return false, apperr.NewBlockStoreErr("empty block hash", nil)
	}

	tag := clusterHashTag(bs.cfg.Streams.Key)
	prefix := bs.cfg.Lock.DedupPublishBlockPrefix
	key := fmt.Sprintf("{%s}:%s:%s", tag, prefix, blockHash)

	ttlSeconds := bs.cfg.Lock.PublishBlockTTLSeconds
	var ttl time.Duration
	if ttlSeconds > 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	} else {
		ttl = 0
	}

    var created bool
    err := pattern.Retry(
        ctx,
        func(attempt int) error {
            c, err := bs.rdb.SetNX(ctx, key, "1", ttl).Result()
            if err != nil {
                bs.log.Warn("Redis SETNX published marker failed", "key", key, "attempt", attempt, "err", err)
                return err
            }
            created = c
            return nil
        },
        pattern.WithMaxAttempts(5),
        pattern.WithInitialDelay(100*time.Millisecond),
        pattern.WithMaxDelay(500*time.Millisecond),
        pattern.WithJitter(0.2),
    )
    if err != nil {
        return false, apperr.NewBlockStoreErr("failed to store published marker", err)
    }

	if created {
		bs.log.Trace("Stored published marker", "key", key)
	} else {
		bs.log.Trace("Published marker already exists", "key", key)
	}
	return created, nil
}

// IsBlockPublished checks whether a durable published marker exists for the
// given block hash. It returns true if the marker exists.
func (bs *BlockLogger) IsBlockPublished(ctx context.Context, blockHash string) (bool, error) {
	if strings.TrimSpace(blockHash) == "" {
		return false, apperr.NewBlockStoreErr("empty block hash", nil)
	}
	tag := clusterHashTag(bs.cfg.Streams.Key)
	prefix := bs.cfg.Lock.DedupPublishBlockPrefix
	key := fmt.Sprintf("{%s}:%s:%s", tag, prefix, blockHash)
	n, err := bs.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, apperr.NewBlockStoreErr("failed to check published marker", err)
	}
	return n == 1, nil
}
