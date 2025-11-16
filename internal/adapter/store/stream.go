package store

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
	"github.com/redis/go-redis/v9"
)

// BlockStream encapsulates Redis stream reding new stream block entries.
// It mirrors the cache.Config options and uses the shared validator instance.
type BlockStream struct {
	rdb     *redis.Client
	logger  applog.AppLogger
	wg      *sync.WaitGroup
	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
	handler port.StreamMessageHandler
	cfg     Config
}

// NewBlockStream validates the Config, constructs a Redis client with optional
// TLS, and returns an initialized BlockStream.
func NewBlockStream(logger applog.AppLogger, wg *sync.WaitGroup, v *validator.Validate, cfg Config) (*BlockStream, error) {
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

	return &BlockStream{
		rdb:    rdb,
		logger: logger,
		wg:     wg,
		cfg:    cfg,
	}, nil
}

// SetHandler installs the callback used to process each Redis stream message.
// It must be invoked before StartReadFromStream; otherwise the reader returns
// an error.
func (bs *BlockStream) SetHandler(handler port.StreamMessageHandler) {
	bs.handler = handler
}

// StartReadFromStream ensures the consumer group exists and starts a goroutine
// that drains pending messages, reclaims stale ones from failed replicas, and
// then continuously reads new messages, dispatching each through the handler
// and acknowledging on success. The consumer name is made unique per process.
func (bs *BlockStream) StartReadFromStream() error {
	bs.mu.Lock()
	if bs.handler == nil {
		return apperr.NewBlockStreamErr("stream handler is not configured", nil)
	}
	if bs.running {
		bs.mu.Unlock()
		return apperr.NewBlockStreamErr("block stream reader already running", nil)
	}
	// Config struct is validated in NewBlockStream via go-playground/validator,
	// so no additional checks for consumer group/name are necessary here.

	// Use configured consumer name. Keeping it stable across restarts allows
	// draining this consumer's PEL after a crash.
	consumerName := bs.cfg.Streams.ConsumerName

	streamCtx, cancel := context.WithCancel(context.Background())
	bs.cancel = cancel
	bs.running = true
	bs.mu.Unlock()

	// Ensure a consumer group exists (create if missing) with retry until
	// the context is canceled. This covers initial Redis availability too.
	if err := bs.ensureGroupWithRetry(streamCtx); err != nil {
		cancel()
		bs.mu.Lock()
		bs.running = false
		bs.cancel = nil
		bs.mu.Unlock()
		return err
	}

	// Add the stream reader goroutine to the root WaitGroup so shutdown waits
	// for it to exit. This ensures a graceful shutdown: the current message
	// finishes processing and acks before the process terminates.
	bs.wg.Add(1)
	go func() {
		defer bs.wg.Done()
		defer func() {
			bs.mu.Lock()
			bs.running = false
			bs.cancel = nil
			bs.mu.Unlock()
			bs.logger.Trace("Stopped reading from Redis stream", "stream", bs.cfg.Streams.Key)
		}()

		// Ack handled via bs.ackMessage method.
		readCount := bs.cfg.Streams.ReadCount
		blockTimeout := time.Duration(bs.cfg.Streams.ReadBlockTimeoutSeconds) * time.Second
		claimIdle := time.Duration(bs.cfg.Streams.ClaimIdleSeconds) * time.Second

		bs.logger.Trace(
			"Starting Redis stream reader",
			"stream", bs.cfg.Streams.Key,
			"group", bs.cfg.Streams.ConsumerGroup,
			"consumer", consumerName,
			"count", readCount,
		)

		// processMessage moved to a helper to keep this loop concise.
		for {
			// Exit promptly if asked to stop (after the current message finishes).
			select {
			case <-streamCtx.Done():
				bs.logger.Trace("Stream context cancelled, shutting down reader", "stream", bs.cfg.Streams.Key)
				return
			default:
			}

			// 1) Drain this consumer's pending messages (non-blocking)
			if err := bs.drainPending(streamCtx, consumerName, readCount); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if !errors.Is(err, redis.Nil) {
					bs.logger.Warn("Failed to drain pending messages", "stream", bs.cfg.Streams.Key, "err", err)
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// 2) Reclaim stale messages from other consumers (failover)
			if claimIdle > 0 {
				if err := bs.reclaimStale(streamCtx, consumerName, readCount, claimIdle); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return
					}
					bs.logger.Warn("Failed to reclaim stale messages", "stream", bs.cfg.Streams.Key, "err", err)
					time.Sleep(500 * time.Millisecond)
					continue
				}
			}

			// 3) Read new messages (blocking with timeout)
			if err := bs.readNew(streamCtx, consumerName, readCount, blockTimeout); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if !errors.Is(err, redis.Nil) {
					bs.logger.Warn("Failed to read new messages", "stream", bs.cfg.Streams.Key, "err", err)
				}
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}
	}()

	return nil
}

// StopReadFromStream signals the StartReadFromStream goroutine to stop.
func (bs *BlockStream) StopReadFromStream() {
	bs.mu.Lock()
	if bs.cancel == nil {
		bs.mu.Unlock()
		return
	}
	cancel := bs.cancel
	bs.cancel = nil
	bs.mu.Unlock()
	bs.logger.Trace("Stopping Redis stream reader...", "stream", bs.cfg.Streams.Key)
	cancel()
}

// processMessage invokes the configured handler for the given message and
// acknowledges it on success. The handler is executed with a background
// context, so the last in-flight message can be complete during graceful shutdown.
func (bs *BlockStream) processMessage(msg redis.XMessage) {
	handler := bs.handler
	if handler == nil {
		return
	}
	if err := handler(context.Background(), msg); err != nil {
		bs.logger.Error("Stream handler failed", "stream", bs.cfg.Streams.Key, "id", msg.ID, "err", err)
		// Leave unacked; it remains pending for re-consume.
		return
	}
	bs.ackMessage(msg.ID)
}

// ensureConsumerGroup creates the consumer group if it does not exist. It is
// safe to call concurrently and idempotently initializes the stream key.
func (bs *BlockStream) ensureConsumerGroup(ctx context.Context) error {
	err := bs.rdb.XGroupCreateMkStream(ctx, bs.cfg.Streams.Key, bs.cfg.Streams.ConsumerGroup, "0").Err()
	if err == nil {
		bs.logger.Trace("Created Redis consumer group", "stream", bs.cfg.Streams.Key, "group", bs.cfg.Streams.ConsumerGroup)
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		bs.logger.Warn("Redis consumer group already exists", "stream", bs.cfg.Streams.Key, "group", bs.cfg.Streams.ConsumerGroup)
		return nil
	}
	return apperr.NewBlockStreamErr("failed to ensure consumer group", err)
}

// ensureGroupWithRetry keeps attempting to create/verify the consumer group
// until it succeeds or the provided context is canceled.
func (bs *BlockStream) ensureGroupWithRetry(ctx context.Context) error {
	return pattern.Retry(
		ctx,
		func(attempt int) error {
			err := bs.ensureConsumerGroup(ctx)
			if err != nil {
				bs.logger.Warn(
					"Failed to ensure consumer group",
					"stream", bs.cfg.Streams.Key,
					"group", bs.cfg.Streams.ConsumerGroup,
					"attempt", attempt,
					"err", err,
				)
			}
			return err
		},
		pattern.WithInfiniteAttempts(),
		pattern.WithInitialDelay(500*time.Millisecond),
		pattern.WithMaxDelay(5*time.Second),
		pattern.WithMultiplier(2.0),
		pattern.WithJitter(0.2),
	)
}

// drainPending drains this consumer's pending messages without blocking.
// Returns nil when the backlog is empty, context errors to stop the reader,
// or any other error encountered during the read.
func (bs *BlockStream) drainPending(ctx context.Context, consumerName string, readCount int) error {
	for {
		streams, err := bs.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    bs.cfg.Streams.ConsumerGroup,
			Consumer: consumerName,
			Streams:  []string{bs.cfg.Streams.Key, "0"},
			Count:    int64(readCount),
			Block:    -1, // omit BLOCK so this call is non-blocking
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return err
		}
		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			return nil
		}
		for _, s := range streams {
			for _, m := range s.Messages {
				bs.processMessage(m)
			}
		}
	}
}

// reclaimStale uses XAUTOCLAIM to take ownership of messages that have been
// pending on other consumers longer than minIdle. It processes messages and
// stops when the claim cursor is exhausted.
func (bs *BlockStream) reclaimStale(ctx context.Context, consumerName string, readCount int, minIdle time.Duration) error {
	start := "0-0"
	for {
		msgs, next, err := bs.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   bs.cfg.Streams.Key,
			Group:    bs.cfg.Streams.ConsumerGroup,
			Consumer: consumerName,
			MinIdle:  minIdle,
			Start:    start,
			Count:    int64(readCount),
		}).Result()
		if err != nil {
			return err
		}
		for _, m := range msgs {
			bs.processMessage(m)
		}
		if len(msgs) == 0 || next == "" || next == start {
			return nil
		}
		start = next
	}
}

// readNew blocks for up to blockTimeout to read new messages for this consumer
// and processes them when available. Returns redis.Nil when no messages were
// delivered in the given timeout.
func (bs *BlockStream) readNew(ctx context.Context, consumerName string, readCount int, blockTimeout time.Duration) error {
	streams, err := bs.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    bs.cfg.Streams.ConsumerGroup,
		Consumer: consumerName,
		Streams:  []string{bs.cfg.Streams.Key, ">"},
		Count:    int64(readCount),
		Block:    blockTimeout,
	}).Result()
	if err != nil {
		return err
	}
	for _, s := range streams {
		for _, m := range s.Messages {
			bs.processMessage(m)
		}
	}
	return nil
}

// ackMessage acknowledges a message by ID with small bounded retries.
func (bs *BlockStream) ackMessage(id string) {
	if id == "" {
		return
	}
	shouldRetry := func(err error) bool {
		if err == nil {
			return false
		}
		if errors.Is(err, context.Canceled) {
			return false
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return true
		}
		return true
	}
	_ = pattern.Retry(
		context.Background(),
		func(attempt int) error {
			_, err := bs.rdb.XAck(context.Background(), bs.cfg.Streams.Key, bs.cfg.Streams.ConsumerGroup, id).Result()
			if err != nil {
				bs.logger.Warn("XACK failed", "stream", bs.cfg.Streams.Key, "id", id, "attempt", attempt, "err", err)
			}
			return err
		},
		pattern.WithMaxAttempts(3),
		pattern.WithInitialDelay(100*time.Millisecond),
		pattern.WithMaxDelay(500*time.Millisecond),
		pattern.WithShouldRetry(shouldRetry),
	)
}
