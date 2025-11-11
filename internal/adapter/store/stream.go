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
	"github.com/go-redis/redis/v8"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
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

func (bs *BlockStream) StartReadFromStream() error {
	bs.mu.Lock()
	if bs.handler == nil {
		return apperr.NewBlockStreamErr("stream handler is not configured", nil)
	}

	if bs.running {
		bs.mu.Unlock()
		return apperr.NewBlockStreamErr("block stream reader already running", nil)
	}

	if bs.cfg.Streams.ConsumerGroup == "" || bs.cfg.Streams.ConsumerName == "" {
		bs.mu.Unlock()
		return apperr.NewBlockStreamErr("consumer group and consumer name must be configured", nil)
	}

	handler := bs.handler
	if handler == nil {
		bs.mu.Unlock()
		return apperr.NewBlockStreamErr("stream handler is not configured", nil)
	}

	streamCtx, cancel := context.WithCancel(context.Background())
	bs.cancel = cancel
	bs.running = true
	bs.mu.Unlock()

	if err := bs.ensureGroupWithRetry(streamCtx); err != nil {
		cancel()
		bs.mu.Lock()
		bs.running = false
		bs.cancel = nil
		bs.mu.Unlock()
		return err
	}

	bs.wg.Add(1)
	go func() {
		if bs.wg != nil {
			defer bs.wg.Done()
		}
		defer func() {
			bs.mu.Lock()
			bs.running = false
			bs.cancel = nil
			bs.mu.Unlock()
			bs.logger.Trace("Stopped reading from Redis stream", "stream", bs.cfg.Streams.Key)
		}()

		bs.runReader(streamCtx, handler)
	}()

	return nil
}

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

func (bs *BlockStream) runReader(ctx context.Context, handler port.StreamMessageHandler) {
	readCount := bs.cfg.Streams.ReadCount
	if readCount <= 0 {
		readCount = 1
	}

	blockTimeout := time.Duration(bs.cfg.Streams.ReadBlockTimeoutSeconds) * time.Second
	if blockTimeout <= 0 {
		blockTimeout = 5 * time.Second
	}

	pendingArgs := &redis.XReadGroupArgs{
		Group:    bs.cfg.Streams.ConsumerGroup,
		Consumer: bs.cfg.Streams.ConsumerName,
		Streams:  []string{bs.cfg.Streams.Key, "0"},
		Count:    int64(readCount),
		Block:    0,
	}
	activeArgs := &redis.XReadGroupArgs{
		Group:    bs.cfg.Streams.ConsumerGroup,
		Consumer: bs.cfg.Streams.ConsumerName,
		Streams:  []string{bs.cfg.Streams.Key, ">"},
		Count:    int64(readCount),
		Block:    blockTimeout,
	}

	claimIdle := time.Duration(bs.cfg.Streams.ClaimIdleSeconds) * time.Second

	bs.logger.Trace(
		"Starting Redis stream reader",
		"stream", bs.cfg.Streams.Key,
		"group", bs.cfg.Streams.ConsumerGroup,
		"consumer", bs.cfg.Streams.ConsumerName,
		"count", readCount,
	)

	for {
		select {
		case <-ctx.Done():
			bs.logger.Trace("Stream context cancelled, shutting down reader", "stream", bs.cfg.Streams.Key)
			return
		default:
		}

		if err := bs.consumeMessages(ctx, pendingArgs, handler, true); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if !errors.Is(err, redis.Nil) {
				bs.logger.Warn("Failed to drain pending messages", "stream", bs.cfg.Streams.Key, "err", err)
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if claimIdle > 0 {
			if err := bs.claimStaleMessages(ctx, handler, claimIdle, int64(readCount)); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				bs.logger.Warn("Failed to reclaim stale messages", "stream", bs.cfg.Streams.Key, "err", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}

		if err := bs.consumeMessages(ctx, activeArgs, handler, false); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			if !errors.Is(err, redis.Nil) {
				bs.logger.Warn("Failed to read new messages", "stream", bs.cfg.Streams.Key, "err", err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (bs *BlockStream) consumeMessages(ctx context.Context, args *redis.XReadGroupArgs, handler port.StreamMessageHandler, drainPending bool) error {
	for {
		streams, err := bs.rdb.XReadGroup(ctx, args).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			return err
		}

		if len(streams) == 0 {
			if drainPending {
				return nil
			}
			continue
		}

		bs.dispatchMessages(ctx, streams, handler)
		if drainPending {
			continue
		}
		return nil
	}
}

func (bs *BlockStream) claimStaleMessages(ctx context.Context, handler port.StreamMessageHandler, minIdle time.Duration, count int64) error {
	start := "0-0"

	for {
		msgs, next, err := bs.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   bs.cfg.Streams.Key,
			Group:    bs.cfg.Streams.ConsumerGroup,
			Consumer: bs.cfg.Streams.ConsumerName,
			MinIdle:  minIdle,
			Start:    start,
			Count:    count,
		}).Result()
		if err != nil {
			return err
		}

		for _, msg := range msgs {
			if err := handler(ctx, msg); err != nil {
				bs.logger.Error("Handler failed for claimed message", "stream", bs.cfg.Streams.Key, "id", msg.ID, "err", err)
				continue
			}
			if err := bs.ackMessage(ctx, msg.ID); err != nil {
				bs.logger.Error("Failed to ack claimed message", "stream", bs.cfg.Streams.Key, "id", msg.ID, "err", err)
			}
		}

		if len(msgs) == 0 || next == "" || next == start {
			return nil
		}
		start = next
	}
}

func (bs *BlockStream) dispatchMessages(ctx context.Context, streams []redis.XStream, handler port.StreamMessageHandler) {
	for _, stream := range streams {
		for _, message := range stream.Messages {
			if err := handler(ctx, message); err != nil {
				bs.logger.Error("Stream handler failed", "stream", stream.Stream, "id", message.ID, "err", err)
				continue
			}
			if err := bs.ackMessage(ctx, message.ID); err != nil {
				bs.logger.Error("Failed to acknowledge message", "stream", stream.Stream, "id", message.ID, "err", err)
			}
		}
	}
}

func (bs *BlockStream) ackMessage(ctx context.Context, messageID string) error {
	if messageID == "" {
		return nil
	}
	_, err := bs.rdb.XAck(ctx, bs.cfg.Streams.Key, bs.cfg.Streams.ConsumerGroup, messageID).Result()
	return err
}
