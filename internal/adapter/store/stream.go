package store

import (
    "context"
    "crypto/tls"
    "errors"
    "net"
    "strconv"
    "strings"
    "sync"
    "time"

	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
    "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
    "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
    "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
    imetrics "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/metrics"
    "github.com/pingcap/failpoint"
    "github.com/redis/go-redis/v9"
)

// FPFailBeforeAck simulates a crash immediately before acknowledging a consumed
// stream message. Flow tests enable it to ensure pending entries are safely
// re-consumed by another replica and that publish remains idempotent.
const FPFailBeforeAck = "fail-before-ack"

// BlockStream encapsulates Redis stream reading new stream block entries.
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
		logger.Error("invalid redis config", "err", err)
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
		bs.mu.Unlock()
		return apperr.NewBlockStreamErr("stream handler is not configured", nil)
	}
	if bs.running {
		bs.mu.Unlock()
		return apperr.NewBlockStreamErr("block stream reader already running", nil)
	}

	consumerName := bs.cfg.Streams.ConsumerName

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
		defer bs.wg.Done()
		defer func() {
			bs.mu.Lock()
			bs.running = false
			bs.cancel = nil
			bs.mu.Unlock()
			bs.logger.Trace("Stopped reading from Redis stream", "stream", bs.cfg.Streams.Key)
		}()

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

		for {
			select {
			case <-streamCtx.Done():
				bs.logger.Trace("Stream reader stopping; unacked messages remain pending for retry", "stream", bs.cfg.Streams.Key, "group", bs.cfg.Streams.ConsumerGroup, "consumer", consumerName)
				return
			default:
			}
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
	bs.logger.Trace("Stopping Redis stream reader; pending messages remain available for re-consume", "stream", bs.cfg.Streams.Key, "group", bs.cfg.Streams.ConsumerGroup)
	cancel()
}

func (bs *BlockStream) processMessage(msg redis.XMessage) {
    handler := bs.handler
    if handler == nil {
        return
    }
    if err := handler(context.Background(), msg); err != nil {
        bs.logger.Error("Stream handler failed; leaving unacked for retry", "stream", bs.cfg.Streams.Key, "id", msg.ID, "err", err)
        return
    }
    failpoint.Inject(FPFailBeforeAck, func() {
        bs.logger.Fatal("failpoint triggered: fail-before-ack", "stream", bs.cfg.Streams.Key, "id", msg.ID)
    })
    bs.ackMessage(msg.ID)
    bs.observeEndToEndLatency(msg)
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
    imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentRedis, "group_create").Inc()
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

func (bs *BlockStream) drainPending(ctx context.Context, consumerName string, readCount int) error {
    for {
        streams, err := bs.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
            Group:    bs.cfg.Streams.ConsumerGroup,
            Consumer: consumerName,
            Streams:  []string{bs.cfg.Streams.Key, "0"},
            Count:    int64(readCount),
            Block:    0,
        }).Result()
        if err != nil {
            if isNoGroupErr(err) {
                if gerr := bs.ensureGroupWithRetry(ctx); gerr != nil {
                    return gerr
                }
                continue
            }
            if errors.Is(err, redis.Nil) {
                return nil
            }
            if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
                imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentRedis, "xreadgroup_ctx").Inc()
                return err
            }
            imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentRedis, "xreadgroup").Inc()
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
            if isNoGroupErr(err) {
                if gerr := bs.ensureGroupWithRetry(ctx); gerr != nil {
                    return gerr
                }
                start = "0-0"
                continue
            }
            imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentRedis, "xautoclaim").Inc()
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

func (bs *BlockStream) readNew(ctx context.Context, consumerName string, readCount int, blockTimeout time.Duration) error {
    streams, err := bs.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
        Group:    bs.cfg.Streams.ConsumerGroup,
        Consumer: consumerName,
        Streams:  []string{bs.cfg.Streams.Key, ">"},
        Count:    int64(readCount),
        Block:    blockTimeout,
    }).Result()
    if err != nil {
        if isNoGroupErr(err) {
            if gerr := bs.ensureGroupWithRetry(ctx); gerr != nil {
                return gerr
            }
            return nil
        }
        if errors.Is(err, redis.Nil) {
            return nil
        }
        imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentRedis, "xreadgroup").Inc()
        return err
    }
	for _, s := range streams {
		for _, m := range s.Messages {
			bs.processMessage(m)
		}
	}
	return nil
}

func (bs *BlockStream) ackMessage(id string) {
    if id == "" {
        return
    }
    shouldRetry := func(err error) bool {
        switch {
        case err == nil, errors.Is(err, context.Canceled):
            return false
        case errors.Is(err, context.DeadlineExceeded):
            return true
        default:
            return true
        }
    }
    err := pattern.Retry(
        context.Background(),
        func(attempt int) error {
            _, err := bs.rdb.XAck(context.Background(), bs.cfg.Streams.Key, bs.cfg.Streams.ConsumerGroup, id).Result()
            if err != nil {
                bs.logger.Warn("XACK failed; message remains pending for retry", "stream", bs.cfg.Streams.Key, "id", id, "attempt", attempt, "err", err)
            }
            return err
        },
        pattern.WithMaxAttempts(3),
        pattern.WithInitialDelay(100*time.Millisecond),
        pattern.WithMaxDelay(500*time.Millisecond),
        pattern.WithShouldRetry(shouldRetry),
    )
    if err != nil {
        imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentRedis, "xack").Inc()
    }
}

func isNoGroupErr(err error) bool {
    if err == nil {
        return false
    }
    return strings.Contains(strings.ToUpper(err.Error()), "NOGROUP")
}

// extractScannedAtMillis returns the enqueue timestamp in milliseconds since epoch
// if the stream message contains a "scanned_at_ms" field.
func extractScannedAtMillis(msg redis.XMessage) (int64, bool) {
    v, ok := msg.Values["scanned_at_ms"]
    if !ok {
        return 0, false
    }
    switch t := v.(type) {
    case string:
        n, err := strconv.ParseInt(t, 10, 64)
        if err != nil {
            return 0, false
        }
        return n, true
    case []byte:
        n, err := strconv.ParseInt(string(t), 10, 64)
        if err != nil {
            return 0, false
        }
        return n, true
    default:
        return 0, false
    }
}

// observeEndToEndLatency reads the scanned_at_ms field and records the pipeline latency metric.
func (bs *BlockStream) observeEndToEndLatency(msg redis.XMessage) {
    if scannedAt, ok := extractScannedAtMillis(msg); ok {
        now := time.Now().UnixMilli()
        d := now - scannedAt
        if d < 0 {
            d = 0
        }
        imetrics.Pipeline().EndToEndLatencyMS.Observe(float64(d))
    }
}
