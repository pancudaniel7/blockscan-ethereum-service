package publish

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/usecase"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
)

const (
	defaultRetryAttempts       = 5
	defaultRetryInitialBackoff = 200 * time.Millisecond
	defaultRetryMaxBackoff     = 2 * time.Second
	defaultRetryJitter         = 0.2
	defaultWriteTimeout        = 10 * time.Second
)

// KafkaPublisher represents the adapter responsible for publishing events to Kafka.
type KafkaPublisher struct {
	log          applog.AppLogger
	client       *kgo.Client
	cfg          Config
	writeTimeout time.Duration
	retryOpts    []pattern.RetryOption
}

// NewKafkaPublisher builds a Kafka-backed publisher with validated configuration and retry settings.
func NewKafkaPublisher(log applog.AppLogger, cfg Config, v *validator.Validate) (*KafkaPublisher, error) {
	if err := v.Struct(cfg); err != nil {
		return nil, apperr.NewInvalidArgErr("invalid kafka publisher config", err)
	}

	maxAttempts := cfg.MaxRetryAttempts
	if maxAttempts == 0 {
		maxAttempts = defaultRetryAttempts
	}

	initialBackoff := millisecondsOrDefault(cfg.RetryInitialBackoffMS, defaultRetryInitialBackoff)
	maxBackoff := millisecondsOrDefault(cfg.RetryMaxBackoffMS, defaultRetryMaxBackoff)
	if maxBackoff < initialBackoff {
		maxBackoff = initialBackoff
	}

	writeTimeout := secondsOrDefault(cfg.WriteTimeoutSeconds, defaultWriteTimeout)
	jitter := cfg.RetryJitter
	if jitter <= 0 {
		jitter = defaultRetryJitter
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	if cfg.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(cfg.TransactionalID))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, apperr.NewInvalidArgErr("failed to init kafka client", err)
	}

	kp := &KafkaPublisher{
		log:          log,
		client:       client,
		cfg:          cfg,
		writeTimeout: writeTimeout,
	}

	kp.retryOpts = []pattern.RetryOption{
		pattern.WithMaxAttempts(maxAttempts),
		pattern.WithInitialDelay(initialBackoff),
		pattern.WithMaxDelay(maxBackoff),
		pattern.WithJitter(jitter),
		pattern.WithShouldRetry(kp.shouldRetry),
	}

	return kp, nil
}

// PublishBlock serializes and publishes the given block into Kafka using an idempotent key and retries.
// Optional headers will be added to the record (e.g., source-message-id).
func (kp *KafkaPublisher) PublishBlock(ctx context.Context, block *entity.Block, headers map[string]string) error {
	if block == nil {
		return apperr.NewInvalidArgErr("block is required", nil)
	}

	payload, err := usecase.MarshalBlockJSON(block)
	if err != nil {
		kp.log.Error("Failed to marshal block payload", "err", err)
		return apperr.NewBlockStreamErr("failed to marshal block payload", err)
	}

	rec := kp.buildRecord(block, payload, headers)
	if err := pattern.Retry(ctx, func(attempt int) error {
		// For transactional producers, wrap each message in a short transaction.
		if kp.cfg.TransactionalID != "" {
			if err := kp.client.BeginTransaction(); err != nil {
				return err
			}
		}

		attemptCtx, cancel := context.WithTimeout(ctx, kp.writeTimeout)
		defer cancel()

		// Produce synchronously; FirstErr returns the first error in the batch.
		res := kp.client.ProduceSync(attemptCtx, rec)
		writeErr := res.FirstErr()
		if kp.cfg.TransactionalID != "" {
			// Try to commit if produce succeeded; abort on error.
			if writeErr == nil {
				if err := kp.client.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
					writeErr = err
				}
			} else {
				_ = kp.client.EndTransaction(context.Background(), kgo.TryAbort)
			}
		}

		if writeErr != nil {
			if kp.shouldRetry(writeErr) {
				kp.log.Warn("Kafka publish attempt failed", "attempt", attempt, "hash", block.Hash.Hex(), "topic", kp.cfg.Topic, "err", writeErr)
			} else {
				kp.log.Error("Kafka publish failed (non-retriable)", "hash", block.Hash.Hex(), "topic", kp.cfg.Topic, "err", writeErr)
			}
		}
		return writeErr
	}, kp.retryOpts...); err != nil {
		return apperr.NewBlockStreamErr("failed to publish block to kafka", err)
	}

	kp.log.Trace("Published block to Kafka", "topic", kp.cfg.Topic, "hash", block.Hash.Hex(), "number", block.Header.Number)
	return nil
}

func (kp *KafkaPublisher) buildRecord(block *entity.Block, payload []byte, extras map[string]string) *kgo.Record {
	// Timestamp left to broker (CreateTime / LogAppendTime), not set explicitly.

	headers := []kgo.RecordHeader{
		{Key: "block-number", Value: []byte(strconv.FormatUint(block.Header.Number, 10))},
		{Key: "block-hash", Value: []byte(block.Hash.Hex())},
	}

	// Append any extra headers provided by callers (e.g., source-message-id)
	if len(extras) > 0 {
		for k, v := range extras {
			if k == "" {
				continue
			}
			headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}

	return &kgo.Record{
		Topic:   kp.cfg.Topic,
		Key:     append([]byte(nil), block.Hash.Bytes()...),
		Value:   payload,
		Headers: headers,
	}
}

func (kp *KafkaPublisher) shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}

	// Treat broker-marked retriable errors as retryable (leader changes,
	// coordinator load, not enough replicas, etc.).
	if kerr.IsRetriable(err) {
		return true
	}

	// When the topic may be provisioned shortly after startup, temporarily
	// retry UnknownTopicOrPartition to allow the provisioner to catch up.
	if errors.Is(err, kerr.UnknownTopicOrPartition) {
		return true
	}
	return false
}

func millisecondsOrDefault(ms int, fallback time.Duration) time.Duration {
	if ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func secondsOrDefault(seconds int, fallback time.Duration) time.Duration {
	if seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}
