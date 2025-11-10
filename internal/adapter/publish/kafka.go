package publish

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go"

	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
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
	writer       *kafka.Writer
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

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1,
		ReadTimeout:  writeTimeout,
		WriteTimeout: writeTimeout,
		Transport: &kafka.Transport{
			ClientID: cfg.ClientID,
		},
		AllowAutoTopicCreation: false,
	}

	kp := &KafkaPublisher{
		log:          log,
		writer:       writer,
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
func (kp *KafkaPublisher) PublishBlock(block *entity.Block) error {
	if block == nil {
		return apperr.NewInvalidArgErr("block is required", nil)
	}

	payload, err := json.Marshal(block)
	if err != nil {
		kp.log.Error("Failed to marshal block payload", "err", err)
		return apperr.NewBlockStreamErr("failed to marshal block payload", err)
	}

	message := kp.buildMessage(block, payload)
	if err := pattern.Retry(context.Background(), func(attempt int) error {
		attemptCtx, cancel := context.WithTimeout(context.Background(), kp.writeTimeout)
		defer cancel()

		writeErr := kp.writer.WriteMessages(attemptCtx, message)
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

func (kp *KafkaPublisher) buildMessage(block *entity.Block, payload []byte) kafka.Message {
	blockTime := time.Now()
	if block.Header.Time > 0 {
		ts := int64(block.Header.Time)
		blockTime = time.Unix(ts, 0)
	}

	headers := []kafka.Header{
		{Key: "block-number", Value: []byte(strconv.FormatUint(block.Header.Number, 10))},
		{Key: "block-hash", Value: []byte(block.Hash.Hex())},
	}

	return kafka.Message{
		Key:     append([]byte(nil), block.Hash.Bytes()...),
		Value:   payload,
		Time:    blockTime,
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

	var batchErr kafka.WriteErrors
	if errors.As(err, &batchErr) {
		for _, inner := range batchErr {
			if inner == nil {
				continue
			}
			if !kp.shouldRetry(inner) {
				return false
			}
		}
		return len(batchErr) > 0
	}

	var kafkaErr kafka.Error
	if errors.As(err, &kafkaErr) {
		return kafkaErr.Temporary()
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
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
