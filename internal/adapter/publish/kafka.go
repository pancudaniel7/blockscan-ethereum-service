package publish

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/usecase"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	imetrics "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/metrics"
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
	client       kgoClient
	cfg          Config
	writeTimeout time.Duration
	retryOpts    []pattern.RetryOption
}

type kgoClient interface {
	BeginTransaction() error
	EndTransaction(context.Context, kgo.TransactionEndTry) error
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

var newKgoClient = func(opts ...kgo.Opt) (kgoClient, error) { return kgo.NewClient(opts...) }

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

	if opt, ok := producerCompressionOpt(cfg.ProducerCompression); ok {
		opts = append(opts, opt)
	}
	if cfg.ProducerBatchMaxBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(cfg.ProducerBatchMaxBytes)))
	}
	client, err := newKgoClient(opts...)
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
        return apperr.NewBlockPublishErr("failed to marshal block payload", err)
    }

    size := len(payload)
    kp.log.Trace("Prepared block payload", "topic", kp.cfg.Topic, "hash", block.Hash.Hex(), "number", block.Header.Number, "value_bytes", size)
    if kp.cfg.MaxRecordBytes > 0 && size > kp.cfg.MaxRecordBytes {
        kp.log.Warn(
            "Kafka record rejected by size guard",
            "topic", kp.cfg.Topic,
            "hash", block.Hash.Hex(),
            "number", block.Header.Number,
            "value_bytes", size,
            "limit_bytes", kp.cfg.MaxRecordBytes,
        )
        return apperr.NewBlockPublishErr("payload exceeds max_record_bytes", nil)
    }

	rec := kp.buildRecord(block, payload, headers)
	if err := kp.publishWithRetry(ctx, rec, block); err != nil {
		return apperr.NewBlockPublishErr("failed to publish block to kafka", err)
	}

    kp.log.Trace("Published block to Kafka", "topic", kp.cfg.Topic, "hash", block.Hash.Hex(), "number", block.Header.Number, "value_bytes", size)
	return nil
}

func (kp *KafkaPublisher) buildRecord(block *entity.Block, payload []byte, extras map[string]string) *kgo.Record {
	headers := []kgo.RecordHeader{
		{Key: "block-number", Value: []byte(strconv.FormatUint(block.Header.Number, 10))},
		{Key: "block-hash", Value: []byte(block.Hash.Hex())},
	}
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

func (kp *KafkaPublisher) publishWithRetry(ctx context.Context, rec *kgo.Record, block *entity.Block) error {
	return pattern.Retry(ctx, func(attempt int) error {
		err := kp.produceOnce(ctx, rec)
		if err != nil {
			if kp.shouldRetry(err) {
				kp.log.Warn("Kafka publish attempt failed", "attempt", attempt, "hash", block.Hash.Hex(), "topic", kp.cfg.Topic, "err", err)
			} else {
				kp.log.Error("Kafka publish failed (non-retriable)", "hash", block.Hash.Hex(), "topic", kp.cfg.Topic, "err", err)
			}
		}
		return err
	}, kp.retryOpts...)
}

func (kp *KafkaPublisher) produceOnce(ctx context.Context, rec *kgo.Record) error {
    start := time.Now()
    if kp.cfg.TransactionalID != "" {
        if err := kp.client.BeginTransaction(); err != nil {
            imetrics.Kafka().ProduceLatencyMS.Observe(float64(time.Since(start).Milliseconds()))
            imetrics.Kafka().ProduceAttemptsTotal.Inc()
            imetrics.Kafka().ProduceErrorsTotal.WithLabelValues("txn_begin").Inc()
            return err
        }
    }

    attemptCtx, cancel := context.WithTimeout(ctx, kp.writeTimeout)
    defer cancel()

    res := kp.client.ProduceSync(attemptCtx, rec)
    writeErr := res.FirstErr()
    if kp.cfg.TransactionalID != "" {
        if writeErr == nil {
            if err := kp.client.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
                writeErr = err
            }
        } else {
            _ = kp.client.EndTransaction(context.Background(), kgo.TryAbort)
        }
    }

    imetrics.Kafka().ProduceLatencyMS.Observe(float64(time.Since(start).Milliseconds()))
    imetrics.Kafka().ProduceAttemptsTotal.Inc()
    if writeErr == nil {
        imetrics.Kafka().ProduceSuccessTotal.Inc()
    } else {
        imetrics.Kafka().ProduceErrorsTotal.WithLabelValues(classifyKafkaError(writeErr)).Inc()
    }
    return writeErr
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
		return true
	}

	if kerr.IsRetriable(err) {
		return true
	}
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

// producerCompressionOpt maps a human-friendly compression name to a
// corresponding kgo option. Returns (opt, true) when the name is recognized,
// otherwise (zero, false) meaning no compression override.
func producerCompressionOpt(name string) (kgo.Opt, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "gzip":
		return kgo.ProducerBatchCompression(kgo.GzipCompression()), true
	case "snappy":
		return kgo.ProducerBatchCompression(kgo.SnappyCompression()), true
	case "lz4":
		return kgo.ProducerBatchCompression(kgo.Lz4Compression()), true
	case "zstd", "":
		return kgo.ProducerBatchCompression(kgo.ZstdCompression()), true
	case "none":
		return nil, false
    default:
        return nil, false
    }
}

// classifyKafkaError returns a coarse error type for produce attempts.
func classifyKafkaError(err error) string {
    if err == nil {
        return "none"
    }
    if errors.Is(err, context.DeadlineExceeded) {
        return "timeout"
    }
    var netErr net.Error
    if errors.As(err, &netErr) {
        return "network"
    }
    if kerr.IsRetriable(err) {
        return "retriable"
    }
    if errors.Is(err, kerr.TopicAuthorizationFailed) || errors.Is(err, kerr.ClusterAuthorizationFailed) {
        return "auth"
    }
    return "other"
}
