package store

// Config contains connection and behavior options for the Redis-backed
// BlockLogger. The struct is validated via go-playground/validator tags.
type Config struct {
	Host               string `validate:"required,hostname|ip"`
	Port               string `validate:"required,numeric"`
	Password           string
	DB                 int `validate:"gte=0"`
	UseTLS             bool
	PoolSize           int          `validate:"gte=0"`
	MaxRetries         int          `validate:"gte=0"`
	DialTimeoutSeconds int          `validate:"gte=0"`
	Streams            StreamConfig `validate:"required"`
	Lock               LockConfig   `validate:"required"`
}

// StreamConfig groups all stream-related settings such as keys and consumer behavior.
type StreamConfig struct {
	// Key is the Redis stream where blocks are published.
	Key string `validate:"required"`

	// ConsumerGroup identifies the Redis consumer group used for stream reads.
	ConsumerGroup string `validate:"required"`

	// ConsumerName identifies this instance within the consumer group.
	ConsumerName string `validate:"required"`

	// ReadCount limits the number of messages fetched per XREADGROUP call.
	ReadCount int `validate:"required,gte=1"`

	// ReadBlockTimeoutSeconds sets the blocking timeout used while waiting for new messages.
	// Must be >= 1s to avoid indefinite blocking or zero timeouts.
	ReadBlockTimeoutSeconds int `validate:"required,gte=1"`

	// ClaimIdleSeconds controls how long a message can stay pending before being reclaimed.
	ClaimIdleSeconds int `validate:"gte=0"`
}

// LockConfig encapsulates options for the deduplication lock used by the Lua script.
type LockConfig struct {
	// DedupPrefix is the prefix used for the idempotency SET key (e.g., "block").
	DedupPrefix string `validate:"required"`

	// BlockTTLSeconds is the TTL for the dedup key.
	BlockTTLSeconds int `validate:"required,gte=1"`
}
