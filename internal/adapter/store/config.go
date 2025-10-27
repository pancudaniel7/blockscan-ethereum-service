package block

// Config contains connection and behavior options for the Redis-backed
// BlockLogger. The struct is validated via go-playground/validator tags.
type Config struct {
	Host               string `validate:"required,hostname|ip"`
	Port               string `validate:"required,numeric"`
	Password           string
	DB                 int `validate:"gte=0"`
	UseTLS             bool
	PoolSize           int `validate:"gte=0"`
	MaxRetries         int `validate:"gte=0"`
	DialTimeoutSeconds int `validate:"gte=0"`
	// StreamKey is the Redis stream where blocks are published.
	StreamKey string `validate:"required"`
	// DedupPrefix is the prefix used for the idempotency SET key (e.g., "block").
	DedupPrefix string `validate:"required"`
	// BlockTTLSeconds is the TTL for the dedup key.
	BlockTTLSeconds int `validate:"required,gte=1"`
}
