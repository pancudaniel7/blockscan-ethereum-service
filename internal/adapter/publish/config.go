package publish

// Config captures the Kafka connectivity and retry behavior for the publisher.
type Config struct {
	Brokers               []string `validate:"required,min=1,dive,required"`
	Topic                 string   `validate:"required"`
	ClientID              string   `validate:"required"`
	TransactionalID       string   `validate:"omitempty"`
	MaxRetryAttempts      int      `validate:"omitempty,gte=1"`
	RetryInitialBackoffMS int      `validate:"omitempty,gte=0"`
	RetryMaxBackoffMS     int      `validate:"omitempty,gte=0"`
	RetryJitter           float64  `validate:"omitempty,gte=0"`
	WriteTimeoutSeconds   int      `validate:"omitempty,gte=1"`
}
