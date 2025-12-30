package scan

// Config holds configuration for the Ethereum scan.
//
// WebSocketsURL is the Ethereum node endpoint used to subscribe to new heads
// (ws/wss) or to connect for HTTP polling (http/https). When FinalizedBlocks
// is true, the scan will poll for finalized blocks using the provided
// confirmation depth and delay.
type Config struct {
    WebSocketsURL   string `validate:"required,uri"`
    FinalizedBlocks bool
    // When FinalizedBlocks is false, these are optional and ignored.
    // Validate only if FinalizedBlocks=true; otherwise allow zero values.
    FinalizedPollDelay     uint64 `validate:"omitempty,required_if=FinalizedBlocks true,gte=5,lte=64"`
    FinalizedConfirmations uint64 `validate:"omitempty,required_if=FinalizedBlocks true,gte=32,lte=128"`
    // Dial backoff/retry settings for reconnects. When zero, sensible defaults
    // are applied and attempts are infinite.
    DialMaxRetryAttempts      int     `validate:"omitempty,gte=0"`
    DialRetryInitialBackoffMS int     `validate:"omitempty,gte=0"`
    DialRetryMaxBackoffMS     int     `validate:"omitempty,gte=0"`
    DialRetryJitter           float64 `validate:"omitempty,gte=0"`
}
