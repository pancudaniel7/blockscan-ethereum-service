package scan

// Config holds configuration for the Ethereum scan.
//
// WebSocketsURL is the Ethereum node endpoint used to subscribe to new heads
// (ws/wss) or to connect for HTTP polling (http/https). When FinalizedBlocks
// is true, the scan will poll for finalized blocks using the provided
// confirmation depth and delay.
type Config struct {
	WebSocketsURL          string `validate:"required,uri"`
	FinalizedBlocks        bool
	FinalizedPollDelay     uint64 `validate:"excluded_unless=FinalizedBlocks true,required,gte=5,lte=64"`
	FinalizedConfirmations uint64 `validate:"excluded_unless=FinalizedBlocks true,required,gte=32,lte=128"`
}
