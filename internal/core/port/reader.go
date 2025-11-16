package port

import (
	"context"

    "github.com/redis/go-redis/v9"
)

// StreamMessageHandler processes individual Redis stream entries.
type StreamMessageHandler func(context.Context, redis.XMessage) error

// StoreStreamReader abstracts the Redis stream reader.
type StoreStreamReader interface {
	SetHandler(handler StreamMessageHandler)
	StartReadFromStream() error
	StopReadFromStream()
}
