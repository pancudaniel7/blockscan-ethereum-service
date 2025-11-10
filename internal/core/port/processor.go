package port

import (
	"context"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-redis/redis/v8"
)

// ProcessorService coordinates the business workflow between scanners, stores, and publishers.
type ProcessorService interface {
	StoreBlock(ctx context.Context, block *types.Block) error
	ReadAndPublishBlock(ctx context.Context, msg redis.XMessage) error
}
