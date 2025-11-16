package port

import (
    "context"
    "github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
)

type Publisher interface {
    // PublishBlock sends the given block to the upstream transport.
    // Optional headers can be provided (e.g., source message id for de-dup).
    PublishBlock(ctx context.Context, block *entity.Block, headers map[string]string) error
}
