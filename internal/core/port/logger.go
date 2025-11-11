package port

import (
	"context"

	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
)

// StoreLogger abstracts the persistence layer responsible for deduplicated block storage.
type StoreLogger interface {
	Store(ctx context.Context, block *entity.Block) (bool, error)
}
