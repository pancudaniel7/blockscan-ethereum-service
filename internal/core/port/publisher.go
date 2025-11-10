package port

import "github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"

type Publisher interface {
	PublishBlock(block *entity.Block) error
}
