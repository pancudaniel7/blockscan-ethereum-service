package usecase

import (
	"context"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-redis/redis/v8"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

type BlockProcessorService struct {
	log         applog.AppLogger
	storeLogger port.StoreLogger
	publisher   port.Publisher
}

func NewBlockProcessorService(log applog.AppLogger, storeLogger port.StoreLogger, publisher port.Publisher) *BlockProcessorService {
	return &BlockProcessorService{log: log, storeLogger: storeLogger, publisher: publisher}
}

func (bps *BlockProcessorService) StoreBlock(ctx context.Context, block *types.Block) error {
	if block == nil {
		return apperr.NewInvalidArgErr("block is required", nil)
	}

	entityBlock := mapBlock(block)
	stored, err := bps.storeLogger.Store(ctx, entityBlock)
	if err != nil {
		bps.log.Error("failed to storeLogger block", "number", entityBlock.Header.Number, "hash", entityBlock.Hash.Hex(), "err", err)
		return apperr.NewBlockProcessErr("failed to storeLogger block", err)
	}

	if stored {
		bps.log.Info("Stored block", "number", entityBlock.Header.Number, "hash", entityBlock.Hash.Hex())
	} else {
		bps.log.Warn("Block already stored (dedup hit)", "number", entityBlock.Header.Number, "hash", entityBlock.Hash.Hex())
	}

	return nil
}

func (bps *BlockProcessorService) ReadAndPublishBlock(ctx context.Context, msg redis.XMessage) error {

	return nil
}
