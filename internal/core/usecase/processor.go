package usecase

import (
	"context"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/redis/go-redis/v9"
)

type BlockProcessorService struct {
	log          applog.AppLogger
	storeLogger  port.StoreLogger
	streamReader port.StoreStreamReader
	publisher    port.Publisher
}

func NewBlockProcessorService(log applog.AppLogger, storeLogger port.StoreLogger, streamReader port.StoreStreamReader, publisher port.Publisher) *BlockProcessorService {
	return &BlockProcessorService{log: log, storeLogger: storeLogger, streamReader: streamReader, publisher: publisher}
}

func (bps *BlockProcessorService) StoreBlock(ctx context.Context, block *types.Block) error {
	if block == nil {
		return apperr.NewBlockProcessErr("block is required", nil)
	}

	entityBlock := mapBlock(block)
	stored, err := bps.storeLogger.StoreBlock(ctx, entityBlock)
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	hash, payload, number, err := extractAllFields(msg)
	if err != nil {
		return apperr.NewBlockProcessErr("failed to extract fields from stream message", err)
	}

	block, err := UnmarshalBlockJSON([]byte(*payload))
	if err != nil {
		return apperr.NewBlockProcessErr("failed to unmarshal block payload", err)
	}

	if (block.Hash == common.Hash{}) && *hash != "" {
		block.Hash = common.HexToHash(*hash)
	}
	if block.Header.Number == 0 && *number != 0 {
		block.Header.Number = *number
	}

	// If this block was already published (e.g., previous run set the
	// durable marker), skip re-publishing and allow ack to proceed.
	if ok, err := bps.storeLogger.IsBlockPublished(ctx, block.Hash.Hex()); err != nil {
		return apperr.NewBlockProcessErr("failed to check published marker", err)
	} else if ok {
		bps.log.Trace("Skipping publish; marker exists", "hash", block.Hash.Hex(), "number", block.Header.Number)
		return nil
	}

	headers := map[string]string{"source-message-id": msg.ID}
	if err := bps.publisher.PublishBlock(ctx, block, headers); err != nil {
		bps.log.Error("failed to publish block", "hash", block.Hash.Hex(), "number", block.Header.Number, "err", err)
		return apperr.NewBlockProcessErr("failed to publish block", err)
	}

	// Record the durable marker before allowing ack to proceed.
	if _, err := bps.storeLogger.StorePublishedBlockHash(ctx, block.Hash.Hex()); err != nil {
		bps.log.Error("failed to store published marker", "hash", block.Hash.Hex(), "number", block.Header.Number, "err", err)
		return apperr.NewBlockProcessErr("failed to store published marker", err)
	}

	bps.log.Trace("Published block from Redis stream", "hash", block.Hash.Hex(), "number", block.Header.Number, "message_id", msg.ID)
	return nil
}

func extractAllFields(msg redis.XMessage) (*string, *string, *uint64, error) {
	hash, err := extractStringField(msg, "hash")
	if err != nil {
		return nil, nil, nil, err
	}

	if hash == "" {
		return nil, nil, nil, apperr.NewBlockProcessErr("stream message missing block hash", nil)
	}

	strMsgNum, err := extractStringField(msg, "number")
	if err != nil {
		return nil, nil, nil, err
	}

	if strMsgNum == "" {
		return nil, nil, nil, apperr.NewBlockProcessErr("stream message missing block number", nil)
	}

	number, err := strconv.ParseUint(strMsgNum, 10, 64)
	if err != nil {
		return nil, nil, nil, apperr.NewBlockProcessErr("invalid block number in stream message", err)
	}

	payload, err := extractStringField(msg, "payload")
	if err != nil {
		return nil, nil, nil, err
	}

	if payload == "" {
		return nil, nil, nil, apperr.NewBlockProcessErr("stream message missing block payload", nil)
	}
	return &hash, &payload, &number, nil
}

func extractStringField(msg redis.XMessage, key string) (string, error) {
	val, ok := msg.Values[key]
	if !ok {
		return "", apperr.NewBlockProcessErr("stream message missing field: "+key, nil)
	}

	switch v := val.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", apperr.NewBlockProcessErr("stream field "+key+" must be a string", nil)
	}
}
