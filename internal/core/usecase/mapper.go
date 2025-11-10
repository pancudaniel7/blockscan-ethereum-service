package usecase

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
)

// mapBlock converts a go-ethereum block into the internal entity representation.
// Nil inputs return nil for convenience when callers deal with optional blocks.
func mapBlock(block *types.Block) *entity.Block {
	if block == nil {
		return nil
	}

	return &entity.Block{
		Hash:         block.Hash(),
		Header:       mapHeader(block.Header()),
		Uncles:       mapHeaders(block.Uncles()),
		Transactions: mapTransactions(block.Transactions()),
		Withdrawals:  mapWithdrawals(block.Withdrawals()),
	}
}

func mapHeader(header *types.Header) entity.Header {
	if header == nil {
		return entity.Header{}
	}

	mapped := entity.Header{
		ParentHash:       header.ParentHash,
		UncleHash:        header.UncleHash,
		Coinbase:         header.Coinbase,
		Root:             header.Root,
		TxHash:           header.TxHash,
		ReceiptHash:      header.ReceiptHash,
		Bloom:            header.Bloom,
		Difficulty:       header.Difficulty,
		GasLimit:         header.GasLimit,
		GasUsed:          header.GasUsed,
		Time:             header.Time,
		Extra:            cloneBytes(header.Extra),
		MixDigest:        header.MixDigest,
		Nonce:            header.Nonce,
		BaseFee:          header.BaseFee,
		WithdrawalsHash:  header.WithdrawalsHash,
		ParentBeaconRoot: header.ParentBeaconRoot,
		BlobGasUsed:      header.BlobGasUsed,
		ExcessBlobGas:    header.ExcessBlobGas,
	}

	if header.Number != nil {
		mapped.Number = header.Number.Uint64()
	}

	return mapped
}

func mapHeaders(headers []*types.Header) []entity.Header {
	if len(headers) == 0 {
		return nil
	}

	result := make([]entity.Header, len(headers))
	for i, header := range headers {
		result[i] = mapHeader(header)
	}

	return result
}

func mapTransactions(txs types.Transactions) []entity.Transaction {
	if len(txs) == 0 {
		return nil
	}

	result := make([]entity.Transaction, len(txs))
	for i, tx := range txs {
		result[i] = entity.Transaction{
			Hash:                 tx.Hash(),
			Type:                 tx.Type(),
			To:                   tx.To(),
			Value:                tx.Value(),
			Gas:                  tx.Gas(),
			GasPrice:             tx.GasPrice(),
			MaxFeePerGas:         tx.GasFeeCap(),
			MaxPriorityFeePerGas: tx.GasTipCap(),
			Nonce:                tx.Nonce(),
			Data:                 cloneBytes(tx.Data()),
			AccessList:           cloneAccessList(tx.AccessList()),
			ChainID:              tx.ChainId(),
		}
	}

	return result
}

func mapWithdrawals(withdrawals types.Withdrawals) []entity.Withdrawal {
	if len(withdrawals) == 0 {
		return nil
	}

	result := make([]entity.Withdrawal, len(withdrawals))
	for i, withdrawal := range withdrawals {
		if withdrawal == nil {
			continue
		}
		result[i] = entity.Withdrawal{
			Index:     withdrawal.Index,
			Validator: withdrawal.Validator,
			Address:   withdrawal.Address,
			Amount:    withdrawal.Amount,
		}
	}

	return result
}

func cloneBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out
}

func cloneAccessList(list types.AccessList) types.AccessList {
	if len(list) == 0 {
		return nil
	}

	clone := make(types.AccessList, len(list))
	for i, entry := range list {
		clone[i] = types.AccessTuple{
			Address:     entry.Address,
			StorageKeys: cloneStorageKeys(entry.StorageKeys),
		}
	}
	return clone
}

func cloneStorageKeys(keys []common.Hash) []common.Hash {
	if len(keys) == 0 {
		return nil
	}

	copied := make([]common.Hash, len(keys))
	copy(copied, keys)
	return copied
}
