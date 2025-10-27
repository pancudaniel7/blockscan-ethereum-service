package entity

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Block represents a denormalized view of an Ethereum block, capturing
// header fields, uncles, transactions, and withdrawals.
// Mapping from geth types should be implemented in adapters.
type Block struct {
	// Block hash (computed from header)
	Hash         common.Hash `validate:"required"`
	Header       Header      `validate:"required"`
	Uncles       []Header
	Transactions []Transaction
	Withdrawals  []Withdrawal
}

// Header is a subset representation for uncle headers.
type Header struct {
	ParentHash       common.Hash
	UncleHash        common.Hash
	Coinbase         common.Address
	Root             common.Hash
	TxHash           common.Hash
	ReceiptHash      common.Hash
	Bloom            types.Bloom
	Difficulty       *big.Int
	Number           uint64 `validate:"gte=0"`
	GasLimit         uint64
	GasUsed          uint64
	Time             uint64
	Extra            []byte
	MixDigest        common.Hash
	Nonce            types.BlockNonce
	BaseFee          *big.Int
	WithdrawalsHash  *common.Hash
	ParentBeaconRoot *common.Hash
	BlobGasUsed      *uint64
	ExcessBlobGas    *uint64
}

// Transaction captures key fields from go-ethereum transactions.
type Transaction struct {
	Hash                 common.Hash
	Type                 uint8
	To                   *common.Address
	Value                *big.Int
	Gas                  uint64
	GasPrice             *big.Int
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	Nonce                uint64
	Data                 []byte
	AccessList           types.AccessList
	ChainID              *big.Int
}

// Withdrawal represents a single withdrawal entry from a block (Shanghai+).
type Withdrawal struct {
	Index     uint64
	Validator uint64
	Address   common.Address
	Amount    uint64
}
