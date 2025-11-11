package usecase

import (
	"encoding/hex"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/entity"
)

// BlockDTO is a JSON-friendly representation of entity.Block.
type BlockDTO struct {
	Hash         string           `json:"hash,omitempty"`
	Header       HeaderDTO        `json:"header"`
	Uncles       []HeaderDTO      `json:"uncles,omitempty"`
	Transactions []TransactionDTO `json:"transactions,omitempty"`
	Withdrawals  []WithdrawalDTO  `json:"withdrawals,omitempty"`
}

type HeaderDTO struct {
	ParentHash       string  `json:"parentHash,omitempty"`
	UncleHash        string  `json:"uncleHash,omitempty"`
	Coinbase         string  `json:"coinbase,omitempty"`
	Root             string  `json:"root,omitempty"`
	TxHash           string  `json:"txHash,omitempty"`
	ReceiptHash      string  `json:"receiptHash,omitempty"`
	Bloom            string  `json:"bloom,omitempty"`
	Difficulty       string  `json:"difficulty,omitempty"`
	Number           uint64  `json:"number"`
	GasLimit         uint64  `json:"gasLimit,omitempty"`
	GasUsed          uint64  `json:"gasUsed,omitempty"`
	Time             uint64  `json:"time,omitempty"`
	Extra            string  `json:"extra,omitempty"`
	MixDigest        string  `json:"mixDigest,omitempty"`
	Nonce            string  `json:"nonce,omitempty"`
	BaseFee          string  `json:"baseFee,omitempty"`
	WithdrawalsHash  string  `json:"withdrawalsHash,omitempty"`
	ParentBeaconRoot string  `json:"parentBeaconRoot,omitempty"`
	BlobGasUsed      *uint64 `json:"blobGasUsed,omitempty"`
	ExcessBlobGas    *uint64 `json:"excessBlobGas,omitempty"`
}

type TransactionDTO struct {
	Hash                 string           `json:"hash"`
	Type                 uint8            `json:"type"`
	To                   string           `json:"to,omitempty"`
	Value                string           `json:"value,omitempty"`
	Gas                  uint64           `json:"gas"`
	GasPrice             string           `json:"gasPrice,omitempty"`
	MaxFeePerGas         string           `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string           `json:"maxPriorityFeePerGas,omitempty"`
	Nonce                uint64           `json:"nonce"`
	Data                 string           `json:"data,omitempty"`
	AccessList           []AccessTupleDTO `json:"accessList,omitempty"`
	ChainID              string           `json:"chainId,omitempty"`
}

type AccessTupleDTO struct {
	Address     string   `json:"address"`
	StorageKeys []string `json:"storageKeys,omitempty"`
}

type WithdrawalDTO struct {
	Index     uint64 `json:"index"`
	Validator uint64 `json:"validator"`
	Address   string `json:"address"`
	Amount    uint64 `json:"amount"`
}

// ToDTO converts an entity.Block to a BlockDTO with JSON-friendly fields.
func ToDTO(b *entity.Block) *BlockDTO {
	if b == nil {
		return nil
	}
	dto := &BlockDTO{
		Hash:         b.Hash.Hex(),
		Header:       headerToDTO(b.Header),
		Uncles:       headersToDTO(b.Uncles),
		Transactions: txsToDTO(b.Transactions),
		Withdrawals:  withdrawalsToDTO(b.Withdrawals),
	}
	return dto
}

// FromDTO converts a BlockDTO back into an entity.Block.
func FromDTO(d *BlockDTO) *entity.Block {
	if d == nil {
		return nil
	}
	return &entity.Block{
		Hash:         common.HexToHash(d.Hash),
		Header:       headerFromDTO(d.Header),
		Uncles:       headersFromDTO(d.Uncles),
		Transactions: txsFromDTO(d.Transactions),
		Withdrawals:  withdrawalsFromDTO(d.Withdrawals),
	}
}

func headerToDTO(h entity.Header) HeaderDTO {
	var dto HeaderDTO
	dto.ParentHash = h.ParentHash.Hex()
	dto.UncleHash = h.UncleHash.Hex()
	dto.Coinbase = h.Coinbase.Hex()
	dto.Root = h.Root.Hex()
	dto.TxHash = h.TxHash.Hex()
	dto.ReceiptHash = h.ReceiptHash.Hex()
	// Bloom as hex
	dto.Bloom = "0x" + hex.EncodeToString(h.Bloom[:])
	if h.Difficulty != nil {
		dto.Difficulty = h.Difficulty.String()
	}
	dto.Number = h.Number
	dto.GasLimit = h.GasLimit
	dto.GasUsed = h.GasUsed
	dto.Time = h.Time
	if len(h.Extra) > 0 {
		dto.Extra = "0x" + hex.EncodeToString(h.Extra)
	}
	dto.MixDigest = h.MixDigest.Hex()
	// Nonce as hex
	dto.Nonce = "0x" + hex.EncodeToString(h.Nonce[:])
	if h.BaseFee != nil {
		dto.BaseFee = h.BaseFee.String()
	}
	if h.WithdrawalsHash != nil {
		dto.WithdrawalsHash = h.WithdrawalsHash.Hex()
	}
	if h.ParentBeaconRoot != nil {
		dto.ParentBeaconRoot = h.ParentBeaconRoot.Hex()
	}
	dto.BlobGasUsed = h.BlobGasUsed
	dto.ExcessBlobGas = h.ExcessBlobGas
	return dto
}

func headersToDTO(hs []entity.Header) []HeaderDTO {
	if len(hs) == 0 {
		return nil
	}
	out := make([]HeaderDTO, len(hs))
	for i, h := range hs {
		out[i] = headerToDTO(h)
	}
	return out
}

func txsToDTO(txs []entity.Transaction) []TransactionDTO {
	if len(txs) == 0 {
		return nil
	}
	out := make([]TransactionDTO, len(txs))
	for i, tx := range txs {
		out[i] = TransactionDTO{
			Hash:                 tx.Hash.Hex(),
			Type:                 tx.Type,
			To:                   addrPtrHex(tx.To),
			Value:                bigToString(tx.Value),
			Gas:                  tx.Gas,
			GasPrice:             bigToString(tx.GasPrice),
			MaxFeePerGas:         bigToString(tx.MaxFeePerGas),
			MaxPriorityFeePerGas: bigToString(tx.MaxPriorityFeePerGas),
			Nonce:                tx.Nonce,
			Data:                 bytesToHex(tx.Data),
			AccessList:           accessListToDTO(tx.AccessList),
			ChainID:              bigToString(tx.ChainID),
		}
	}
	return out
}

func withdrawalsToDTO(ws []entity.Withdrawal) []WithdrawalDTO {
	if len(ws) == 0 {
		return nil
	}
	out := make([]WithdrawalDTO, len(ws))
	for i, w := range ws {
		out[i] = WithdrawalDTO{
			Index:     w.Index,
			Validator: w.Validator,
			Address:   w.Address.Hex(),
			Amount:    w.Amount,
		}
	}
	return out
}

func headerFromDTO(d HeaderDTO) entity.Header {
	var h entity.Header
	h.ParentHash = common.HexToHash(d.ParentHash)
	h.UncleHash = common.HexToHash(d.UncleHash)
	h.Coinbase = common.HexToAddress(d.Coinbase)
	h.Root = common.HexToHash(d.Root)
	h.TxHash = common.HexToHash(d.TxHash)
	h.ReceiptHash = common.HexToHash(d.ReceiptHash)
	if s := trim0x(d.Bloom); s != "" {
		if b, err := hex.DecodeString(s); err == nil && len(b) == len(h.Bloom) {
			copy(h.Bloom[:], b)
		}
	}
	h.Difficulty = stringToBig(d.Difficulty)
	h.Number = d.Number
	h.GasLimit = d.GasLimit
	h.GasUsed = d.GasUsed
	h.Time = d.Time
	if s := trim0x(d.Extra); s != "" {
		if b, err := hex.DecodeString(s); err == nil {
			h.Extra = make([]byte, len(b))
			copy(h.Extra, b)
		}
	}
	h.MixDigest = common.HexToHash(d.MixDigest)
	if s := trim0x(d.Nonce); s != "" {
		if b, err := hex.DecodeString(s); err == nil {
			var nonce types.BlockNonce
			copy(nonce[:], b)
			h.Nonce = nonce
		}
	}
	h.BaseFee = stringToBig(d.BaseFee)
	if d.WithdrawalsHash != "" {
		wh := common.HexToHash(d.WithdrawalsHash)
		h.WithdrawalsHash = &wh
	}
	if d.ParentBeaconRoot != "" {
		pbr := common.HexToHash(d.ParentBeaconRoot)
		h.ParentBeaconRoot = &pbr
	}
	h.BlobGasUsed = d.BlobGasUsed
	h.ExcessBlobGas = d.ExcessBlobGas
	return h
}

func headersFromDTO(ds []HeaderDTO) []entity.Header {
	if len(ds) == 0 {
		return nil
	}
	out := make([]entity.Header, len(ds))
	for i, d := range ds {
		out[i] = headerFromDTO(d)
	}
	return out
}

func txsFromDTO(ds []TransactionDTO) []entity.Transaction {
	if len(ds) == 0 {
		return nil
	}
	out := make([]entity.Transaction, len(ds))
	for i, d := range ds {
		var toPtr *common.Address
		if d.To != "" {
			addr := common.HexToAddress(d.To)
			toPtr = &addr
		}
		out[i] = entity.Transaction{
			Hash:                 common.HexToHash(d.Hash),
			Type:                 d.Type,
			To:                   toPtr,
			Value:                stringToBig(d.Value),
			Gas:                  d.Gas,
			GasPrice:             stringToBig(d.GasPrice),
			MaxFeePerGas:         stringToBig(d.MaxFeePerGas),
			MaxPriorityFeePerGas: stringToBig(d.MaxPriorityFeePerGas),
			Nonce:                d.Nonce,
			Data:                 hexToBytes(d.Data),
			AccessList:           accessListFromDTO(d.AccessList),
			ChainID:              stringToBig(d.ChainID),
		}
	}
	return out
}

func withdrawalsFromDTO(ds []WithdrawalDTO) []entity.Withdrawal {
	if len(ds) == 0 {
		return nil
	}
	out := make([]entity.Withdrawal, len(ds))
	for i, d := range ds {
		out[i] = entity.Withdrawal{
			Index:     d.Index,
			Validator: d.Validator,
			Address:   common.HexToAddress(d.Address),
			Amount:    d.Amount,
		}
	}
	return out
}

func accessListToDTO(list types.AccessList) []AccessTupleDTO {
	if len(list) == 0 {
		return nil
	}
	out := make([]AccessTupleDTO, len(list))
	for i, t := range list {
		keys := make([]string, len(t.StorageKeys))
		for j, k := range t.StorageKeys {
			keys[j] = k.Hex()
		}
		out[i] = AccessTupleDTO{Address: t.Address.Hex(), StorageKeys: keys}
	}
	return out
}

func accessListFromDTO(list []AccessTupleDTO) types.AccessList {
	if len(list) == 0 {
		return nil
	}
	out := make(types.AccessList, len(list))
	for i, t := range list {
		keys := make([]common.Hash, len(t.StorageKeys))
		for j, k := range t.StorageKeys {
			keys[j] = common.HexToHash(k)
		}
		out[i] = types.AccessTuple{Address: common.HexToAddress(t.Address), StorageKeys: keys}
	}
	return out
}

func bigToString(b *big.Int) string {
	if b == nil {
		return ""
	}
	return b.String()
}

func stringToBig(s string) *big.Int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	// decimal base
	if bi, ok := new(big.Int).SetString(s, 10); ok {
		return bi
	}
	// hex with 0x prefix
	s2 := trim0x(s)
	if s2 != "" {
		if b, err := hex.DecodeString(s2); err == nil {
			return new(big.Int).SetBytes(b)
		}
	}
	return nil
}

func bytesToHex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return "0x" + hex.EncodeToString(b)
}

func hexToBytes(s string) []byte {
	s = trim0x(s)
	if s == "" {
		return nil
	}
	if b, err := hex.DecodeString(s); err == nil {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	return nil
}

func addrPtrHex(a *common.Address) string {
	if a == nil {
		return ""
	}
	return a.Hex()
}

func trim0x(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if strings.HasPrefix(s, "0x") {
		return s[2:]
	}
	return s
}
