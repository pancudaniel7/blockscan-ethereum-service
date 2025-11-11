package usecase

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strings"

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

// ----- JSON mapping helpers (mirroring entity JSON transforms) -----

type headerJSON struct {
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

type txJSON struct {
	Hash                 string            `json:"hash"`
	Type                 uint8             `json:"type"`
	To                   string            `json:"to,omitempty"`
	Value                string            `json:"value,omitempty"`
	Gas                  uint64            `json:"gas"`
	GasPrice             string            `json:"gasPrice,omitempty"`
	MaxFeePerGas         string            `json:"maxFeePerGas,omitempty"`
	MaxPriorityFeePerGas string            `json:"maxPriorityFeePerGas,omitempty"`
	Nonce                uint64            `json:"nonce"`
	Data                 string            `json:"data,omitempty"`
	AccessList           []accessTupleJSON `json:"accessList,omitempty"`
	ChainID              string            `json:"chainId,omitempty"`
}

type accessTupleJSON struct {
	Address     string   `json:"address"`
	StorageKeys []string `json:"storageKeys,omitempty"`
}

type withdrawalJSON struct {
	Index     uint64 `json:"index"`
	Validator uint64 `json:"validator"`
	Address   string `json:"address"`
	Amount    uint64 `json:"amount"`
}

type blockJSON struct {
	Hash         string           `json:"hash,omitempty"`
	Header       headerJSON       `json:"header"`
	Uncles       []headerJSON     `json:"uncles,omitempty"`
	Transactions []txJSON         `json:"transactions,omitempty"`
	Withdrawals  []withdrawalJSON `json:"withdrawals,omitempty"`
}

// MarshalBlockJSON encodes entity.Block into JSON with friendly encodings.
func MarshalBlockJSON(b *entity.Block) ([]byte, error) {
	if b == nil {
		return json.Marshal(nil)
	}
	bj := blockJSON{
		Hash:         b.Hash.Hex(),
		Header:       headerToJSON(b.Header),
		Uncles:       headersToJSON(b.Uncles),
		Transactions: txsToJSON(b.Transactions),
		Withdrawals:  withdrawalsToJSON(b.Withdrawals),
	}
	return json.Marshal(bj)
}

// UnmarshalBlockJSON decodes JSON into an entity.Block with proper conversions.
func UnmarshalBlockJSON(data []byte) (*entity.Block, error) {
	var bj blockJSON
	if err := json.Unmarshal(data, &bj); err != nil {
		return nil, err
	}
	blk := &entity.Block{
		Hash:         common.HexToHash(bj.Hash),
		Header:       headerFromJSON(bj.Header),
		Uncles:       headersFromJSON(bj.Uncles),
		Transactions: txsFromJSON(bj.Transactions),
		Withdrawals:  withdrawalsFromJSON(bj.Withdrawals),
	}
	return blk, nil
}

func headerToJSON(h entity.Header) headerJSON {
	var j headerJSON
	j.ParentHash = h.ParentHash.Hex()
	j.UncleHash = h.UncleHash.Hex()
	j.Coinbase = h.Coinbase.Hex()
	j.Root = h.Root.Hex()
	j.TxHash = h.TxHash.Hex()
	j.ReceiptHash = h.ReceiptHash.Hex()
	j.Bloom = "0x" + hex.EncodeToString(h.Bloom[:])
	if h.Difficulty != nil {
		j.Difficulty = h.Difficulty.String()
	}
	j.Number = h.Number
	j.GasLimit = h.GasLimit
	j.GasUsed = h.GasUsed
	j.Time = h.Time
	if len(h.Extra) > 0 {
		j.Extra = "0x" + hex.EncodeToString(h.Extra)
	}
	j.MixDigest = h.MixDigest.Hex()
	j.Nonce = "0x" + hex.EncodeToString(h.Nonce[:])
	if h.BaseFee != nil {
		j.BaseFee = h.BaseFee.String()
	}
	if h.WithdrawalsHash != nil {
		j.WithdrawalsHash = h.WithdrawalsHash.Hex()
	}
	if h.ParentBeaconRoot != nil {
		j.ParentBeaconRoot = h.ParentBeaconRoot.Hex()
	}
	j.BlobGasUsed = h.BlobGasUsed
	j.ExcessBlobGas = h.ExcessBlobGas
	return j
}

func headerFromJSON(j headerJSON) entity.Header {
	var h entity.Header
	h.ParentHash = common.HexToHash(j.ParentHash)
	h.UncleHash = common.HexToHash(j.UncleHash)
	h.Coinbase = common.HexToAddress(j.Coinbase)
	h.Root = common.HexToHash(j.Root)
	h.TxHash = common.HexToHash(j.TxHash)
	h.ReceiptHash = common.HexToHash(j.ReceiptHash)
	if s := trim0x(j.Bloom); s != "" {
		if b, err := hex.DecodeString(s); err == nil && len(b) == len(h.Bloom) {
			copy(h.Bloom[:], b)
		}
	}
	h.Difficulty = stringToBig(j.Difficulty)
	h.Number = j.Number
	h.GasLimit = j.GasLimit
	h.GasUsed = j.GasUsed
	h.Time = j.Time
	if s := trim0x(j.Extra); s != "" {
		if b, err := hex.DecodeString(s); err == nil {
			h.Extra = make([]byte, len(b))
			copy(h.Extra, b)
		}
	}
	h.MixDigest = common.HexToHash(j.MixDigest)
	if s := trim0x(j.Nonce); s != "" {
		if b, err := hex.DecodeString(s); err == nil {
			var n types.BlockNonce
			copy(n[:], b)
			h.Nonce = n
		}
	}
	h.BaseFee = stringToBig(j.BaseFee)
	if j.WithdrawalsHash != "" {
		wh := common.HexToHash(j.WithdrawalsHash)
		h.WithdrawalsHash = &wh
	}
	if j.ParentBeaconRoot != "" {
		pbr := common.HexToHash(j.ParentBeaconRoot)
		h.ParentBeaconRoot = &pbr
	}
	h.BlobGasUsed = j.BlobGasUsed
	h.ExcessBlobGas = j.ExcessBlobGas
	return h
}

func headersToJSON(hs []entity.Header) []headerJSON {
	if len(hs) == 0 {
		return nil
	}
	out := make([]headerJSON, len(hs))
	for i, h := range hs {
		out[i] = headerToJSON(h)
	}
	return out
}

func headersFromJSON(js []headerJSON) []entity.Header {
	if len(js) == 0 {
		return nil
	}
	out := make([]entity.Header, len(js))
	for i, h := range js {
		out[i] = headerFromJSON(h)
	}
	return out
}

func txsToJSON(txs []entity.Transaction) []txJSON {
	if len(txs) == 0 {
		return nil
	}
	out := make([]txJSON, len(txs))
	for i, tx := range txs {
		var to string
		if tx.To != nil {
			to = tx.To.Hex()
		}
		out[i] = txJSON{
			Hash: tx.Hash.Hex(), Type: tx.Type, To: to,
			Value: bigToString(tx.Value), Gas: tx.Gas, GasPrice: bigToString(tx.GasPrice),
			MaxFeePerGas: bigToString(tx.MaxFeePerGas), MaxPriorityFeePerGas: bigToString(tx.MaxPriorityFeePerGas),
			Nonce: tx.Nonce, Data: bytesToHex(tx.Data), AccessList: accessListToJSON(tx.AccessList), ChainID: bigToString(tx.ChainID),
		}
	}
	return out
}

func txsFromJSON(js []txJSON) []entity.Transaction {
	if len(js) == 0 {
		return nil
	}
	out := make([]entity.Transaction, len(js))
	for i, t := range js {
		var to *common.Address
		if t.To != "" {
			addr := common.HexToAddress(t.To)
			to = &addr
		}
		out[i] = entity.Transaction{
			Hash: common.HexToHash(t.Hash), Type: t.Type, To: to,
			Value: stringToBig(t.Value), Gas: t.Gas, GasPrice: stringToBig(t.GasPrice),
			MaxFeePerGas: stringToBig(t.MaxFeePerGas), MaxPriorityFeePerGas: stringToBig(t.MaxPriorityFeePerGas),
			Nonce: t.Nonce, Data: hexToBytes(t.Data), AccessList: accessListFromJSON(t.AccessList), ChainID: stringToBig(t.ChainID),
		}
	}
	return out
}

func withdrawalsToJSON(ws []entity.Withdrawal) []withdrawalJSON {
	if len(ws) == 0 {
		return nil
	}
	out := make([]withdrawalJSON, len(ws))
	for i, w := range ws {
		out[i] = withdrawalJSON{Index: w.Index, Validator: w.Validator, Address: w.Address.Hex(), Amount: w.Amount}
	}
	return out
}

func withdrawalsFromJSON(js []withdrawalJSON) []entity.Withdrawal {
	if len(js) == 0 {
		return nil
	}
	out := make([]entity.Withdrawal, len(js))
	for i, w := range js {
		out[i] = entity.Withdrawal{Index: w.Index, Validator: w.Validator, Address: common.HexToAddress(w.Address), Amount: w.Amount}
	}
	return out
}

func accessListToJSON(list types.AccessList) []accessTupleJSON {
	if len(list) == 0 {
		return nil
	}
	out := make([]accessTupleJSON, len(list))
	for i, t := range list {
		keys := make([]string, len(t.StorageKeys))
		for j, k := range t.StorageKeys {
			keys[j] = k.Hex()
		}
		out[i] = accessTupleJSON{Address: t.Address.Hex(), StorageKeys: keys}
	}
	return out
}

func accessListFromJSON(list []accessTupleJSON) types.AccessList {
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
	if bi, ok := new(big.Int).SetString(s, 10); ok {
		return bi
	}
	s2 := trim0x(s)
	if b, err := hex.DecodeString(s2); err == nil {
		return new(big.Int).SetBytes(b)
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

func trim0x(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if strings.HasPrefix(s, "0x") {
		return s[2:]
	}
	return s
}
