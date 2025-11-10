package port

import (
	"context"

	"github.com/ethereum/go-ethereum/core/types"
)

// BlockHandler consumes fully materialized Ethereum blocks produced by the scan.
type BlockHandler func(context.Context, *types.Block) error

// Scanner represents the Ethereum block ingestion source (e.g. websocket subscriber).
// Implementations must support handler injection, lifecycle management, and graceful shutdown.
type Scanner interface {
	SetHandler(handler BlockHandler)
	StartScanning() error
	StopScanning()
}
