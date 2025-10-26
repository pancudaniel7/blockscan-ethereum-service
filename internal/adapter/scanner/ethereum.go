package scanner

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

// Config holds configuration for the Ethereum scanner.
//
// WebSocketsURL is the Ethereum node WebSocket endpoint used to subscribe to
// new heads. FinalizedBlocks toggles whether the scanner should poll for
// finalized blocks instead of subscribing to new heads. FinalizedPollDelay
// controls the polling frequency (seconds) when finalized scanning is enabled.
// FinalizedConfirmations is the number of confirmations used to determine
// finalized block height when polling mode is active.
type Config struct {
	WebSocketsURL          string `validate:"required,uri"`
	FinalizedBlocks        bool
	FinalizedPollDelay     uint64 `validate:"required,gte=5,lte=64"`
	FinalizedConfirmations uint64 `validate:"required,gte=32,lte=128"`
}

// EthereumScanner scans Ethereum blocks either by subscribing to new heads or
// by polling for finalized blocks. It performs validation on its Config when
// started and uses the provided logger for diagnostic messages.
//
// Use NewEthereumScanner to construct an instance and StartScanning to begin
// scanning. StopScanning cancels the internal context and stops the scanner.
type EthereumScanner struct {
	logger    applog.AppLogger
	wg        *sync.WaitGroup
	validator *validator.Validate
	config    Config
	cancel    context.CancelFunc
}

// NewEthereumScanner creates a new EthereumScanner with the given logger,
// wait group and configuration. The logger is used for informational and
// error messages. The provided wait group will be used to track the scanner
// goroutine lifecycle.
func NewEthereumScanner(logger applog.AppLogger, wg *sync.WaitGroup, config Config) *EthereumScanner {
	return &EthereumScanner{
		logger:    logger,
		wg:        wg,
		validator: validator.New(),
		config:    config,
	}
}

// StartScanning validates the scanner configuration and starts the scanning
// goroutine. It returns an error if configuration validation fails.
func (s *EthereumScanner) StartScanning() error {
	if err := s.validator.Struct(s.config); err != nil {
		s.logger.Error(fmt.Sprintf("Invalid config: %v", err))
		return &apperr.BlockScanErr{Msg: "Invalid config", Cause: err}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if s.config.FinalizedBlocks {
			s.scanFinalized(ctx)
		} else {
			s.scanNewHeads(ctx)
		}
	}()

	return nil
}

// scanNewHeads subscribes to new head events via WebSockets and processes
// incoming headers. It reconnects and retries on errors. The function runs
// until the provided context is canceled.
func (s *EthereumScanner) scanNewHeads(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping Ethereum scanner...")
			return
		default:
		}

		client, err := ethclient.DialContext(ctx, s.config.WebSocketsURL)
		if err != nil {
			s.logger.Error(fmt.Sprintf("Failed to connect: %v", err))
			time.Sleep(3 * time.Second)
			continue
		}

		headers := make(chan *types.Header, 32)
		sub, err := client.SubscribeNewHead(ctx, headers)
		if err != nil {
			s.logger.Error(fmt.Sprintf("Subscription failed: %v", err))
			client.Close()
			time.Sleep(3 * time.Second)
			continue
		}

		s.logger.Info("Connected and subscribed to new Ethereum blocks")

	loop:
		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Context canceled, closing subscription...")
				sub.Unsubscribe()
				client.Close()
				return

			case err := <-sub.Err():
				s.logger.Error(fmt.Sprintf("Subscription error: %v", err))
				sub.Unsubscribe()
				client.Close()
				time.Sleep(3 * time.Second)
				break loop

			case header, ok := <-headers:
				if !ok {
					s.logger.Warn("Headers channel closed — restarting subscription")
					sub.Unsubscribe()
					client.Close()
					time.Sleep(3 * time.Second)
					break loop
				}

				block, err := client.BlockByHash(ctx, header.Hash())
				if err != nil {
					s.logger.Error(fmt.Sprintf("Failed to get block: %v", err))
					continue
				}
				s.logger.Trace(fmt.Sprintf("Scanned block #%v (%d txs)", block.NumberU64(), len(block.Transactions())))
				// TODO: downstream processor
			}
		}
	}
}

// scanFinalized polls the node for finalized block heights (using the
// configured confirmation depth) and fetches blocks by number. It runs until
// the context is canceled and will sleep between iterations according to the
// configured poll delay.
func (s *EthereumScanner) scanFinalized(ctx context.Context) {
	s.logger.Info("Scanning finalized Ethereum blocks (polling mode)")

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping finalized scanner...")
			return
		default:
		}

		client, err := ethclient.DialContext(ctx, s.config.WebSocketsURL)
		if err != nil {
			s.logger.Error(fmt.Sprintf("Failed to connect: %v", err))
			time.Sleep(3 * time.Second)
			continue
		}

		s.logger.Info("Connected to Ethereum node for finalized blocks")

		var lastProcessed uint64
		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Context canceled, closing finalized client...")
				client.Close()
				return
			default:
			}

			latest, err := client.BlockNumber(ctx)
			if err != nil {
				s.logger.Error(fmt.Sprintf("Failed to get latest block number: %v", err))
				client.Close()
				time.Sleep(3 * time.Second)
				break
			}

			if latest < s.config.FinalizedConfirmations {
				s.logger.Warn(fmt.Sprintf("Chain height %d < %d — waiting for finalization window",
					latest, s.config.FinalizedConfirmations))
				time.Sleep(5 * time.Second)
				continue
			}

			finalized := latest - s.config.FinalizedConfirmations
			if finalized <= lastProcessed { // skip duplicate blocks
				time.Sleep(time.Duration(s.config.FinalizedPollDelay) * time.Second)
				continue
			}

			block, err := client.BlockByNumber(ctx, new(big.Int).SetUint64(finalized))
			if err != nil {
				s.logger.Error(fmt.Sprintf("Failed to fetch finalized block: %v", err))
				client.Close()
				time.Sleep(3 * time.Second)
				break
			}

			s.logger.Trace(fmt.Sprintf("Scanned finalized block #%v (%d txs)", block.NumberU64(), len(block.Transactions())))
			// TODO: downstream processor

			lastProcessed = finalized
			time.Sleep(time.Duration(s.config.FinalizedPollDelay) * time.Second)
		}
	}
}

// StopScanning cancels the scanner's internal context (if any), which causes
// the scanning goroutine to exit gracefully.
func (s *EthereumScanner) StopScanning() {
	if s.cancel != nil {
		s.logger.Info("Cancelling Ethereum scanning...")
		s.cancel()
	}
	s.logger.Info("Ethereum scanning stopped")
}
