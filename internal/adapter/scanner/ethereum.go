package scanner

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
)

// Config holds configuration for the Ethereum scanner.
//
// WebSocketsURL is the Ethereum node endpoint used to subscribe to new heads
// (ws/wss) or to connect for HTTP polling (http/https). When FinalizedBlocks
// is true, the scanner will poll for finalized blocks using the provided
// confirmation depth and delay.
type Config struct {
	WebSocketsURL          string `validate:"required,uri"`
	FinalizedBlocks        bool
	FinalizedPollDelay     uint64 `validate:"excluded_unless=FinalizedBlocks true,required,gte=5,lte=64"`
	FinalizedConfirmations uint64 `validate:"excluded_unless=FinalizedBlocks true,required,gte=32,lte=128"`
}

// EthereumScanner scans Ethereum blocks either by subscribing to new heads or
// by polling for finalized blocks. It performs validation on its Config when
// started and uses the provided log for diagnostic messages.
//
// Use NewEthereumScanner to construct an instance and StartScanning to begin
// scanning. StopScanning cancels the internal context and stops the scanner.
type EthereumScanner struct {
	log           applog.AppLogger
	wg            *sync.WaitGroup
	validator     *validator.Validate
	config        Config
	cancel        context.CancelFunc
	lastProcessed uint64
	handler       BlockHandler
}

const drainFetchTimeout = 5 * time.Second

// BlockHandler consumes fully fetched blocks from the scanner. Returning an
// error keeps the scanner running while logging the failure.
type BlockHandler func(context.Context, *types.Block) error

// NewEthereumScanner creates a new EthereumScanner with the given log,
// wait group and configuration. The log is used for informational and
// error messages. The provided wait group will be used to track the scanner
// goroutine lifecycle.
func NewEthereumScanner(log applog.AppLogger, wg *sync.WaitGroup, cfg Config, v *validator.Validate) (*EthereumScanner, error) {
	if err := v.Struct(cfg); err != nil {
		log.Error("Invalid cfg", "err", err)
		return nil, apperr.NewBlockScanErr("Invalid cfg", err)
	}

	return &EthereumScanner{
		log:       log,
		wg:        wg,
		validator: v,
		config:    cfg,
	}, nil
}

// SetHandler registers the callback invoked for each fully fetched block.
func (s *EthereumScanner) SetHandler(handler BlockHandler) {
	s.handler = handler
}

// StartScanning validates the scanner configuration and starts the scanning
// goroutine. It returns an error if configuration validation fails.
func (s *EthereumScanner) StartScanning() error {
	if s.handler == nil {
		return apperr.NewBlockScanErr("block handler is not configured", nil)
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
outer:
	for {
		select {
		case <-ctx.Done():
			s.log.Info("Stopping Ethereum scanner...")
			return
		default:
		}

		client, err := s.connectClient(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("Failed to connect to Ethereum node", "err", err)
			continue
		}

		headers := make(chan *types.Header, 32)
		sub, err := client.SubscribeNewHead(ctx, headers)
		if err != nil {
			s.log.Error("Subscription to new heads failed", "err", err)
			client.Close()
			continue
		}

		s.log.Info("Subscribed to new Ethereum heads")

		draining := false

	inner:
		for {
			if draining && len(headers) == 0 {
				s.log.Info("Drained buffered headers; closing subscription")
				sub.Unsubscribe()
				client.Close()
				return
			}

			select {
			case <-ctx.Done():
				if !draining {
					s.log.Info("Cancellation received; draining buffered headers...")
					draining = true
				}

			case err := <-sub.Err():
				if draining {
					s.log.Info("Subscription closed while draining", "err", err)
					sub.Unsubscribe()
					client.Close()
					return
				}
				s.log.Warn("Subscription error, will reconnect", "err", err)
				sub.Unsubscribe()
				client.Close()
				break inner

			case header, ok := <-headers:
				if !ok {
					if draining {
						s.log.Info("Headers channel drained")
						sub.Unsubscribe()
						client.Close()
						return
					}
					s.log.Warn("Headers channel closed — restarting subscription")
					sub.Unsubscribe()
					client.Close()
					break inner
				}

				blockCtx := ctx
				var cancelFetch context.CancelFunc
				if draining {
					blockCtx, cancelFetch = context.WithTimeout(context.Background(), drainFetchTimeout)
				}
				block, err := client.BlockByHash(blockCtx, header.Hash())
				if cancelFetch != nil {
					cancelFetch()
				}
				if err != nil {
					s.log.Error("Failed to fetch block by hash", "hash", header.Hash().Hex(), "err", err)
					continue
				}
				s.log.Trace("Scanned block", "number", block.NumberU64(), "txs", len(block.Transactions()))
				if err := s.handler(ctx, block); err != nil {
					s.log.Error("Block handler failed (new heads)", "number", block.NumberU64(), "err", err)
				}
			}
		}

		continue outer
	}
}

// scanFinalized polls the node for finalized block heights (using the
// configured confirmation depth) and fetches blocks by number. It runs until
// the context is canceled and will sleep between iterations according to the
// configured poll delay.
func (s *EthereumScanner) scanFinalized(ctx context.Context) {
	s.log.Info("Scanning finalized Ethereum blocks (polling mode)")

	pollDelay := time.Duration(s.config.FinalizedPollDelay) * time.Second

outer:
	for {
		select {
		case <-ctx.Done():
			s.log.Info("Stopping finalized scanner...")
			return
		default:
		}

		client, err := s.connectClient(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("Failed to connect to Ethereum node, trying again...", "err", err)
			continue
		}

		s.log.Info("Connected to Ethereum node for finalized polling")

		pendingHeights := make([]uint64, 0)
		draining := false

		for {
			if len(pendingHeights) == 0 {
				select {
				case <-ctx.Done():
					if !draining {
						s.log.Info("Cancellation received; draining finalized backlog...")
						draining = true
					}
				default:
				}
			}

			if len(pendingHeights) == 0 && !draining {
				latest, err := client.BlockNumber(ctx)
				if err != nil {
					if ctx.Err() != nil {
						s.log.Info("Stop requested while fetching block number; draining backlog")
						draining = true
						continue
					}
					s.log.Error("Failed to get latest block number", "err", err)
					client.Close()
					continue outer
				}

				if latest < s.config.FinalizedConfirmations {
					s.log.Warn("Waiting for finalization window", "height", latest, "confirmations", s.config.FinalizedConfirmations)
					time.Sleep(5 * time.Second)
					continue
				}

				finalized := latest - s.config.FinalizedConfirmations
				next := s.lastProcessed + 1
				if s.lastProcessed == 0 {
					// On the first run, start at the current finalized tip to avoid replaying a large backlog
					next = finalized
				}
				if next <= finalized {
					for h := next; h <= finalized; h++ {
						pendingHeights = append(pendingHeights, h)
					}
				}
			}

			if len(pendingHeights) == 0 {
				if draining {
					s.log.Info("Finalized backlog drained; closing client")
					client.Close()
					return
				}
				time.Sleep(pollDelay)
				continue
			}

			height := pendingHeights[0]
			pendingHeights = pendingHeights[1:]

			fetchCtx := ctx
			var cancelFetch context.CancelFunc
			if draining {
				fetchCtx, cancelFetch = context.WithTimeout(context.Background(), drainFetchTimeout)
			}

			blk, err := client.BlockByNumber(fetchCtx, new(big.Int).SetUint64(height))
			if cancelFetch != nil {
				cancelFetch()
			}
			if err != nil {
				if ctx.Err() != nil {
					s.log.Info("Context canceled while fetching finalized block, continuing drain", "height", height)
					draining = true
					continue
				}
				s.log.Error("Failed to fetch finalized block", "height", height, "err", err)
				client.Close()
				continue outer
			}

			s.log.Trace("Scanned finalized block", "number", blk.NumberU64(), "txs", len(blk.Transactions()))
			if err := s.handler(ctx, blk); err != nil {
				s.log.Error("Block handler failed (finalized)", "number", blk.NumberU64(), "err", err)
				continue
			}
			s.lastProcessed = height

			if draining && len(pendingHeights) == 0 {
				s.log.Info("Finalized backlog drained; closing client")
				client.Close()
				return
			}
		}
	}
}

// StopScanning cancels the scanner's internal context (if any), which causes
// the scanning goroutine to exit gracefully.
func (s *EthereumScanner) StopScanning() {
	if s.cancel != nil {
		s.log.Info("Cancelling Ethereum scanning...")
		s.cancel()
	}
	s.log.Info("Ethereum scanning stopped")
}

// connectClient dials to the Ethereum node with exponential backoff and jitter
// until success or context cancellation.
func (s *EthereumScanner) connectClient(ctx context.Context) (*ethclient.Client, error) {
	var client *ethclient.Client
	err := pattern.Retry(
		ctx,
		func(attempt int) error {
			c, err := ethclient.DialContext(ctx, s.config.WebSocketsURL)
			if err != nil {
				s.log.Warn("Ethereum dial failed", "attempt", attempt, "err", err)
				return err
			}
			client = c
			return nil
		},
		pattern.WithInfiniteAttempts(),
		pattern.WithInitialDelay(500*time.Millisecond),
		pattern.WithMaxDelay(10*time.Second),
		pattern.WithMultiplier(2.0),
		pattern.WithJitter(0.2),
	)
	if err != nil {
		return nil, err
	}
	return client, nil
}
