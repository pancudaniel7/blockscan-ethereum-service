package scan

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/pattern"
)

// EthereumScanner scans Ethereum blocks either by subscribing to new heads or
// by polling for finalized blocks. It performs validation on its Config when
// started and uses the provided log for diagnostic messages.
//
// Use NewEthereumScanner to construct an instance and StartScanning to begin
// scanning. StopScanning cancels the internal context and stops the scan.
type EthereumScanner struct {
	log           applog.AppLogger
	wg            *sync.WaitGroup
	config        Config
	cancel        context.CancelFunc
	lastProcessed uint64
	handler       port.BlockHandler
	mu            sync.Mutex
	running       bool
}

const drainFetchTimeout = 5 * time.Second
const fetchTimeout = 10 * time.Second

// NewEthereumScanner creates a new EthereumScanner with the given log,
// wait group and configuration. The log is used for informational and
// error messages. The provided wait group will be used to track the scan
// goroutine lifecycle.
func NewEthereumScanner(log applog.AppLogger, wg *sync.WaitGroup, cfg Config, v *validator.Validate) (*EthereumScanner, error) {
	if err := v.Struct(cfg); err != nil {
		log.Error("Invalid cfg", "err", err)
		return nil, apperr.NewBlockScanErr("Invalid cfg", err)
	}

	return &EthereumScanner{
		log:    log,
		wg:     wg,
		config: cfg,
	}, nil
}

// SetHandler registers the callback invoked for each fully fetched block.
func (s *EthereumScanner) SetHandler(handler port.BlockHandler) {
	s.handler = handler
}

// StartScanning validates the scan configuration and starts the scanning
// goroutine. It returns an error if configuration validation fails.
func (s *EthereumScanner) StartScanning() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return apperr.NewBlockScanErr("scanner already running", nil)
	}
	if s.handler == nil {
		s.mu.Unlock()
		return apperr.NewBlockScanErr("block handler is not configured", nil)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.running = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			s.running = false
			s.cancel = nil
			s.mu.Unlock()
		}()
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
			s.log.Trace("Stopping Ethereum scan...")
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

		s.log.Trace("Subscribed to new Ethereum heads")
		draining := false

	inner:
		for {
			if draining && len(headers) == 0 {
				s.log.Trace("Drained buffered headers; closing subscription")
				sub.Unsubscribe()
				client.Close()
				return
			}

			select {
			case <-ctx.Done():
				if !draining {
					s.log.Trace("Cancellation received; draining buffered headers...")
					draining = true
				}

			case err := <-sub.Err():
				if draining {
					s.log.Trace("Subscription closed while draining", "err", err)
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
						s.log.Trace("Headers channel drained")
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
	s.log.Trace("Scanning finalized Ethereum blocks (polling mode)")

	pollDelay := time.Duration(s.config.FinalizedPollDelay) * time.Second

	// Preserve backlog across reconnects to avoid skipping heights on transient failures.
	pendingHeights := make([]uint64, 0)
	draining := false

outer:
	for {
		select {
		case <-ctx.Done():
			if !draining {
				s.log.Trace("Stopping finalized scan; will drain pending if any...")
				draining = true
			}
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

		s.log.Trace("Connected to Ethereum node for finalized polling")

		for {
			// Recompute backlog only when empty and not draining.
			if len(pendingHeights) == 0 && !draining {
				// Try to read the current finalized head directly; fallback to confirmations math.
				finalized, err := s.currentFinalizedNumber(ctx, client)
				if err != nil {
					if ctx.Err() != nil {
						s.log.Trace("Stop requested while resolving finalized head; entering drain if needed")
						draining = true
					} else {
						s.log.Warn("Failed to resolve finalized head; reconnecting", "err", err)
						client.Close()
						continue outer
					}
				} else {
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
			}

			if len(pendingHeights) == 0 {
				if draining {
					s.log.Trace("Finalized backlog drained; closing client")
					client.Close()
					return
				}
				// Wait for the next poll or cancellation.
				select {
				case <-ctx.Done():
					if !draining {
						s.log.Trace("Cancellation received during idle; start draining")
						draining = true
					}
					continue
				case <-time.After(pollDelay):
					continue
				}
			}

			height := pendingHeights[0]

			// Use timeouts for RPC fetches to avoid hanging on shutdown or network stalls.
			var (
				fetchCtx    context.Context
				cancelFetch context.CancelFunc
				effectiveTO = fetchTimeout
			)
			if draining {
				effectiveTO = drainFetchTimeout
			}
			fetchCtx, cancelFetch = context.WithTimeout(context.Background(), effectiveTO)
			blk, err := client.BlockByNumber(fetchCtx, new(big.Int).SetUint64(height))
			cancelFetch()
			if err != nil {
				if ctx.Err() != nil {
					s.log.Trace("Context canceled while fetching finalized block, continuing drain", "height", height)
					draining = true

					// Do not drop the height; retry after reconnect
					client.Close()
					continue outer
				}
				s.log.Warn("Failed to fetch finalized block; will reconnect and retry", "height", height, "err", err)

				// Keep the height in the queue and reconnect
				client.Close()
				continue outer
			}

			s.log.Trace("Scanned finalized block", "number", blk.NumberU64(), "txs", len(blk.Transactions()))
			if err := s.handler(ctx, blk); err != nil {
				s.log.Error("Block handler failed (finalized)", "number", blk.NumberU64(), "err", err)
				// Drop the block to avoid infinite retry loop; lastProcessed is not advanced
			} else {
				s.lastProcessed = height
			}

			// Pop the processed height from the backlog
			pendingHeights = pendingHeights[1:]
		}
	}
}

// currentFinalizedNumber returns the current finalized block height using the
// RPC 'finalized' tag when available. If the node does not support the tag
// (or returns an error), it falls back to `latest - confirmations`.
func (s *EthereumScanner) currentFinalizedNumber(ctx context.Context, client *ethclient.Client) (uint64, error) {
	// Try the canonical 'finalized' tag first.
	header, err := client.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err == nil && header != nil {
		return header.Number.Uint64(), nil
	}

	// Fallback to latest-confirmations if configured.
	latest, lerr := client.BlockNumber(ctx)
	if lerr != nil {
		if err != nil {
			// Prefer the original error if both failed.
			return 0, err
		}
		return 0, lerr
	}
	if latest < s.config.FinalizedConfirmations {
		// Not enough chain height to satisfy the confirmation window yet
		return 0, apperr.NewBlockScanErr("finalization window not yet available", nil)
	}
	return latest - s.config.FinalizedConfirmations, nil
}

// StopScanning cancels the scan's internal context (if any), which causes
// the scanning goroutine to exit gracefully.
func (s *EthereumScanner) StopScanning() {
	s.mu.Lock()
	if s.cancel == nil {
		s.mu.Unlock()
		s.log.Trace("Ethereum scanning stopped")
		return
	}
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()

	s.log.Trace("Cancelling Ethereum scanning...")
	cancel()
	s.log.Trace("Ethereum scanning stopped")
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
