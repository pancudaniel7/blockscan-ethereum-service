package scan

import (
	"context"
	"math/big"
	"sync"
	"time"

	"errors"
	"net"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	imetrics "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/metrics"
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
	config        *Config
	cancel        context.CancelFunc
	lastProcessed uint64
	handler       port.BlockHandler
	mu            sync.Mutex
	running       bool
	newClient     func(context.Context) (ethereumClient, error)
	dialRetryOpts []pattern.RetryOption
}

const drainFetchTimeout = 5 * time.Second
const fetchTimeout = 10 * time.Second

// NewEthereumScanner creates a new EthereumScanner with the given log,
// wait group, and configuration. The log is used for informational and
// error messages. The provided wait group will be used to track the scan
// goroutine lifecycle.
func NewEthereumScanner(log applog.AppLogger, wg *sync.WaitGroup, cfg *Config, v *validator.Validate) (*EthereumScanner, error) {
	if err := v.Struct(cfg); err != nil {
		log.Error("invalid config", "err", err)
		return nil, apperr.NewBlockScanErr("invalid config", err)
	}

	s := &EthereumScanner{
		log:    log,
		wg:     wg,
		config: cfg,
	}
	s.newClient = func(ctx context.Context) (ethereumClient, error) {
		return ethclient.DialContext(ctx, cfg.WebSocketsURL)
	}
	s.dialRetryOpts = dialRetryOptionsFromConfig(cfg)

	return s, nil
}

// dialRetryOptionsFromConfig builds retry options from the provided Config.
func dialRetryOptionsFromConfig(cfg *Config) []pattern.RetryOption {
	var opts []pattern.RetryOption
	if cfg.DialMaxRetryAttempts > 0 {
		opts = append(opts, pattern.WithMaxAttempts(cfg.DialMaxRetryAttempts))
	} else {
		opts = append(opts, pattern.WithInfiniteAttempts())
	}
	if cfg.DialRetryInitialBackoffMS > 0 {
		opts = append(opts, pattern.WithInitialDelay(time.Duration(cfg.DialRetryInitialBackoffMS)*time.Millisecond))
	}
	if cfg.DialRetryMaxBackoffMS > 0 {
		opts = append(opts, pattern.WithMaxDelay(time.Duration(cfg.DialRetryMaxBackoffMS)*time.Millisecond))
	}
	if cfg.DialRetryJitter > 0 {
		opts = append(opts, pattern.WithJitter(cfg.DialRetryJitter))
	}
	return opts
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

func (s *EthereumScanner) scanNewHeads(ctx context.Context) {
outer:
	for {
		select {
		case <-ctx.Done():
			imetrics.Scanner().Connected.Set(0)
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
			imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentScanner, classifyScanError(err)).Inc()
			continue
		}

		headers := make(chan *types.Header, 32)
		sub, err := client.SubscribeNewHead(ctx, headers)
		if err != nil {
			imetrics.Scanner().ReconnectsTotal.WithLabelValues("subscribe").Inc()
			s.log.Error("Subscription to new heads failed", "err", err)
			imetrics.App().ErrorsTotal.WithLabelValues(imetrics.ComponentScanner, "subscribe").Inc()
			client.Close()
			continue
		}

		imetrics.Scanner().Connected.Set(1)
		s.log.Trace("Subscribed to new Ethereum heads")

	inner:
		for {
			select {
			case <-ctx.Done():
				imetrics.Scanner().Connected.Set(0)
				sub.Unsubscribe()
				client.Close()
				return

			case err := <-sub.Err():
				imetrics.Scanner().ReconnectsTotal.WithLabelValues("subscribe").Inc()
				imetrics.Scanner().Connected.Set(0)
				s.log.Warn("Subscription error, will reconnect", "err", err)
				imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "subscribe").Inc()
				sub.Unsubscribe()
				client.Close()
				break inner

				case header, ok := <-headers:
					if !ok {
						imetrics.Scanner().ReconnectsTotal.WithLabelValues("subscribe").Inc()
						imetrics.Scanner().Connected.Set(0)
						s.log.Warn("Headers channel closed — restarting subscription")
                        imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "headers_closed").Inc()
						sub.Unsubscribe()
						client.Close()
						break inner
					}

				target := header.Number.Uint64()
				start := s.lastProcessed + 1
				if s.lastProcessed == 0 {
					start = target
				}
				for h := start; h <= target; h++ {
					fetchStart := time.Now()
					blk, err := s.fetchBlockByNumber(ctx, client, h)
						if err != nil {
							imetrics.Scanner().FetchLatencyMS.WithLabelValues("new_head").Observe(float64(time.Since(fetchStart).Milliseconds()))
							imetrics.Scanner().FetchErrorsTotal.WithLabelValues("new_head", classifyScanError(err)).Inc()
							imetrics.Scanner().ReconnectsTotal.WithLabelValues("fetch").Inc()
							s.log.Warn("Failed to fetch block; will reconnect", "number", h, "err", err)
                            imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "fetch").Inc()
							sub.Unsubscribe()
							client.Close()
							break inner
						}
					imetrics.Scanner().FetchLatencyMS.WithLabelValues("new_head").Observe(float64(time.Since(fetchStart).Milliseconds()))
					if err := s.handleBlock(ctx, blk); err != nil {
						imetrics.Scanner().HandlerErrorsTotal.WithLabelValues("new_head").Inc()
						imetrics.Scanner().ReconnectsTotal.WithLabelValues("handler").Inc()
						s.log.Error("Block handler failed (new heads); will reconnect", "number", blk.NumberU64(), "err", err)
						sub.Unsubscribe()
						client.Close()
						break inner
					}
					imetrics.Scanner().ScannedBlocksTotal.WithLabelValues("new_head", "ethereum").Inc()
				}
			}
		}

		continue outer
	}
}

func (s *EthereumScanner) scanFinalized(ctx context.Context) {
	s.log.Trace("Scanning finalized Ethereum blocks (polling mode)")

	pollDelay := time.Duration(s.config.FinalizedPollDelay) * time.Second
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
                imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "connect").Inc()
				continue
			}

		imetrics.Scanner().Connected.Set(1)
		s.log.Trace("Connected to Ethereum node for finalized polling")

		for {
			if len(pendingHeights) == 0 && !draining {
				finalized, err := s.currentFinalizedNumber(ctx, client)
				if err != nil {
					if ctx.Err() != nil {
						s.log.Trace("Stop requested while resolving finalized head; entering drain if needed")
						draining = true
						} else {
							s.log.Warn("Failed to resolve finalized head; reconnecting", "err", err)
                            imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "finalized_head").Inc()
							client.Close()
							continue outer
						}
					} else {
					next := s.lastProcessed + 1
					if s.lastProcessed == 0 {
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

			var (
				fetchCtx    context.Context
				cancelFetch context.CancelFunc
				effectiveTO = fetchTimeout
			)
			if draining {
				effectiveTO = drainFetchTimeout
			}
			fetchCtx, cancelFetch = context.WithTimeout(context.Background(), effectiveTO)
			fetchStart := time.Now()
			blk, err := client.BlockByNumber(fetchCtx, new(big.Int).SetUint64(height))
			cancelFetch()
			if err != nil {
				imetrics.Scanner().FetchLatencyMS.WithLabelValues("finalized").Observe(float64(time.Since(fetchStart).Milliseconds()))
				imetrics.Scanner().FetchErrorsTotal.WithLabelValues("finalized", classifyScanError(err)).Inc()
				imetrics.Scanner().ReconnectsTotal.WithLabelValues("fetch").Inc()
				if ctx.Err() != nil {
					s.log.Trace("Context canceled while fetching finalized block, continuing drain", "height", height)
					draining = true
					client.Close()
					continue outer
					}
					s.log.Warn("Failed to fetch finalized block; will reconnect and retry", "height", height, "err", err)
                    imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "fetch_finalized").Inc()
					client.Close()
					continue outer
				}
			var handleCtx = ctx
			var cancelHandle context.CancelFunc
			if draining {
				handleCtx, cancelHandle = context.WithTimeout(context.Background(), effectiveTO)
			}
			err = s.handleBlock(handleCtx, blk)
			if cancelHandle != nil {
				cancelHandle()
			}
				if err != nil {
					imetrics.Scanner().HandlerErrorsTotal.WithLabelValues("finalized").Inc()
					imetrics.Scanner().ReconnectsTotal.WithLabelValues("handler").Inc()
					s.log.Warn("Block handler failed (finalized); will reconnect and retry", "number", blk.NumberU64(), "err", err)
                    imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "handler_finalized").Inc()
					client.Close()
					continue outer
				}
			imetrics.Scanner().FetchLatencyMS.WithLabelValues("finalized").Observe(float64(time.Since(fetchStart).Milliseconds()))
			imetrics.Scanner().ScannedBlocksTotal.WithLabelValues("finalized", "ethereum").Inc()
			pendingHeights = pendingHeights[1:]
		}
	}
}

func (s *EthereumScanner) currentFinalizedNumber(ctx context.Context, client ethereumClient) (uint64, error) {
	header, err := client.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err == nil && header != nil {
		return header.Number.Uint64(), nil
	}
	latest, lerr := client.BlockNumber(ctx)
	if lerr != nil {
		if err != nil {
			return 0, err
		}
		return 0, lerr
	}
	if latest < s.config.FinalizedConfirmations {
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

func (s *EthereumScanner) connectClient(ctx context.Context) (ethereumClient, error) {
	var client ethereumClient
	opts := []pattern.RetryOption{
		pattern.WithInfiniteAttempts(),
		pattern.WithInitialDelay(500 * time.Millisecond),
		pattern.WithMaxDelay(10 * time.Second),
		pattern.WithMultiplier(2.0),
		pattern.WithJitter(0.2),
	}
	if len(s.dialRetryOpts) > 0 {
		opts = append(opts, s.dialRetryOpts...)
	}
	err := pattern.Retry(
		ctx,
		func(attempt int) error {
			if s.newClient == nil {
				return apperr.NewBlockScanErr("client factory not configured", nil)
			}
			c, err := s.newClient(ctx)
				if err != nil {
					imetrics.Scanner().ReconnectsTotal.WithLabelValues("dial").Inc()
					s.log.Warn("Ethereum dial failed", "attempt", attempt, "err", err)
                    imetrics.App().WarningsTotal.WithLabelValues(imetrics.ComponentScanner, "dial").Inc()
					return err
				}
			client = c
			return nil
		},
		opts...,
	)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func classifyScanError(err error) string {
	var netErr net.Error

	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &netErr):
		return "network"
	default:
		return "rpc"
	}
}

func (s *EthereumScanner) withFetchTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, fetchTimeout)
}

func (s *EthereumScanner) fetchBlockByNumber(ctx context.Context, client ethereumClient, number uint64) (*types.Block, error) {
	fetchCtx, cancel := s.withFetchTimeout(ctx)
	defer cancel()
	return client.BlockByNumber(fetchCtx, new(big.Int).SetUint64(number))
}

func (s *EthereumScanner) handleBlock(ctx context.Context, blk *types.Block) error {
	if err := s.handler(ctx, blk); err != nil {
		return err
	}
	n := blk.NumberU64()
	if n > s.lastProcessed {
		s.lastProcessed = n
	}
	s.log.Trace("Scanned block", "number", n, "txs", len(blk.Transactions()))
	return nil
}

type ethereumClient interface {
	SubscribeNewHead(context.Context, chan<- *types.Header) (ethereum.Subscription, error)
	BlockByNumber(context.Context, *big.Int) (*types.Block, error)
	HeaderByNumber(context.Context, *big.Int) (*types.Header, error)
	BlockNumber(context.Context) (uint64, error)
	Close()
}
