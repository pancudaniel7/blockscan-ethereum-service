package scanner

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/apperr"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

type Config struct {
	WebSocketsURL string `validate:"required,url"`
}

type EthereumScanner struct {
	logger    applog.AppLogger
	wg        *sync.WaitGroup
	validator *validator.Validate
	config    Config
	cancel    context.CancelFunc
}

func NewEthereumScanner(logger applog.AppLogger, wg *sync.WaitGroup, config Config) *EthereumScanner {
	return &EthereumScanner{
		logger:    logger,
		wg:        wg,
		validator: validator.New(),
		config:    config,
	}
}

func (s *EthereumScanner) StartScanning() error {
	if err := s.validator.Struct(s.config); err != nil {
		s.logger.Error(fmt.Sprintf("Invalid config: %v", err))
		return &apperr.BlockScanErr{
			Msg:   "Invalid config",
			Cause: err,
		}
	}

	s.logger.Info("Connecting to Ethereum WebSocket...")
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

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

			headers := make(chan *types.Header)
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
					break loop // restart outer for

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
	}()

	return nil
}

func (s *EthereumScanner) StopScanning() {
	if s.cancel != nil {
		s.logger.Info("Cancelling Ethereum scanning...")
		s.cancel()
	}
	s.logger.Info("Ethereum scanning stopped")
}
