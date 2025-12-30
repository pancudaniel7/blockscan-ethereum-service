package infra

import (
	"fmt"
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/adapter/scan"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/spf13/viper"
)

// InitScanner constructs an Ethereum scan using configuration provided via Viper.
// It validates the scan config and returns a port.Scanner implementation.
func InitScanner(log applog.AppLogger, wg *sync.WaitGroup, v *validator.Validate) (port.Scanner, error) {
	if wg == nil {
		wg = &sync.WaitGroup{}
	}
	if v == nil {
		v = validator.New()
	}

    cfg := scan.Config{
        WebSocketsURL:          viper.GetString("scanner.websocket_url"),
        FinalizedBlocks:        viper.GetBool("scanner.finalized_blocks"),
        FinalizedPollDelay:     uint64(viper.GetInt("scanner.finalized_poll_delay")),
        FinalizedConfirmations: uint64(viper.GetInt("scanner.finalized_confirmations")),
        DialMaxRetryAttempts:      viper.GetInt("scanner.dial_max_retry_attempts"),
        DialRetryInitialBackoffMS: viper.GetInt("scanner.dial_retry_initial_backoff_ms"),
        DialRetryMaxBackoffMS:     viper.GetInt("scanner.dial_retry_max_backoff_ms"),
        DialRetryJitter:           viper.GetFloat64("scanner.dial_retry_jitter"),
    }

	s, err := scan.NewEthereumScanner(log, wg, &cfg, v)
	if err != nil {
		return nil, fmt.Errorf("infra: failed to init scan: %w", err)
	}
	return s, nil
}
