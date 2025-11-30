//go:build integration

package test

import (
    "sync"
    "testing"

    "github.com/go-playground/validator/v10"
    "github.com/pancudaniel7/blockscan-ethereum-service/internal/adapter/scan"
    "github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
    "github.com/pancudaniel7/blockscan-ethereum-service/test/util"
)

var (
    wg sync.WaitGroup
)

func ScanAndPublishNonFinalizedBlocksTest(t *testing.T) {
	if err := util.InitConfig(); err != nil {
		t.Fatalf("Failed to initialize configs: %v", err)
	}
	//ctx := context.Background()

    if err := InitContainers(); err != nil {
        t.Fatalf("Failed to init containers: %v", err)
    }

	_ = InitScanner(t)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Test scanning and publishing non-finalized blocks",
			run: func(t *testing.T) {

			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)

		})
	}
}

// No local TestMain; global one in init_containers.go handles cleanup.

func InitScanner(t *testing.T) *scan.EthereumScanner {
	logger := applog.NewAppDefaultLogger()
	cfg := scan.Config{}
	v := validator.New()
	scanner, err := scan.NewEthereumScanner(logger, &wg, &cfg, v)
	if err != nil {
		t.Fatalf("Failed to init scanner: %v", err)
	}
	return scanner
}
