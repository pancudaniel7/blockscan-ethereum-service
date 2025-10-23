package main

import (
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/infra"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

var (
	logger applog.AppLogger
)

func main() {
	logger = applog.NewAppDefaultLogger()
	if err := infra.LoadConfig(); err != nil {
		logger.Fatal("failed to load config: %v", err.Error())
	}
}
