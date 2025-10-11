package main

import (
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applogger"
)

var (
	logger applogger.Logger
)

func main() {
	logger = applogger.NewAppDefaultLogger()
}
