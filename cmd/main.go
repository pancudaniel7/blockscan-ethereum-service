package main

import (
	"log"
	"sync"

	"github.com/gofiber/fiber/v3"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/infra"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

var (
	logger applog.AppLogger
	server *fiber.App
)

func main() {
	if err := infra.LoadConfig(); err != nil {
		log.Fatal("Failed to load config: ", err)
	}
	logger = applog.NewAppDefaultLogger()

	var wg sync.WaitGroup
	server = infra.StartServer(logger, &wg)

	callBack := func() error {
		logger.Debug("Executing shutdown callback...")
		return nil
	}
	infra.ShutdownServer(logger, &wg, server, callBack)
}
