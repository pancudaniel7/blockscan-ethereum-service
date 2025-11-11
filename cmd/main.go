package main

import (
	"log"
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/gofiber/fiber/v3"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/port"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/core/usecase"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/infra"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
)

var (
	wg     sync.WaitGroup
	logger applog.AppLogger
	valid  *validator.Validate
	server *fiber.App

	blockScanner      port.Scanner
	blockStoreLogger  port.StoreLogger
	blockStreamReader port.StoreStreamReader

	blockPublisher        port.Publisher
	blockProcessorService port.ProcessorService
)

func initComponents() {
    var err error
    blockStoreLogger, err = infra.InitStoreLogger(logger, &wg, valid)
    if err != nil {
        panic("Failed to init store logger: " + err.Error())
    }

    blockStreamReader, err = infra.InitBlockStreamReader(logger, &wg, valid)
    if err != nil {
        panic("Failed to init block stream: " + err.Error())
    }
    blockPublisher, err = infra.InitBlockPublisher(logger, valid)
    if err != nil {
        panic("Failed to init block publisher: " + err.Error())
    }

    blockScanner, err = infra.InitScanner(logger, &wg, valid)
    if err != nil {
        panic("Failed to init block scanner: " + err.Error())
    }

    blockProcessorService = usecase.NewBlockProcessorService(logger, blockStoreLogger, blockStreamReader, blockPublisher)

	blockScanner.SetHandler(blockProcessorService.StoreBlock)
	blockStreamReader.SetHandler(blockProcessorService.ReadAndPublishBlock)
}

func main() {
	if err := infra.LoadConfig(); err != nil {
		log.Fatal("Failed to load config: ", err)
	}
	logger = applog.NewAppDefaultLogger()
	valid = validator.New()

	initComponents()
	server = infra.StartServer(logger, &wg)

	// Start stream reader
	if err := blockStreamReader.StartReadFromStream(); err != nil {
		logger.Error("Failed to start block stream reader: ", "err", err)
		panic("Failed to start block stream reader: " + err.Error())
	}

	// Start block scanner
	if err := blockScanner.StartScanning(); err != nil {
		logger.Error("Failed to start block scanner: ", "err", err)
		panic("Failed to start block scanner: " + err.Error())
	}

	// Shutdown handling
	callBack := func() error {
		logger.Info("Executing shutdown routines...")
		blockScanner.StopScanning()
		blockStreamReader.StopReadFromStream()
		return nil
	}
	infra.ShutdownServer(logger, &wg, server, callBack)
}
