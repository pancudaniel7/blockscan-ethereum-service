package infra

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/pkg/applog"
	"github.com/spf13/viper"
)

func StartServer(logger applog.AppLogger, wg *sync.WaitGroup) *fiber.App {
	an := viper.GetString("server.name")
	app := fiber.New(fiber.Config{AppName: an})
	port := viper.GetString("server.port")

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := app.Listen(fmt.Sprintf(":%s", port)); err != nil {
			logger.Fatal("Failed to start server: %v", err.Error())
		}
	}()
	return app
}

func ShutdownServer(logger applog.AppLogger, wg *sync.WaitGroup, server *fiber.App, processTerminationCallBack func() error) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(quit)

	<-quit
	logger.Info("Shutdown signal received, stopping server...")

	if processTerminationCallBack != nil {
		if err := processTerminationCallBack(); err != nil {
			logger.Error("Server shutdown failed: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.ShutdownWithContext(ctx); err != nil {
		logger.Error("Server shutdown failed: %v", err)
	}

	wg.Wait()
	logger.Info("Shutdown complete")
}
