package infra

import (
	"github.com/gofiber/fiber/v3"
	"github.com/pancudaniel7/blockscan-ethereum-service/internal/adapter/http"
)

func InitRoutes(server *fiber.App) {
	server.Get("/health", http.Health)
}
