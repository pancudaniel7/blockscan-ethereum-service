package http

import "github.com/gofiber/fiber/v3"

func Health(ctx fiber.Ctx) error {
	ctx.Status(fiber.StatusOK)
	return ctx.JSON(fiber.Map{"status": "up"})
}
