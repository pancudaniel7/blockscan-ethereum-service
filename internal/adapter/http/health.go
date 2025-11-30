package http

import "github.com/gofiber/fiber/v3"

func Health(ctx fiber.Ctx) error {
	ctx.Status(fiber.StatusOK)
	_ = ctx.JSON("UP!")
	return nil
}
