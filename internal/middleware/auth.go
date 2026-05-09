package middleware

import (
	"strings"

	"github.com/gofiber/fiber/v3"
)

// Auth returns a Fiber middleware that validates a Bearer API key.
// The expected key is read from the X-API-Key header or the Authorization
// header (Bearer <key>). Pass the valid key from your Config.

func Auth(validKey string) fiber.Handler {
	return func(c fiber.Ctx) error {
		key := c.Get("X-API-Key")
		if key == "" {
			auth := c.Get("Authorization")
			key = strings.TrimPrefix(auth, "Bearer ")
		}
		if key != validKey || validKey == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "unauthorized — valid API key required",
			})
		}
		return c.Next()
	}
}
