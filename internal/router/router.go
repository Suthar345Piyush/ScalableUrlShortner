// all fiber router at one place in this file

package router

import (
	"time"

	"github.com/Suthar345Piyush/internal/handler"
	"github.com/Suthar345Piyush/internal/middleware"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
)

// register function will connects all the routes with our fiber app

func Register(
	app *fiber.App,
	h *handler.Handler,
	redisClient *redis.ClusterClient,
	apiKey string,
	rateLimitMax int,
	rateLimitWindow time.Duration,
) {

	// get , everyone can access it, no rate limit, no auth, cdn will handle all the traffic disruptions

	app.Get("/:code", h.Redirect)

	// api group of auth and rate limit both

	api := app.Group("/api", middleware.RateLimit(redisClient, rateLimitMax, rateLimitWindow), middleware.Auth(apiKey))

	// url shorten and stats route

	api.Post("/shorten", h.Shorten)
	api.Get("/stats/:code", h.Stats)

	// lastly health check

	app.Get("/health", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{"status": "ok"})
	})

}
