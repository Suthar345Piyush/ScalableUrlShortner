// sliding window rate limiting implementation

package middleware

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/gofiber/fiber/v3"
)

// rate limiter struct

type rateLimiter struct {
	client *redis.ClusterClient
	max    int           // max request user/client can do
	window time.Duration // window time frame/span
}

// rate limiting based on IP, so rate limit function returns an fiber middleware that limits requests per IP

func RateLimit(client *redis.ClusterClient, max int, window time.Duration) fiber.Handler {
	rl := &rateLimiter{client: client, max: max, window: window}
	return rl.handle
}

// rate limit function returns this handle as fiber middleware handler with limits request per IP
// it is fiber middleware which requires context ctx

func (rl *rateLimiter) handle(c fiber.Ctx) error {

	// taking the IP from the context (c), not the pointer to interface, but we want the interface itself

	key := fmt.Sprintf("rl:%s", c.IP())

	ctx := context.Background()

	// using redis INCR and EXPIRE way to implement the rate limiting

	count, err := rl.client.Incr(ctx, key).Result()

	// if any error occurs then we sent it to next rather then stopping it, which don't block/restrict the legit users

	if err != nil {
		return c.Next()
	}

	// setting the expiry when first request hits to window

	if count == 1 {
		rl.client.Expire(ctx, key, rl.window)
	}

	// if requests count exceeded the window limit then will show the "too many requests"  response

	if count > int64(rl.max) {
		return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
			"error": "rate limit exceeded - try again later",
		})
	}

	// client can self throttle , so exposing the rate limit headers

	c.Set("X-RateLimit-Limit", fmt.Sprintf("%d", rl.max))
	c.Set("X=RateLimit-Remaining", fmt.Sprintf("%d", int64(rl.max)-count))

	return c.Next()

}
