// post api shorten handler

package handler

import (
	"time"

	"github.com/Suthar345Piyush/internal/service"
	"github.com/gofiber/fiber/v3"
)

// shorten request struct
type shortenRequest struct {
	LongURL   string `json:"long_url"`
	ExpiresAt string `json:"expires_at,omitempty"` // optional
}

// shorten url response struct

type ShortenResponse struct {
	ShortCode string `json:"short_code"`
	ShortURL  string `json:"short_url"`
}

// shorten handler
// post request /api/shorten

// body will contain like {"long_url"="", "expiresAt"=""}

func (h *Handler) Shorten(c fiber.Ctx) error {

	var body shortenRequest

	if err := c.Bind().Body(&body); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid JSON body",
		})
	}

	if body.LongURL == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "long_url is required",
		})
	}

	req := service.ShortenRequest{
		LongURL: body.LongURL,
		UserID:  UserIDFromContext(c), // extracting this user id from auth middleware
	}

	if body.ExpiresAt != "" {
		t, err := time.Parse(time.RFC3339, body.ExpiresAt)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "expires_at must be RFC3339 format e.g. 2026-5-9T00:00:00Z",
			})
		}
		req.ExpiresAt = &t
	}

}

// function for userIdFromContext
// returns the id

func userIdFromContext(c *fiber.Ctx) int64 {
	id, _ := c.Locals("user_id").(int64)
	return id
}
