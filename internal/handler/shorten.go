// post api shorten handler

package handler

import (
	"errors"
	"time"

	"github.com/Suthar345Piyush/internal/service"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
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
		UserID:  userIDFromContext(c), // extracting this user id from auth middleware
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

	resp, err := h.svc.Shorten(c.Context(), req)

	if err != nil {
		if errors.Is(err, service.ErrInvalidURL) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		h.log.Error("shorten: service error", zap.String("url", body.LongURL), zap.Error(err))

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "internal server error",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(ShortenResponse{
		ShortCode: resp.ShortCode,
		ShortURL:  resp.ShortURL,
	})
}

// function for userIdFromContext
// returns the id

func userIDFromContext(c fiber.Ctx) int64 {
	id, _ := c.Locals("user_id").(int64)
	return id
}
