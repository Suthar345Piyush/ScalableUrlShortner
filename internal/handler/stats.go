// stats handler using GET /api/stats/:code

package handler

import (
	"errors"

	"github.com/Suthar345Piyush/internal/service"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
)

// stats response struct, json body with req -> /api/stats/:code

type StatsResponse struct {
	ShortCode  string  `json:"short_code"`
	LongURL    string  `json:"long_url"`
	ClickCount int64   `json:"click_count"`
	CreatedAt  string  `json:"created_at"`
	ExpiresAt  *string `json:"expires_at,omitempty"`
}

// stats handler function, which returns metadata of short url

func (h *Handler) Stats(c fiber.Ctx) error {

	code := c.Params("code")

	if code == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "short code is required",
		})
	}

	record, err := h.svc.GetStats(c.Context(), code)

	if err != nil {
		if errors.Is(err, service.ErrNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "short url not found",
			})
		}

		h.log.Error("stats: service error", zap.String("code", code), zap.Error(err))

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "internal server error",
		})
	}

	// taking the response
	// specifying the created at format here

	resp := StatsResponse{
		ShortCode:  record.ShortCode,
		LongURL:    record.LongURL,
		ClickCount: record.ClickCount,
		CreatedAt:  record.CreatedAt.Format("2026-05-09T13:39:34Z"),
	}

	if record.ExpiresAt != nil {
		s := record.ExpiresAt.Format("2026-05-09T13:39:34Z")
		resp.ExpiresAt = &s
	}

	return c.JSON(resp)

}
