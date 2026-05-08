// redirection
// redirect to GET /:code - returns 301 -> permanent redirections
// it will take the fiber context

package handler

import (
	"errors"
	"time"

	"github.com/Suthar345Piyush/internal/events"
	"github.com/Suthar345Piyush/internal/service"
	"github.com/gofiber/fiber/v3"
	"go.uber.org/zap"
)

// handler function

func (h *Handler) Redirect(c fiber.Ctx) error {

	code := c.Params("code")

	if code == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "short code is required",
		})
	}

	// taking the long url
	// here showing all three errors here, like - url expired, not found, or any internal server error

	longURL, err := h.svc.GetURL(c.Context(), code)

	if err != nil {

		switch {

		case errors.Is(err, service.ErrNotFound):
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "short url not found",
			})

		case errors.Is(err, service.ErrExpired):
			return c.Status(fiber.StatusGone).JSON(fiber.Map{
				"error": "short url has expired",
			})

		default:
			h.log.Error("redirect: get url", zap.String("code", code), zap.Error(err))

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "internal server error occurred",
			})
		}
	}

	/*

			cdn will cache this in 24 hrs = 86400 seconds
			headers on the first
		  this s-maxage overrides the max-age for edge servers while leaving browser cache unaffected

			When both Cache-Control and CDN-Cache-Control headers are present, the more specific CDN-Cache-Control header takes precedence for edge caching decisions, allowing for a tiered strategy where edge servers and browsers have independent cache lifetimes.


			CDN-Cache-Control > Cache Control

	*/

	// this particularly an shared cache

	c.Set("Cache-Control", "public, max-age=86400, s-maxage=86400")
	c.Set("CDN-Cache-Control", "max-age=86400")

	// publishing the click event to kafka
	// the go routine in the event/producer.go for click event named "Record Click"

	h.producer.RecordClick(events.ClickEvent{
		ShortCode: code,
		Timestamp: time.Now().UTC(),
		IP:        c.IP(),
		Referer:   c.Get("Referer"),
		UserAgent: c.Get("User-Agent"),
	})

	return c.Redirect(longURL, fiber.StatusMovedPermanently)

}
