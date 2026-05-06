// the service layer will depand on this URL store interface, not on the pgx directly

package store

import (
	"context"
	"errors"
	"time"
)

// returning errnot found , if no row matches the short code

var ErrNotFound = errors.New("store: short code not found")

// main url struct

type URL struct {
	ShortCode  string
	LongURL    string
	UserID     int64
	CreatedAt  time.Time
	ExpiresAt  *time.Time // it can no expiry if, mentioned as nil
	ClickCount int64
}

// the url layer from which the service will interect and for read and writes url's

type URLStore interface {
	Get(ctx context.Context, code string) (*URL, error)

	Insert(ctx context.Context, u *URL) error

	Delete(ctx context.Context, code string) error

	// atomically increment the click counter for a short code

	IncrClick(ctx context.Context, code string) error
}
