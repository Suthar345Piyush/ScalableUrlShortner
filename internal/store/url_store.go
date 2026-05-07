// pgx queries

package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// url store for single pgx pool - one shard

type urlStore struct {
	pool *pgxpool.Pool
}

// new url store is new instance

func newURLStore(pool *pgxpool.Pool) *urlStore {
	return &urlStore{pool: pool}
}

const queryTimeout = 200 * time.Millisecond

// get function, it will return the url

func (s *urlStore) Get(ctx context.Context, code string) (*URL, error) {

	ctx, cancel := context.WithTimeout(ctx, queryTimeout)

	defer cancel()

	// query

	const q = `SELECT short_code, long_url, user_id, created_at, expire_at, click_count FROM urls WHERE short_code = $1`

	var u URL

	err := s.pool.QueryRow(ctx, q, code).Scan(
		&u.ShortCode,
		&u.LongURL,
		&u.UserID,
		&u.CreatedAt,
		&u.ExpiresAt,
		&u.ClickCount,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("url_store get %q: %w", code, err)
	}

	return &u, nil

}

// insert the content in the pool

func (s *urlStore) Insert(ctx context.Context, u *URL) error {

	ctx, cancel := context.WithTimeout(ctx, queryTimeout)

	defer cancel()

	const q = `INSERT INTO urls (short_code, long_url, user_id, created_at, expires_at) VALUES ($1, $2, $3, $4, $5)`

	_, err := s.pool.Exec(ctx, q,
		u.ShortCode,
		u.LongURL,
		u.UserID,
		u.CreatedAt,
		u.ExpiresAt,
	)

	if err != nil {
		return fmt.Errorf("url_store insert %q: %w", u.ShortCode, err)
	}

	return nil

}

// delete

func (s *urlStore) Delete(ctx context.Context, code string) error {

	ctx, cancel := context.WithTimeout(ctx, queryTimeout)

	defer cancel()

	const q = `DELETE FROM urls WHERE short_code = $1`

	_, err := s.pool.Exec(ctx, q, code)

	if err != nil {
		return fmt.Errorf("url_store delete %q: %w", code, err)
	}

	return nil

}

// increment the counter when clicked

func (s *urlStore) IncrClick(ctx context.Context, code string) error {

	ctx, cancel := context.WithTimeout(ctx, queryTimeout)

	defer cancel()

	const q = `UPDATE urls SET click_count = click_count + 1 WHERE short_code = $1`

	_, err := s.pool.Exec(ctx, q, code)

	if err != nil {
		return fmt.Errorf("url_store incr_click %q: %w", code, err)
	}

	return nil

}
