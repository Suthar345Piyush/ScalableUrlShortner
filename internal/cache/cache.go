// url cache interface

// this will provide two cache - 1. redis cache by redis cluster, 2 - lrucache - by ristretto an inprocess lru cache

package cache

import (
	"context"
	"errors"
)

// it will returned when key is not present in the cache

var ErrCacheMiss = errors.New("cache: miss")

// url cache is by both redis and lru

type URLCache interface {

	// Get will retrieve the long url for the given short code, it will show ErrCacheMiss, if the key doesn't exist

	Get(ctx context.Context, code string) (string, error)

	// Set will store the mapping with the default TTL

	Set(ctx context.Context, code, longURL string) error

	// delete will remove the key, it will be called when short url is deleted or updated

	Delete(ctx context.Context, code string) error
}
