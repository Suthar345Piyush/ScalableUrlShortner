// ristretto (golang) in-process LRU cache
// ristretto from graph database

package cache

import (
	"context"
	"fmt"

	"github.com/dgraph-io/ristretto"
)

// lru ttl time seconds

const lruTTLSeconds = 300

// ristretto lru cache

type LRUCache struct {
	cache *ristretto.Cache
}

// new lru function

func NewLRU(maxItems int64) (*LRUCache, error) {

	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: maxItems * 10,
		MaxCost:     maxItems,
		BufferItems: 64,
	})

	if err != nil {
		return nil, fmt.Errorf("lru: failed to create ristretto cache: %w", err)
	}

	return &LRUCache{cache: c}, nil
}

// Get function to return the err, if key is absent

func (l *LRUCache) Get(_ context.Context, code string) (string, error) {

	val, ok := l.cache.Get(code)

	if !ok {
		return "", ErrCacheMiss
	}

	url, ok := val.(string)

	if !ok {
		return "", ErrCacheMiss
	}

	return url, nil
}

// set will store the url with our ttl, ttl is in nanoseconds

func (l *LRUCache) Set(_ context.Context, code, longURL string) error {
	l.cache.SetWithTTL(code, longURL, 1, lruTTLSeconds*1e9)

	return nil
}

// delete will going delete the key from the lru

func (l *LRUCache) Delete(_ context.Context, code string) error {
	l.cache.Del(code)
	return nil
}
