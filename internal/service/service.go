// all the business logic is in the service

package service

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/Suthar345Piyush/internal/cache"
	"github.com/Suthar345Piyush/internal/events"
	"github.com/Suthar345Piyush/internal/idgen"
	"github.com/Suthar345Piyush/internal/store"
	"github.com/Suthar345Piyush/pkg/base62"
	"go.uber.org/zap"
)

// ShortenRequest struct

type ShortenRequest struct {
	LongURL   string
	UserID    int64
	ExpiresAt *time.Time
}

// shorten response

type ShortenResponse struct {
	ShortCode string
	ShortURL  string
}

// url service

type URLService interface {
	GetURL(ctx context.Context, code string) (string, error)
	Shorten(ctx context.Context, req ShortenRequest) (*ShortenResponse, error)
	DeleteURL(ctx context.Context, code string) error
	GetStats(ctx context.Context, code string) (*store.URL, error)
}

// main service struct

type service struct {
	store    store.URLStore
	redis    cache.RedisCache
	lru      cache.LRUCache
	idgen    idgen.Generator
	producer events.Producer
	baseURL  string
	log      *zap.Logger
}

// function to fit all into the url service

func New(st store.URLStore, redis cache.RedisCache, lru cache.LRUCache, gen idgen.Generator, producer events.Producer, baseURL string, log *zap.Logger) URLService {
	return &service{
		store:    st,
		redis:    redis,
		lru:      lru,
		idgen:    gen,
		producer: producer,
		baseURL:  baseURL,
		log:      log,
	}
}

// get url used to short code to a long url
// lru -> redis -> db

func (s *service) GetURL(ctx context.Context, code string) (string, error) {

	// local lru

	if longURL, err := s.lru.Get(ctx, code); err == nil {
		return longURL, nil
	}

	// redis

	if longURL, err := s.redis.Get(ctx, code); err == nil {
		_ = s.lru.Set(ctx, code, longURL)

		return longURL, nil
	}

	// database record

	record, err := s.store.Get(ctx, code)

	if errors.Is(err, store.ErrNotFound) {
		return "", store.ErrNotFound
	}

	if err != nil {
		return "", fmt.Errorf("service: get url: %w", err)
	}

	// checking the expiry

	if record.ExpiresAt != nil && record.ExpiresAt.Before(time.Now()) {
		return "", ErrExpired
	}

	// back fill both cache (lru and redis) if they misses

	_ = s.redis.Set(ctx, code, record.LongURL)
	_ = s.lru.Set(ctx, code, record.LongURL)

	// keep incermenting the counter

	go func() {
		if err := s.store.IncrClick(context.Background(), code); err != nil {
			s.log.Warn("increment click failed", zap.String("code", code), zap.Error(err))
		}
	}()

	return record.LongURL, nil

}

// short url from long url

func (s *service) Shorten(ctx context.Context, req ShortenRequest) (*ShortenResponse, error) {

	if err := validateURL(req.LongURL); err != nil {
		return nil, err
	}

	id := s.idgen.Next()

	code := base62.Encode(id)

	u := store.URL{
		ShortCode: code,
		LongURL:   req.LongURL,
		UserID:    req.UserID,
		ExpiresAt: req.ExpiresAt,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.store.Insert(ctx, &u); err != nil {
		return nil, fmt.Errorf("service: shorten insert: %w", err)
	}

	// first redirect should be from the cache , so we will fill it

	_ = s.redis.Set(ctx, code, req.LongURL)
	_ = s.lru.Set(ctx, code, req.LongURL)

	return &ShortenResponse{
		ShortCode: code,
		ShortURL:  s.baseURL + "/" + code,
	}, nil
}

// url validation function

func validateURL(rawURL string) error {
	u, err := url.ParseRequestURI(rawURL)

	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return ErrInvalidURL
	}

	return nil
}

// delete url

func (s *service) DeleteURL(ctx context.Context, code string) error {

	if err := s.store.Delete(ctx, code); err != nil {
		return fmt.Errorf("service: delete: %w", err)
	}

	// delete from redis and lru cache

	_ = s.redis.Delete(ctx, code)
	_ = s.lru.Delete(ctx, code)

	return nil
}

// get stats

func (s *service) GetStats(ctx context.Context, code string) (*store.URL, error) {

	record, err := s.store.Get(ctx, code)

	if errors.Is(err, store.ErrNotFound) {
		return nil, ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("service: stats: %w", err)
	}

	return record, nil
}
