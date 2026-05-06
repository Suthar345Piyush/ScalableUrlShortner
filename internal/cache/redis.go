// redis cluster client
// with multiuser redis cluster, and  for single one, and get, set, and delete function for key

package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisTTL = 24 * time.Hour
const keyPrefix = "url:"

// redis cache for redis cluster

type RedisCache struct {
	client *redis.ClusterClient
}

// redis cache connected to given cluster nodes addresses

func NewRedis(addrs []string) (*RedisCache, error) {

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrs,
		PoolSize:     100,
		MinIdleConns: 10,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		MaxRetries:   3,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	return &RedisCache{client: client}, nil

}

// for single redis node instance , for local development and testing

func NewRedisSingle(addr string) (*RedisCache, error) {

	single := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     100,
		MinIdleConns: 10,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		MaxRetries:   3,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	if err := single.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	return NewRedis([]string{addr})
}

// get will retrieves the long url , and return errcachemiss on absence of key

func (r *RedisCache) Get(ctx context.Context, code string) (string, error) {

	val, err := r.client.Get(ctx, keyPrefix+code).Result()

	if errors.Is(err, redis.Nil) {
		return "", ErrCacheMiss
	}

	if err != nil {
		return "", fmt.Errorf("redis get %q: %w", code, err)
	}

	return val, nil
}

// set will store the key and map to redis ttl (24hrs)

func (r *RedisCache) Set(ctx context.Context, code, longURL string) error {

	if err := r.client.Set(ctx, keyPrefix+code, longURL, redisTTL).Err(); err != nil {
		return fmt.Errorf("redis is %q: %w", code, err)
	}

	return nil

}

// delete will remove the key from redis cluster

func (r *RedisCache) Delete(ctx context.Context, code string) error {
	if err := r.client.Del(ctx, keyPrefix+code); err != nil {
		return fmt.Errorf("redis del %q: %w", code, err)
	}

	return nil
}
