/*

   the shard router will make individual pgx connection pool for each shard and route every query

	 to their correct shard by computing using this formula


	 formula to query a shard at a given time  = hash(short_code) % N

	 N -> numShards

	 -> firstly it will hash the short code and normalize it by taking modulo with total number of shards we have

*/

package store

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// shard router struct
type ShardRouter struct {
	pools []*pgxpool.Pool
}

// function to make pgxpool connection with the DSN (data source name) it will contain our connection details
// dsn it would contain one element at least, cannot be empty

func NewShardRouter(ctx context.Context, dsns []string) (*ShardRouter, error) {

	// cheking length of dsn

	if len(dsns) == 0 {
		return nil, fmt.Errorf("shard_router: at least one DSN required")
	}

	// making a pgx pool slice

	pools := make([]*pgxpool.Pool, 0, len(dsns))

	// iterate on the dsns

	for i, dsn := range dsns {

		cfg, err := pgxpool.ParseConfig(dsn)

		if err != nil {
			return nil, fmt.Errorf("shard_router: shard %d invalid dsn: %w", i, err)
		}

		cfg.MaxConns = 30
		cfg.MinConns = 5

		pool, err := pgxpool.NewWithConfig(ctx, cfg)

		if err != nil {
			return nil, fmt.Errorf("shard_router: shard %d connect failed: %w", i, err)
		}

		// ping check

		if err := pool.Ping(ctx); err != nil {
			return nil, fmt.Errorf("shard_router: shard %d ping failed: %w", i, err)
		}

		pools = append(pools, pool)

	}

	return &ShardRouter{pools: pools}, nil

}

// now we get the pool responsible for the short code

func (r *ShardRouter) shardFor(code string) *pgxpool.Pool {

	h := fnv.New32a()
	_, _ = h.Write([]byte(code))

	// hash(short_code) % N

	return r.pools[h.Sum32()%uint32(len(r.pools))]

}

// total no. shards which are configured

func (r *ShardRouter) NumShards() int {
	return len(r.pools)
}

// closing all the connection pools, calling this when application shutdown
// closing each pool from pools

func (r *ShardRouter) Close() {

	for _, p := range r.pools {
		p.Close()
	}

}

// get to take url record from shard

func (r *ShardRouter) Get(ctx context.Context, code string) (*URL, error) {
	return newURLStore(r.shardFor(code)).Get(ctx, code)
}

// insert function to write the url to correct shard

func (r *ShardRouter) Insert(ctx context.Context, u *URL) error {
	return newURLStore(r.shardFor(u.ShortCode)).Insert(ctx, u)
}

// delete will remove the url from shard

func (r *ShardRouter) Delete(ctx context.Context, code string) error {
	return newURLStore(r.shardFor(code)).Delete(ctx, code)
}

// increament click  counter on shard

func (r *ShardRouter) IncrClick(ctx context.Context, code string) error {
	return newURLStore(r.shardFor(code)).IncrClick(ctx, code)
}
