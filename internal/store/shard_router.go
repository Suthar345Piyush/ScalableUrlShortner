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

	"github.com/jackc/pgx/v5/pgxpool"
)

// shard router struct
type ShardRouter struct {
	pools []*pgxpool.Pool
}

// function to make pgxpool connection with the DSN (data source name) it will contain our connection details
// dsn it would contain one element at least, cannot be empty

func NewShardRouter(ctx context.Context, dsns []string) (*ShardRouter, error) {

}
