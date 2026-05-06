// package config, taking all will be taken from the env file
// using viper for managing configs

package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// config struct which holds all the configs for url shortner

/*

- all kind of config like, server, snowflake id, db, redis cluster node address, kafka broker address

- lru size, rate limit, base url , api key, logging level

*/

type Config struct {

	// server

	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// snowflake random and unique id for each  running shard

	NodeID int64

	// db
	DBShards []string

	// redis cluster nodes  address, using by client to connect with the redis cluster

	RedisAddrs []string

	// kafka broker and kafka topic - Broker(Topic)

	KafkaBrokers []string
	KafkaTopic   []string

	// local lru cache size

	LRUMaxItems int64

	// rate limiting per window  and maximum limit , time duration for rate limit window

	RateLimitMax    int
	RateLimitWindow time.Duration

	// base url, for  client side to make (short url)

	BaseURL string

	APIKey string

	// logging level  - warn | info | debug | error

	LogLevel string
}

// setting up the viper config to spread out the *Config

func Load() *Config {

	v := viper.New()

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// some defaults values

	v.SetDefault("PORT", "8080")
	v.SetDefault("READ_TIMEOUT", "5s")
	v.SetDefault("WRITE_TIMEOUT", "10s")
	v.SetDefault("NODE_ID", 0)
	v.SetDefault("DB_SHARDS", "postgres://user:pass@localhost:5432/urls?sslmode=disable")
	v.SetDefault("REDIS_ADDRS", "localhost:6379")
	// will change 9092 -> 9093 to encrypted client connection to the redis cluster, for now it is unencrypted at port 9092
	v.SetDefault("KAFKA_BROKERS", "localhost:9092")
	v.SetDefault("KAFKA_TOPIC", "url.clicks")

	v.SetDefault("LRU_MAX_ITEMS", 50_000)
	v.SetDefault("RATE_LIMIT_MAX", 60)
	v.SetDefault("RATE_LIMIT_WINDOW", "1m")
	v.SetDefault("BASE_URL", "http://localhost:8080")
	v.SetDefault("API_KEY", "will-change-it")
	v.SetDefault("LOG_LEVEL", "info")

	return &Config{
		Port:         v.GetString("PORT"),
		ReadTimeout:  v.GetDuration("READ_TIMEOUT"),
		WriteTimeout: v.GetDuration("WRITE_TIMEOUT"),
		NodeID:       v.GetInt64("NODE_ID"),
		DBShards:     splitTrimmed(v.GetString("DB_SHARDS")),
	}

}

// spliting strings and removing the whitespace and discarding the empty strings
// done on comma seperated

func splitTrimmed(s string) []string {

	parts := strings.Split(s, ",")

	out := make([]string, 0, len(parts))

	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}

	return out

}
