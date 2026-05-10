// consumer by kafka which writes click events to analytics database like clickhouse

// this consumer reads clickevents from the kafka's url.clicks, batch inserts them into the postgres table, which runs a separate deployment from api service

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Suthar345Piyush/internal/events"
	"github.com/Suthar345Piyush/internal/logger"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// main function

func main() {

	// logger
	log := logger.New(getEnv("LOG_LEVEL", "info"))

	defer log.Sync()

	// kafka broker (worker), splitted by comma

	brokers := splitComma(getEnv("KAFKA_BROKERS", "localhost:9092"))

	//kafka topic (channel)

	topic := getEnv("KAFKA_TOPIC", "url.clicks")

	// analytics dsn(data source name)

	analyticsDSN := getEnv("ANALYTICS_DB_DSN", "postgres://user:pass@localhost:5432/analytics?sslmode=disable")

	// we have to make a postgres analytics pool

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, analyticsDSN)

	if err != nil {
		log.Fatal("analytics db: connection failed", zap.Error(err))
	}

	defer pool.Close()

	// schema verification of the analytics db - basically schema initialization
	if err := ensureSchema(ctx, pool); err != nil {
		log.Fatal("analytics db: schema init failed", zap.Error(err))
	}

	/*
		the main kafka consumer client, using kgo kafka client library for apache kafka, one client can both consume and produce, it can do this as a single/alone client or in a group  as well
	*/

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("url-click-consumer"),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		log.Fatal("kafka: consumer init failed", zap.Error(err))
	}

	defer client.Close()

	// quit channel

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Info("consumer ready", zap.Strings("brokers", brokers), zap.String("topic", topic))

	/*
		using the select with channels, the default runs immediately if no channel operation is ready, using to make it non blocking, this will shutdown when quit receives a signal, along with this, it is an infinite loop, which checks if "quit" channel has data, if "Yes" then shutdown it, if "No" then run the default, then loop runs again instantly (very fast)
	*/

	for {
		select {
		case <-quit:
			log.Info("consumer shutting down")
			return

		default:
		}
	}

	// poll context

	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

	fetches := client.PollFetches(pollCtx)
	cancel()

	if fetches.IsClientClosed() {
		return
	}

	if errs := fetches.Errors(); len(errs) > 0 {
		for _, e := range errs {
			log.Error("kafka fetch error", zap.Error(e.Err))
			continue
		}
	}

	var batch []events.ClickEvent

	fetches.EachRecord(func(r *kgo.Record) {

		var e events.ClickEvent

		if err := json.Unmarshal(r.Value, &e); err != nil {
			log.Warn("consumer: unmarshal error", zap.Error(err))
			return
		}
		batch = append(batch, e)

	})

	if len(batch) > 0 {
		if err := insertBatch(ctx, pool, batch); err != nil {
			log.Error("consumer: insert batch failure", zap.Error(err))
			continue
		}
		log.Info("consumer: batch inserted", zap.Int("count", len(batch)))
	}

	if err := client.CommitUncommittedOffsets(ctx); err != nil {
		log.Error("consumer: commit offesets failed", zap.Error(err))
	}
}

// using postgresql copy protocol will bulk inserts the click events

func insertBatch(ctx context.Context, pool *pgxpool.Pool, batch []events.ClickEvent) error {
	const q = `
	  INSERT INTO click_events (short_code, ts, ip, country, referer, user_agent)
		VALUES ($1, $2, $3, $4, $5, $6)
	 
	 `

	// connection

	conn, err := pool.Acquire(ctx)

	if err != nil {
		return fmt.Errorf("insertBatch acquire: %w", err)
	}

	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("insertBatch begin tx: %w", err)
	}

	defer tx.Rollback(ctx)

	for _, e := range batch {
		if _, err := tx.Exec(ctx, q, e.ShortCode, e.Timestamp, e.IP, e.Country, e.Referer, e.UserAgent); err != nil {

			return fmt.Errorf("insertBatch exec: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// ensure schema function to create click events table

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	const q = `
		 CREATE TABLE IF NOT EXISTS click_events (
			 id  BIGSERIAL PRIMARY KEY,
			 short_code VARCHAR(12)  NOT NULL,
			 ts TIMESTAMPZ NOT NULL,
			 ip TEXT,
			 country TEXT,
			 referer TEXT,
			 user_agent TEXT,
		 );

		 CREATE INDEX IF NOT EXISTS idx_ce_short_code ON click_events (short_code);
		 CREATE INDEX IF NOT EXISTS idx_ce_ts ON click_events (ts DESC);
		`

	_, err := pool.Exec(ctx, q)
	return err
}

// get env function

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// split comma

func splitComma(s string) []string {
	var out []string

	start := 0

	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if t := trim(s[start:i]); t != "" {
				out = append(out, t)
			}
			start = i + 1
		}
	}

	if t := trim(s[start:]); t != "" {
		out = append(out, t)
	}

	return out
}

// trim helper function

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}

	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}

	return s
}
