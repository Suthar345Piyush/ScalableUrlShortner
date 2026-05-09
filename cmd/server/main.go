// everything inside this main file, with graceful shutdown on sigint and sigterm

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Suthar345Piyush/internal/cache"
	"github.com/Suthar345Piyush/internal/config"
	"github.com/Suthar345Piyush/internal/events"
	"github.com/Suthar345Piyush/internal/handler"
	"github.com/Suthar345Piyush/internal/idgen"
	"github.com/Suthar345Piyush/internal/logger"
	"github.com/Suthar345Piyush/internal/router"
	"github.com/Suthar345Piyush/internal/service"
	"github.com/Suthar345Piyush/internal/store"
	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {

	// config

	cfg := config.Load()

	// logger

	log := logger.New(cfg.BaseURL)

	defer log.Sync()

	log.Info("starting url-shortner", zap.String("port", cfg.Port))

	// database sharding router

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	defer cancel()

	// shard router

	shardRouter, err := store.NewShardRouter(ctx, cfg.DBShards)

	if err != nil {
		log.Fatal("db: shard router init failed", zap.Error(err))
	}

	defer shardRouter.Close()

	// redis cache

	redisCache, err := cache.NewRedis(cfg.RedisAddrs)

	if err != nil {
		log.Fatal("redis: init failed", zap.Error(err))
	}

	// local lru

	lruCache, err := cache.NewLRU(cfg.LRUMaxItems)

	if err != nil {
		log.Fatal("lru: init failed", zap.Error(err))
	}

	// snowflake id generator

	gen, err := idgen.New(cfg.NodeID)

	if err != nil {
		log.Fatal("idgen: init failed", zap.Error(err))
	}

	// kafka producer

	producer, err := events.NewKafkaProducer(cfg.KafkaBrokers, cfg.KafkaTopic, log)

	if err != nil {
		log.Fatal("kafka: producer init failed", zap.Error(err))
	}

	defer producer.Close()

	// the complete service layer

	svc := service.New(shardRouter, *redisCache, *lruCache, gen, producer, cfg.BaseURL, log)

	// handlers

	h := handler.New(svc, producer, log)

	// fiber application - fiber named instance

	app := fiber.New(fiber.Config{
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,

		// disabling the fiber's error logs, cause we are using the zap logger for logging

		ErrorHandler: func(c fiber.Ctx, err error) error {

			log.Error("unhandled fiber error", zap.Error(err))

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "internal server error",
			})

		},
	})

	// creating the raw redis cluster client

	rawRedis := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: cfg.RedisAddrs,
	})

	router.Register(app, h, rawRedis, cfg.APIKey, cfg.RateLimitMax, cfg.RateLimitWindow)

	// graceful shutdown to the system

	// making channel, shutdown process on sigint, sigterm

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// goroutine  for listen to fiber app

	go func() {
		if err := app.Listen(":" + cfg.Port); err != nil {
			log.Error("fiber listen error", zap.Error(err))
		}
	}()

	log.Info("server ready", zap.String("port", cfg.Port))
	<-quit

	log.Info("shutdown signal received")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer shutdownCancel()

	if err := app.ShutdownWithContext(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", zap.Error(err))
	}

	log.Info("server stopped cleanly")

}

// a helper function to check env availaibility

func envCheck(key string) string {
	v := os.Getenv(key)

	if v == "" {
		panic("required env are not set: " + key)
	}
	return v
}
