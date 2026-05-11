# URL Shortener

A production-grade, horizontally scalable URL shortener built in **Go**. It uses Base62 encoding over Snowflake IDs to generate random and unique short codes, routes reads and writes across multiple PostgreSQL database shards, caches fast at three layers (CDN → Redis → in-process LRU cache), and streams every click event to Kafka for asynchronous analytics — all without a single global lock or coordination bottleneck.

---

## Features

- **Base62 short codes** — 7-character codes from a Snowflake int64. No collision checks needed; uniqueness is structural.
- **DB sharding** — FNV hash-based routing across N PostgreSQL shards. Each shard has one primary (writes) and two replicas (reads).
- **Three-layer caching** — In-process LRU (ristretto) → Redis cluster → DB shard. Cache hit rate target: 95%+.
- **Snowflake ID generation** — Decentralized, time-ordered 64-bit IDs. Up to 4096 IDs/ms per node, no coordination required.
- **Async analytics** — Every redirect fires a click event to Kafka without blocking the response. A separate consumer binary batch-inserts events into an analytics DB.
- **URL expiry** — Optional `expires_at` field per short URL. Expired links return HTTP 410 Gone.
- **Per-IP rate limiting** — Redis-backed sliding window on `/api/*` routes.
- **API key authentication** — Bearer token / `X-API-Key` header on write endpoints.
- **Graceful shutdown** — SIGINT/SIGTERM drains in-flight requests before stopping.
- **Lean Docker image** — Multi-stage build produces a ~10 MB distroless binary image.
- **Structured JSON logging** — Uber's Zap logger with configurable level, ships cleanly to any log aggregator.

---

## Tech Stack

| Layer | Technology | Library / Tool |
|---|---|---|
| Language | Go 1.22+ | — |
| HTTP framework | Fiber v3 | `github.com/gofiber/fiber/v3` |
| In-process cache | Ristretto LRU | `github.com/dgraph-io/ristretto` |
| Distributed cache | Redis 7 (Cluster) | `github.com/redis/go-redis/v9` |
| Database | PostgreSQL 18 (sharded) | `github.com/jackc/pgx/v5` |
| ID generation | Snowflake | `github.com/bwmarrin/snowflake` |
| Short code encoding | Base62 | `pkg/base62` (zero-dependency, written in-house) |
| Event streaming | Apache Kafka | `github.com/twmb/franz-go` |
| Configuration | Viper | `github.com/spf13/viper` |
| Logging | Zap | `go.uber.org/zap` |
| Containerization | Docker + distroless | — |
| CDN / Edge cache | Cloudflare | — |

---

## Architecture Overview

```
Client
  │
  ▼
Cloudflare CDN  ──────────────────────────────► 301 (cache hit, < 10 ms)
  │ cache miss
  ▼
Load Balancer
  │
  ▼
API Server (Fiber)  ── Local LRU (ristretto) ──► return URL (< 1 µs)
  │ LRU miss          ── Redis Cluster ──────────► return URL (~ 1 ms)
  │ Redis miss
  ▼
Shard Router  ──  hash(code) % N
  │
  ├── Shard 0  (primary + 2 replicas)
  ├── Shard 1  (primary + 2 replicas)
  └── Shard N  (primary + 2 replicas)
  │
  ▼ async (goroutine)
Kafka  ──► Consumer binary  ──► Analytics PostgreSQL
```

**Read path:** CDN → LB → API → LRU → Redis → DB shard replica → back-fill caches → 301  
**Write path:** API → Snowflake ID → Base62 encode → DB shard primary → populate Redis + LRU

---

## Project Structure

```
url-shortener/
├── cmd/
│   ├── server/main.go          # HTTP API binary — entry point, wires all deps
│   └── consumer/main.go        # Kafka consumer binary — writes clicks to analytics DB
│
├── internal/
│   ├── config/config.go        # All env vars loaded via Viper, returned as Config struct
│   ├── logger/logger.go        # Zap JSON logger constructor
│   ├── idgen/idgen.go          # Snowflake node wrapper + Generator interface
│   │
│   ├── cache/
│   │   ├── cache.go            # URLCache interface (Get / Set / Delete)
│   │   ├── redis.go            # go-redis v9 cluster client implementation
│   │   └── lru.go              # Ristretto in-process LRU implementation
│   │
│   ├── store/
│   │   ├── store.go            # URLStore interface + URL domain model
│   │   ├── shard_router.go     # FNV hash router — picks the right pgxpool per code
│   │   ├── url_store.go        # pgx queries: Get / Insert / Delete / IncrClick
│   │   └── migrations/
│   │       └── 001_create_urls.sql
│   │
│   ├── events/
│   │   ├── events.go           # ClickEvent struct + Producer interface
│   │   └── producer.go         # franz-go Kafka producer implementation
│   │
│   ├── service/
│   │   ├── service.go          # Business logic: LRU→Redis→DB reads, write path
│   │   └── errors.go           # Sentinel errors: ErrNotFound, ErrExpired, ErrInvalidURL
│   │
│   ├── middleware/
│   │   ├── auth.go             # API key authentication (X-API-Key / Bearer)
│   │   └── ratelimit.go        # Redis sliding-window rate limiter
│   │
│   ├── handler/
│   │   ├── handler.go          # Handler struct + constructor
│   │   ├── redirect.go         # GET /:code  → 301
│   │   ├── shorten.go          # POST /api/shorten
│   │   └── stats.go            # GET /api/stats/:code
│   │
│   └── router/router.go        # All Fiber routes registered in one place
│
├── pkg/
│   └── base62/
│       ├── base62.go           # Encode(int64) string / Decode(string) int64
│       └── base62_test.go      # Table-driven round-trip tests
│
├── docker-compose.yml          # Local dev: Postgres ×2, Redis, Kafka, Analytics DB
├── .env.example                # Template for all environment variables                 
└── go.mod
```

---

## Prerequisites

Make sure the following are installed on your machine before starting:

| Tool | Minimum version | Check |
|---|---|---|
| Go | 1.22+ | `go version` |
| Docker | 24.x | `docker --version` |
| Docker Compose | v2 | `docker compose version` |
| Make | any | `make --version` |
| psql | any | `psql --version` |

---

## Getting Started — Local Development

Follow these steps in order. Every command is run from the project root.

### Step 1 — Clone the repository

```bash
git clone https://github.com/you/url-shortener.git
cd url-shortener
```

### Step 2 — Install Go dependencies

```bash
go mod download
```

This downloads all modules listed in `go.mod` into the local module cache.

### Step 3 — Set up environment variables

```bash
cp .env.example .env
```

Open `.env` and at minimum change these two values:

```env
API_KEY=your-strong-random-secret      # used to authenticate POST /api/shorten
BASE_URL=http://localhost:8080         # leave as-is for local dev
```

Generate a secure API key with:

```bash
openssl rand -hex 32
```

### Step 4 — Start all infrastructure services

```bash
make docker-up
```

This starts the following containers via `docker-compose.yml`:

| Container | Service | Port |
|---|---|---|
| `postgres-shard-0` | PostgreSQL shard 0 | 5432 |
| `postgres-shard-1` | PostgreSQL shard 1 | 5433 |
| `redis` | Redis (single-node) | 6379 |
| `zookeeper` | Kafka dependency | 2181 |
| `kafka` | Kafka broker | 9092 |
| `postgres-analytics` | Analytics PostgreSQL | 5434 |

Wait ~10 seconds for all containers to be healthy before proceeding.

Verify everything is running:

```bash
docker compose ps
```

All services should show status `Up` or `running`.

### Step 5 — Run database migrations

The migration file must be applied to **every shard**. Run it for each shard DSN:

```bash
# Shard 0
psql "postgres://urluser:urlpass@localhost:5432/urls?sslmode=disable" \
  -f internal/store/migrations/001_create_urls.sql

# Shard 1
psql "postgres://urluser:urlpass@localhost:5433/urls?sslmode=disable" \
  -f internal/store/migrations/001_create_urls.sql
```

Or use the Makefile shortcut (run once per shard):

```bash
make migrate SHARD_DSN="postgres://urluser:urlpass@localhost:5432/urls?sslmode=disable"
make migrate SHARD_DSN="postgres://urluser:urlpass@localhost:5433/urls?sslmode=disable"
```

### Step 6 — Start the API server

```bash
make dev
```

You should see output like:

```json
{"ts":"2025-01-01T00:00:00.000Z","level":"INFO","msg":"starting url-shortener","port":"8080"}
{"ts":"2025-01-01T00:00:00.000Z","level":"INFO","msg":"server ready","port":"8080"}
```

The server is now listening on `http://localhost:8080`.

### Step 7 — (Optional) Start the Kafka consumer

In a separate terminal, start the consumer binary that reads click events from Kafka and writes them to the analytics DB:

```bash
make dev-consumer
```

---

## API Reference

### Shorten a URL

**`POST /api/shorten`**

Requires authentication via `X-API-Key` header or `Authorization: Bearer <key>`.

**Request**

```bash
curl -X POST http://localhost:8080/api/shorten \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "long_url": "https://www.example.com/some/very/long/path?with=query&params=true",
    "expires_at": "2025-12-31T23:59:59Z"
  }'
```

`expires_at` is optional. Omit it for a link that never expires.

**Response `201 Created`**

```json
{
  "short_code": "aK9mX2p",
  "short_url": "http://localhost:8080/aK9mX2p"
}
```

---

### Redirect via short code

**`GET /:code`**

No authentication required. Returns a `301 Moved Permanently` redirect.

```bash
curl -L http://localhost:8080/aK9mX2p
```

The response includes cache headers so CDN edge nodes cache the redirect:

```
HTTP/1.1 301 Moved Permanently
Location: https://www.example.com/some/very/long/path?with=query&params=true
Cache-Control: public, max-age=86400, s-maxage=86400
CDN-Cache-Control: max-age=86400
```

**Error responses:**

| Status | Meaning |
|---|---|
| `404 Not Found` | Short code does not exist in any layer |
| `410 Gone` | Short code exists but the link has passed its `expires_at` |

---

### Get link statistics

**`GET /api/stats/:code`**

Requires authentication.

```bash
curl http://localhost:8080/api/stats/aK9mX2p \
  -H "X-API-Key: your-api-key"
```

**Response `200 OK`**

```json
{
  "short_code": "aK9mX2p",
  "long_url": "https://www.example.com/some/very/long/path?with=query&params=true",
  "click_count": 142,
  "created_at": "2025-01-01T10:00:00Z",
  "expires_at": "2025-12-31T23:59:59Z"
}
```

---

### Health check

**`GET /health`**

```bash
curl http://localhost:8080/health
```

```json
{ "status": "ok" }
```

---

## Rate Limiting

The `/api/*` routes are protected by a per-IP Redis sliding-window rate limiter.

Default: **60 requests per minute per IP**.

Configure via `.env`:

```env
RATE_LIMIT_MAX=60
RATE_LIMIT_WINDOW=1m
```

When a client exceeds the limit they receive:

```
HTTP/1.1 429 Too Many Requests
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 0
```

```json
{ "error": "rate limit exceeded — try again later" }
```

---

## Running Tests

```bash
# Run all tests
make test

# Run only package-level tests (fast, no external deps needed)
make test-short
```

The `pkg/base62` package has full round-trip test coverage and can be run without any running infrastructure.

---

## Building for Production

### Build static binaries

```bash
# API server
make build          # outputs bin/server

# Kafka consumer
make build-consumer # outputs bin/consumer

# Both at once
make build-all
```

Both binaries are compiled with `CGO_ENABLED=0` — fully static, no libc dependency.

### Build the Docker image

```bash
docker build -f deploy/Dockerfile -t url-shortener:latest .
```

The final image is based on `distroless/static-debian12` — no shell, no package manager, ~10 MB total.

### Run the Docker image

```bash
docker run --rm \
  --env-file .env \
  -p 8080:8080 \
  url-shortener:latest
```

---

## Environment Variables Reference

All configuration is done via environment variables. The full reference with every variable, its default, and explanation is in `.env.example`.

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP server port |
| `READ_TIMEOUT` | `5s` | Max time to read a full request |
| `WRITE_TIMEOUT` | `10s` | Max time to write a response |
| `BASE_URL` | `http://localhost:8080` | Public base URL for building short links |
| `API_KEY` | — | Secret key for write endpoints (required) |
| `NODE_ID` | `0` | Snowflake node ID — must be unique per pod (0–1023) |
| `DB_SHARDS` | `postgres://...` | Comma-separated PostgreSQL DSNs, one per shard |
| `REDIS_ADDRS` | `localhost:6379` | Comma-separated Redis cluster node addresses |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_TOPIC` | `url.clicks` | Kafka topic for click events |
| `ANALYTICS_DB_DSN` | `postgres://...` | DSN for the analytics database (consumer only) |
| `LRU_MAX_ITEMS` | `50000` | Max entries in the in-process LRU cache per pod |
| `RATE_LIMIT_MAX` | `60` | Max API requests per IP per window |
| `RATE_LIMIT_WINDOW` | `1m` | Rate-limit window duration |
| `LOG_LEVEL` | `info` | Log verbosity: debug, info, warn, error |

---

## Makefile Commands

```bash
make dev              # Run the API server (go run ./cmd/server)
make dev-consumer     # Run the Kafka consumer (go run ./cmd/consumer)
make build            # Compile bin/server
make build-consumer   # Compile bin/consumer
make build-all        # Compile both binaries
make test             # Run all tests with -v
make test-short       # Run tests without external dependencies
make migrate          # Apply SQL migration (requires SHARD_DSN=...)
make lint             # Run golangci-lint
make tidy             # go mod tidy
make docker-up        # Start all infrastructure containers
make docker-down      # Stop all infrastructure containers
make docker-logs      # Tail container logs
```

---

## How Base62 Encoding Works

Every new short URL is generated through this pipeline:

```
1. POST /api/shorten  →  long URL received
2. Snowflake node generates a unique int64  →  e.g. 1829473726482
3. Base62 encode the int64:
       charset: 0-9 A-Z a-z  (62 characters)
       1829473726482  →  "aK9mX2p"
4. Write (short_code="aK9mX2p", long_url=...) to shard
       shard = FNV32("aK9mX2p") % number_of_shards
5. Populate Redis and LRU
6. Return { "short_url": "https://sho.rt/aK9mX2p" }
```

**Why no collision checking?** The Snowflake ID is guaranteed unique by construction (timestamp + machine ID + sequence counter). Base62 is a deterministic bijection — a unique input always produces a unique output. No `SELECT` before `INSERT` is ever needed.

**Capacity:** A 7-character Base62 string supports `62^7 = 3,521,614,606,208` (~3.5 trillion) unique codes. At 10 million new links per day that is 962 years of runway.

---

## Dependency Graph

The strict one-way dependency rule — no layer imports anything above it:

```
pkg/base62          (zero deps)
     ↑
internal/config     (viper)
internal/logger     (zap)
internal/idgen      (snowflake, config)
internal/cache      (go-redis, ristretto)
internal/store      (pgx)
internal/events     (franz-go, zap)
     ↑
internal/service    (cache, store, idgen, events, base62)
     ↑
internal/middleware (go-redis, fiber)
internal/handler    (service, events, fiber, zap)
internal/router     (handler, middleware, fiber)
     ↑
cmd/server/main.go  (everything above, wired together)
cmd/consumer/main.go (events, pgx, franz-go, logger)
```

This structure means every layer is independently testable by mocking the interface one level below it.

---

## License

MIT