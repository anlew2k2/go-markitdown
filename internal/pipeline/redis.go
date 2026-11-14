package pipeline

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Stream and consumer group names, fixed by the event contract.
const (
	StreamInput    = "stream:doc-extractor:input"
	StreamSplitted = "stream:doc-extractor:splitted"
	StreamOutput   = "stream:doc-extractor:output"

	GroupSplitter  = "cg:doc-splitter"
	GroupExtractor = "cg:doc-extractor"
)

// Service modes selected via the SERVICE_MODE environment variable.
const (
	ServiceModeHTTP   = "http"
	ServiceModeWorker = "worker"
)

// DefaultStateTTL is the TTL applied to every per-request Redis key
// (state/completed/parts), per the POC requirement of 24h auto-expiry.
const DefaultStateTTL = 24 * time.Hour

// Config holds the pipeline's runtime configuration, loaded from the
// environment. Everything not listed here (TTL, poll interval, consumer
// block duration, stderr cap, max parts) uses a fixed POC default and is
// intentionally not configurable, per the simplified POC plan.
type Config struct {
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	ConsumerName   string
	ServiceMode    string
	JobTimeout     time.Duration
	ConvertTimeout time.Duration
}

// LoadConfig reads Config from the environment. defaultJobTimeout is
// supplied by the caller since it differs between the Splitter (15m) and
// the Extractor (5m).
func LoadConfig(defaultJobTimeout time.Duration) (Config, error) {
	db, err := strconv.Atoi(envOr("REDIS_DB", "0"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid REDIS_DB: %w", err)
	}

	jobTimeout, err := envDurationOr("JOB_TIMEOUT", defaultJobTimeout)
	if err != nil {
		return Config{}, fmt.Errorf("invalid JOB_TIMEOUT: %w", err)
	}
	convertTimeout, err := envDurationOr("CONVERT_TIMEOUT", 2*time.Minute)
	if err != nil {
		return Config{}, fmt.Errorf("invalid CONVERT_TIMEOUT: %w", err)
	}

	consumer := os.Getenv("CONSUMER_NAME")
	if consumer == "" {
		if host, err := os.Hostname(); err == nil && host != "" {
			consumer = host
		} else {
			consumer = "consumer-" + uuid.NewString()
		}
	}

	return Config{
		RedisAddr:      envOr("REDIS_ADDR", "redis:6379"),
		RedisPassword:  os.Getenv("REDIS_PASSWORD"),
		RedisDB:        db,
		ConsumerName:   consumer,
		ServiceMode:    envOr("SERVICE_MODE", ServiceModeHTTP),
		JobTimeout:     jobTimeout,
		ConvertTimeout: convertTimeout,
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDurationOr(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	return time.ParseDuration(v)
}

// NewRedisClient builds a go-redis client from cfg.
func NewRedisClient(cfg Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}

// Bootstrap ensures both consumer groups and all three streams exist. It is
// idempotent and safe to call on every process startup.
func Bootstrap(ctx context.Context, rdb *redis.Client) error {
	if err := ensureGroup(ctx, rdb, StreamInput, GroupSplitter); err != nil {
		return err
	}
	if err := ensureGroup(ctx, rdb, StreamSplitted, GroupExtractor); err != nil {
		return err
	}
	return ensureStreamExists(ctx, rdb, StreamOutput)
}

func ensureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group %s on %s: %w", group, stream, err)
	}
	return nil
}

// bootstrapStreamScript creates stream KEYS[1] (if absent) by adding and
// immediately deleting a sentinel entry, so downstream readers can rely on
// the output stream existing from the very first startup without ever
// leaving a fake entry behind for a real consumer to see.
var bootstrapStreamScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  local id = redis.call('XADD', KEYS[1], '*', 'bootstrap', '1')
  redis.call('XDEL', KEYS[1], id)
end
return 1
`)

func ensureStreamExists(ctx context.Context, rdb *redis.Client, stream string) error {
	return bootstrapStreamScript.Run(ctx, rdb, []string{stream}).Err()
}
