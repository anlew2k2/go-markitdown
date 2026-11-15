package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/go-markitdown/internal/handler"
	"github.com/go-markitdown/internal/pipeline"
	"github.com/go-markitdown/internal/services"
)

// defaultJobTimeout bounds one download+convert+upload job in worker mode.
const defaultJobTimeout = 5 * time.Minute

// markdownStderrLimit caps how much of a conversion backend's stderr is
// retained for error reporting (CLI subprocess, or each persistent worker
// process).
const markdownStderrLimit = 64 * 1024

// converterStartupTimeout bounds how long one persistent-backend worker
// process gets to import MarkItDown and report ready before it is
// considered failed. Fixed, not configurable, per the implementation plan.
const converterStartupTimeout = 30 * time.Second

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "healthcheck" || os.Args[1] == "-health") {
		resp, err := http.Get("http://127.0.0.1:8080/ping")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	cfg, err := pipeline.LoadConfig(defaultJobTimeout)
	if err != nil {
		log.Fatalf("extractor: config: %v", err)
	}

	if cfg.ServiceMode == pipeline.ServiceModeWorker {
		runWorker(cfg)
		return
	}
	runHTTP()
}

func runHTTP() {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	v1 := router.Group("/api/v1")
	v1.POST("/subprocess/run", handler.RunSubProcess)
	v1.POST("/subprocess/markitdown/run", handler.RunMarkitdown)

	router.Run() // listens on 0.0.0.0:8080 by default
}

func runWorker(cfg pipeline.Config) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := pipeline.NewRedisClient(cfg)
	defer rdb.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
	err := rdb.Ping(pingCtx).Err()
	cancelPing()
	if err != nil {
		log.Fatalf("extractor: redis ping: %v", err)
	}

	if _, err := services.EnsureBucket(ctx); err != nil {
		log.Fatalf("extractor: ensure bucket: %v", err)
	}

	if err := pipeline.Bootstrap(ctx, rdb); err != nil {
		log.Fatalf("extractor: bootstrap: %v", err)
	}

	srv := pipeline.StartHealthServer(":8080")
	defer pipeline.StopHealthServer(srv)

	// workerCtx is separate from ctx (the process's own shutdown signal) so
	// the persistent converter backend can trigger the exact same
	// shutdown/recovery semantics as SIGTERM (stop consuming, leave the
	// in-flight message pending, never mark it FAILED) purely by calling
	// cancelWorker, if it ever becomes unrecoverable.
	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()

	converter, closeConverter, err := buildConverter(workerCtx, cfg, cancelWorker)
	if err != nil {
		log.Fatalf("extractor: converter backend %q: %v", cfg.ConverterBackend, err)
	}
	defer closeConverter()

	worker := pipeline.NewExtractorWorker(rdb, cfg.ConsumerName, cfg.JobTimeout, cfg.ConvertTimeout, cfg.WorkerConcurrency, converter)
	log.Printf("extractor: worker mode started, consumer=%s concurrency=%d converter_backend=%s job_timeout=%s convert_timeout=%s", cfg.ConsumerName, cfg.WorkerConcurrency, cfg.ConverterBackend, cfg.JobTimeout, cfg.ConvertTimeout)
	if err := worker.Run(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("extractor: worker stopped: %v", err)
	}
	log.Printf("extractor: shutdown complete")
}

// buildConverter constructs the conversion backend selected by
// cfg.ConverterBackend. For "persistent" it spawns and ready-checks
// cfg.WorkerConcurrency worker processes up front — any failure here stops
// startup entirely (log.Fatalf in the caller), it never falls back to the
// CLI backend silently. onUnhealthy is wired to stop this process's own
// worker context if a worker dies later and can't be replaced.
func buildConverter(ctx context.Context, cfg pipeline.Config, onUnhealthy func()) (services.Converter, func(), error) {
	switch cfg.ConverterBackend {
	case pipeline.ConverterBackendPersistent:
		pc, err := services.NewPersistentConverter(ctx, cfg.WorkerConcurrency, cfg.ConverterWorkerScript, markdownStderrLimit, converterStartupTimeout, onUnhealthy)
		if err != nil {
			return nil, nil, err
		}
		return pc, pc.Close, nil
	default:
		return services.CLIConverter{StderrLimit: markdownStderrLimit}, func() {}, nil
	}
}
