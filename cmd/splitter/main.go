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

// defaultJobTimeout bounds one download+split+upload job in worker mode.
const defaultJobTimeout = 15 * time.Minute

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
		log.Fatalf("splitter: config: %v", err)
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
	v1.POST("/split-file", handler.SplitFile)

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
		log.Fatalf("splitter: redis ping: %v", err)
	}

	if _, err := services.EnsureBucket(ctx); err != nil {
		log.Fatalf("splitter: ensure bucket: %v", err)
	}

	if err := pipeline.Bootstrap(ctx, rdb); err != nil {
		log.Fatalf("splitter: bootstrap: %v", err)
	}

	srv := pipeline.StartHealthServer(":8080")
	defer pipeline.StopHealthServer(srv)

	worker := pipeline.NewSplitterWorker(rdb, cfg.ConsumerName, cfg.JobTimeout, cfg.WorkerConcurrency)
	log.Printf("splitter: worker mode started, consumer=%s concurrency=%d job_timeout=%s", cfg.ConsumerName, cfg.WorkerConcurrency, cfg.JobTimeout)
	if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("splitter: worker stopped: %v", err)
	}
	log.Printf("splitter: shutdown complete")
}
