package main

import (
	"context"
	"errors"
	"log"
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

func main() {
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

	worker := pipeline.NewExtractorWorker(rdb, cfg.ConsumerName, cfg.JobTimeout, cfg.ConvertTimeout)
	log.Printf("extractor: worker mode started, consumer=%s job_timeout=%s convert_timeout=%s", cfg.ConsumerName, cfg.JobTimeout, cfg.ConvertTimeout)
	if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("extractor: worker stopped: %v", err)
	}
	log.Printf("extractor: shutdown complete")
}
