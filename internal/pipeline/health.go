package pipeline

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"
)

// StartHealthServer starts a minimal HTTP server exposing GET /ping on addr.
// Worker mode otherwise exposes no HTTP port, but container orchestration
// still needs something to probe.
func StartHealthServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"message":"pong"}`))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("pipeline: health server error: %v", err)
		}
	}()
	return srv
}

// StopHealthServer gracefully shuts down srv with a short deadline.
func StopHealthServer(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
