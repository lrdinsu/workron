package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/lrdinsu/workron/internal/scheduler"
	"github.com/lrdinsu/workron/internal/store"
	"github.com/lrdinsu/workron/internal/worker"
)

func main() {
	// Parse CLI flags
	port := flag.Int("port", 8080, "port to listen on")
	numWorkers := flag.Int("workers", 3, "number of concurrent workers")
	flag.Parse()

	// Initialize store and server
	s := store.NewMemoryStore()
	srv := scheduler.NewServer(s)

	// Context for graceful shutdown, canceled when OS signal is received
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker pool
	var wg sync.WaitGroup
	for i := 1; i <= *numWorkers; i++ {
		wg.Add(1)
		w := worker.NewWorker(i, s)
		go func() {
			defer wg.Done()
			w.Start(ctx)
		}()
	}
	log.Printf("[main] started %d workers", *numWorkers)

	// Start HTTP server in a goroutine so it doesn't block signal handling
	addr := fmt.Sprintf(":%d", *port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: srv,
	}

	go func() {
		log.Printf("[main] scheduler listening on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[main] server error: %v", err)
			os.Exit(1)
		}
	}()

	// Block until SIGINT or SIGTERM is received
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[main] shutdown signal received")

	// Stop accepting new HTTP requests
	if err := httpServer.Shutdown(context.Background()); err != nil {
		log.Printf("[main] http server shutdown error: %v", err)
	}

	// Cancel context to signal all workers to stop
	cancel()

	// Wait for all workers to finish their current job
	wg.Wait()
	log.Println("[main] all workers stopped, goodbye")
}
