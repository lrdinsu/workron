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
	mode := flag.String("mode", "scheduler", "scheduler (HTTP API only) | standalone (API + local workers)")
	port := flag.Int("port", 8080, "port to listen on")
	numWorkers := flag.Int("workers", 3, "number of concurrent workers (standalone mode only)")
	dbDriver := flag.String("db-driver", "memory", "storage backend: memory, sqlite, postgres")
	dbURL := flag.String("db-url", "", "database connection string (SQLite path or PostgreSQL URL)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var s store.JobStore
	switch *dbDriver {
	case "postgres":
		if *dbURL == "" {
			log.Fatal("[main] --db-url is required for postgres driver")
		}
		pgStore, err := store.NewPostgresStore(ctx, *dbURL, 10)
		if err != nil {
			log.Fatalf("[main] failed to connect to PostgreSQL: %v", err)
		}
		defer pgStore.Close()
		s = pgStore
		log.Printf("[main] using PostgreSQL store")
	case "sqlite":
		if *dbURL == "" {
			log.Fatal("[main] --db-url is required for sqlite driver")
		}
		sqliteStore, err := store.NewSQLiteStore(*dbURL)
		if err != nil {
			log.Fatalf("[main] failed to open SQLite database: %v", err)
		}
		defer func() { _ = sqliteStore.Close() }()
		s = sqliteStore
		log.Printf("[main] using SQLite store: %s", *dbURL)
	default:
		s = store.NewMemoryStore()
		log.Println("[main] using in-memory store")
	}

	// Initialize the server
	srv := scheduler.NewServer(s)

	// Start the heartbeat reaper to detect dead workers
	go scheduler.StartReaper(ctx, s)

	// Only start local workers in standalone mode
	var wg sync.WaitGroup
	if *mode == "standalone" {
		for i := 1; i <= *numWorkers; i++ {
			wg.Add(1)
			w := worker.NewWorker(i, s)
			go func() {
				defer wg.Done()
				w.Start(ctx)
			}()
		}
		log.Printf("[main] standalone mode: started %d local workers", *numWorkers)
	} else {
		log.Println("[main] scheduler mode: waiting for remote workers")
	}

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
