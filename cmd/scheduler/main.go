package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
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
	// Configure structured JSON logging as the global default.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

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
			slog.Error("--db-url is required for postgres driver")
			os.Exit(1)
		}
		pgStore, err := store.NewPostgresStore(ctx, *dbURL, 10)
		if err != nil {
			slog.Error("failed to connect to PostgreSQL", "error", err)
			os.Exit(1)
		}
		defer pgStore.Close()
		s = pgStore
		slog.Info("using PostgreSQL store")
	case "sqlite":
		if *dbURL == "" {
			slog.Error("--db-url is required for sqlite driver")
			os.Exit(1)
		}
		sqliteStore, err := store.NewSQLiteStore(*dbURL)
		if err != nil {
			slog.Error("failed to open SQLite database", "error", err)
			os.Exit(1)
		}
		defer func() { _ = sqliteStore.Close() }()
		s = sqliteStore
		slog.Info("using SQLite store", "db_url", *dbURL)
	default:
		s = store.NewMemoryStore()
		slog.Info("using in-memory store")
	}

	// Initialize the server
	srv := scheduler.NewServer(s, slog.Default())

	// Start the heartbeat reaper to detect dead workers
	go scheduler.StartReaper(ctx, s, slog.Default())

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
		slog.Info("started local workers", "mode", "standalone", "count", *numWorkers)
	} else {
		slog.Info("waiting for remote workers", "mode", "scheduler")
	}

	// Start HTTP server in a goroutine so it doesn't block signal handling
	addr := fmt.Sprintf(":%d", *port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: srv,
	}

	go func() {
		slog.Info("scheduler listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Block until SIGINT or SIGTERM is received
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutdown signal received")

	// Stop accepting new HTTP requests
	if err := httpServer.Shutdown(context.Background()); err != nil {
		slog.Error("http server shutdown error", "error", err)
	}

	// Cancel context to signal all workers to stop
	cancel()

	// Wait for all workers to finish their current job
	wg.Wait()
	slog.Info("all workers stopped, goodbye")
}
