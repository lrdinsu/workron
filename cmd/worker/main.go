package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/lrdinsu/workron/internal/store"
	"github.com/lrdinsu/workron/internal/worker"
)

// workerHeartbeatInterval must be well below the scheduler's workerStaleTimeout(60s)
// so transient network hiccups do not flip the worker to offline.
const workerHeartbeatInterval = 15 * time.Second

func main() {
	// Configure structured JSON logging as the global default.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Parse CLI flags
	schedulerURL := flag.String("scheduler", "http://localhost:8080", "scheduler base URL")
	numWorkers := flag.Int("workers", 3, "number of concurrent worker goroutines")
	workerID := flag.String("worker-id", "", "unique worker ID (auto-generated if empty)")
	execAddr := flag.String("exec-addr", "", "host:port other workers can reach for gang rendezvous (leave empty for non-gang usage)")
	vramMB := flag.Int("vram-mb", 0, "VRAM this worker advertises (MB); jobs requesting vram_mb>0 skip workers at 0")
	memoryMB := flag.Int("memory-mb", 0, "system memory this worker advertises (MB)")
	flag.Parse()

	// Generate worker ID if not provided
	if *workerID == "" {
		*workerID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}

	// Create the HTTP client that talks to the scheduler
	client := worker.NewSchedulerClient(*schedulerURL, *workerID, slog.Default())

	// Context for graceful shutdown, canceled when OS signal is received
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register with the scheduler so gang admission can see this worker.
	if err := client.RegisterWorker(ctx, store.Worker{
		ID:       *workerID,
		ExecAddr: *execAddr,
		Resources: store.ResourceSpec{
			VRAMMB:   *vramMB,
			MemoryMB: *memoryMB,
		},
	}); err != nil {
		slog.Error("failed to register worker, exiting", "worker_id", *workerID, "error", err)
		os.Exit(1)
	}
	slog.Info("worker registered", "worker_id", *workerID, "exec_addr", *execAddr, "vram_mb", *vramMB, "memory_mb", *memoryMB)

	// Start worker-level heartbeat goroutine so the scheduler's RemoveStaleWorkers
	// pass does not flip this worker to offline while it is idle between jobs.
	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		ticker := time.NewTicker(workerHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := client.SendWorkerHeartbeat(ctx); err != nil {
					slog.Warn("worker heartbeat failed", "worker_id", *workerID, "error", err)
				}
			}
		}
	}()

	// Start worker pool
	var wg sync.WaitGroup
	for i := 1; i <= *numWorkers; i++ {
		wg.Add(1)
		w := worker.NewWorker(i, client, slog.Default())
		go func() {
			defer wg.Done()
			w.Start(ctx)
		}()
	}
	slog.Info("started workers", "count", *numWorkers, "scheduler", *schedulerURL)

	// Block until SIGINT or SIGTERM is received
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutdown signal received")

	// Cancel context to signal all workers and the heartbeat goroutine to stop
	cancel()

	// Wait for all workers to finish their current job
	wg.Wait()
	hbWG.Wait()
	slog.Info("all workers stopped, goodbye")
}
