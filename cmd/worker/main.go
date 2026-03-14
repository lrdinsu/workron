package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/lrdinsu/workron/internal/worker"
)

func main() {
	// Parse CLI flags
	schedulerURL := flag.String("scheduler", "http://localhost:8080", "scheduler base URL")
	numWorkers := flag.Int("workers", 3, "number of concurrent worker goroutines")
	flag.Parse()

	// Create the HTTP client that talks to the scheduler
	client := worker.NewSchedulerClient(*schedulerURL)

	// Context for graceful shutdown, canceled when OS signal is received
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker pool
	var wg sync.WaitGroup
	for i := 1; i <= *numWorkers; i++ {
		wg.Add(1)
		w := worker.NewWorker(i, client)
		go func() {
			defer wg.Done()
			w.Start(ctx)
		}()
	}
	log.Printf("[main] started %d workers, scheduler=%s", *numWorkers, *schedulerURL)

	// Block until SIGINT or SIGTERM is received
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[main] shutdown signal received")

	// Cancel context to signal all workers to stop
	cancel()

	// Wait for all workers to finish their current job
	wg.Wait()
	log.Println("[main] all workers stopped, goodbye")
}
