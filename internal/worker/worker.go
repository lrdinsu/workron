package worker

import (
	"context"
	"log"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

const pollInterval = 1 * time.Second

// JobSource is the minimal interface a Worker needs to fetch and report on jobs.
// Both store.JobStore (in-process) and SchedulerClient (over HTTP) satisfy this.
type JobSource interface {
	ClaimJob() (*store.Job, bool)
	UpdateJobStatus(id string, status store.JobStatus)
}

// Worker polls a JobSource and executes jobs
type Worker struct {
	id       int
	source   JobSource
	executor *Executor
}

// NewWorker creates a new Worker with the given ID and job source.
func NewWorker(id int, source JobSource) *Worker {
	return &Worker{
		id:       id,
		source:   source,
		executor: NewExecutor(),
	}
}

// Start begins the worker's polling loop.
// It blocks until the context is canceled, at which point it finishes any in-progress job and returns.
func (w *Worker) Start(ctx context.Context) {
	log.Printf("[worker-%d] started", w.id)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		// Check for shutdown before doing any work
		select {
		case <-ctx.Done():
			// Context was canceled, scheduler is shutting down
			log.Printf("[worker-%d] shutting down", w.id)
			return
		default:
		}

		job, found := w.source.ClaimJob()
		if found {
			w.process(job)
			continue
		}

		// No pending jobs, wait before polling again
		select {
		case <-ctx.Done():
			log.Printf("[worker-%d] shutting down", w.id)
			return
		case <-ticker.C:
		}

	}
}

// process executes a single job and updates its status via the source.
// If running in-process (store.JobStore), the worker handles retries directly.
// If running over HTTP (SchedulerClient), the scheduler handles retry decisions.
func (w *Worker) process(job *store.Job) {
	log.Printf("[worker-%d] picking up job %s (attempt %d/%d): %q", w.id, job.ID, job.Attempts, job.MaxRetries, job.Command)

	err := w.executor.Execute(job.Command)
	if err != nil {
		if job.Attempts < job.MaxRetries {
			log.Printf("[worker-%d] job %s failed, retrying (attempt %d/%d): %v", w.id, job.ID, job.Attempts, job.MaxRetries, err)
			w.source.UpdateJobStatus(job.ID, store.StatusPending)
			return
		}

		log.Printf("[worker-%d] job %s failed permanently after %d attempts: %v", w.id, job.ID, job.Attempts, err)
		w.source.UpdateJobStatus(job.ID, store.StatusFailed)
		return
	}

	log.Printf("[worker-%d] job %s done", w.id, job.ID)
	w.source.UpdateJobStatus(job.ID, store.StatusDone)
}
