package worker

import (
	"context"
	"log"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

const pollInterval = 1 * time.Second

// Worker polls the job store and executes jobs
type Worker struct {
	id       int
	store    store.JobStore
	executor *Executor
}

// NewWorker creates a new Worker with the given ID and store
func NewWorker(id int, s store.JobStore) *Worker {
	return &Worker{
		id:       id,
		store:    s,
		executor: NewExecutor(),
	}
}

// Start begins the worker's polling loop.
// It blocks until the context is canceled, at which point it finishes any in-progress job and returns.
func (w *Worker) Start(ctx context.Context) {
	log.Printf("[worker-%d] started", w.id)

	for {
		select {
		case <-ctx.Done():
			// Context was canceled, scheduler is shutting down
			log.Printf("[worker-%d] shutting down", w.id)
			return
		default:
			job, found := w.store.ClaimJob()
			if !found {
				// No pending jobs, wait before polling again
				time.Sleep(pollInterval)
				continue
			}

			w.process(job)
		}
	}
}

// process executes a single job and updates its status in the store.
// If the job fails and has remaining retries, it is re-queued as pending.
// If retries are exhausted, it is marked as failed permanently.
func (w *Worker) process(job *store.Job) {
	log.Printf("[worker-%d] picking up job %s (attempt %d/%d: %q", w.id, job.ID, job.Attempts, job.MaxRetries, job.Command)

	err := w.executor.Execute(job.Command)
	if err != nil {
		if job.Attempts < job.MaxRetries {
			log.Printf("[worker-%d] job %s failed, retrying (attempt %d/%d): %v", w.id, job.ID, job.Attempts, job.MaxRetries, err)
			w.store.UpdateJobStatus(job.ID, store.StatusPending)
			return
		}

		log.Printf("[worker-%d] job %s failed permanently after %d attempts: %v", w.id, job.ID, job.Attempts, err)
		w.store.UpdateJobStatus(job.ID, store.StatusFailed)
		return
	}

	log.Printf("[worker-%d] job %s done", w.id, job.ID)
	w.store.UpdateJobStatus(job.ID, store.StatusDone)
}
