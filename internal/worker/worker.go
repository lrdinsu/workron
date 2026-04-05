package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

const pollInterval = 1 * time.Second
const heartbeatInterval = 5 * time.Second

// JobSource is the minimal interface a Worker needs to fetch and report on jobs.
// Both store.JobStore (in-process) and SchedulerClient (over HTTP) satisfy this.
type JobSource interface {
	ClaimJob(ctx context.Context) (*store.Job, bool)
	UpdateJobStatus(ctx context.Context, id string, status store.JobStatus)
	SendHeartbeat(ctx context.Context, id string) error
}

// Worker polls a JobSource and executes jobs
type Worker struct {
	id       int
	source   JobSource
	executor *Executor
	logger   *slog.Logger
}

// NewWorker creates a new Worker with the given ID and job source.
// The logger is decorated with worker_id so every log line identifies this worker.
func NewWorker(id int, source JobSource, logger *slog.Logger) *Worker {
	return &Worker{
		id:       id,
		source:   source,
		executor: NewExecutor(),
		logger:   logger.With("worker_id", id),
	}
}

// Start begins the worker's polling loop.
// It blocks until the context is canceled, at which point it finishes any in-progress job and returns.
func (w *Worker) Start(ctx context.Context) {
	w.logger.Info("worker started")

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		// Check for shutdown before doing any work
		select {
		case <-ctx.Done():
			// Context was canceled, scheduler is shutting down
			w.logger.Info("worker shutting down")
			return
		default:
		}

		job, found := w.source.ClaimJob(ctx)
		if found {
			w.process(ctx, job)
			continue
		}

		// No pending jobs, wait for the next tick or context cancellation
		select {
		case <-ctx.Done():
			w.logger.Info("worker shutting down")
			return
		case <-ticker.C:
		}

	}
}

// process executes a single job and updates its status via the source.
// If running in-process (store.JobStore), the worker handles retries directly.
// If running over HTTP (SchedulerClient), the scheduler handles retry decisions.
func (w *Worker) process(ctx context.Context, job *store.Job) {
	w.logger.Info("job picked up", "job_id", job.ID, "attempt", job.Attempts, "max_retries", job.MaxRetries, "command", job.Command)

	// Start a heartbeat goroutine, canceled when the job finishes
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.sendHeartbeats(hbCtx, job.ID)

	err := w.executor.Execute(ctx, job.Command, nil)
	if err != nil {
		if job.Attempts < job.MaxRetries {
			w.logger.Warn("job failed, retrying", "job_id", job.ID, "attempt", job.Attempts, "max_retries", job.MaxRetries, "error", err)
			w.source.UpdateJobStatus(ctx, job.ID, store.StatusPending)
			return
		}

		w.logger.Error("job failed permanently", "job_id", job.ID, "attempt", job.Attempts, "error", err)
		w.source.UpdateJobStatus(ctx, job.ID, store.StatusFailed)
		return
	}

	w.logger.Info("job done", "job_id", job.ID)
	w.source.UpdateJobStatus(ctx, job.ID, store.StatusDone)
}

// sendHeartbeats periodically ping the source to signal the worker is still alive.
// It stops when the context is canceled (i.e., the job finishes).
func (w *Worker) sendHeartbeats(ctx context.Context, jobID string) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.source.SendHeartbeat(ctx, jobID); err != nil {
				w.logger.Warn("heartbeat failed", "job_id", jobID, "error", err)
			}
		}
	}
}
