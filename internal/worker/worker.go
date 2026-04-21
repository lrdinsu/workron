package worker

import (
	"context"
	"log/slog"
	"sync"
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
	SendHeartbeat(ctx context.Context, id string) (store.HeartbeatResult, error)
}

// preemptReporter is an optional interface satisfied by JobSource implementations
// that can tell the scheduler a job has finished draining after a preempt signal.
// SchedulerClient satisfies it; direct-store sources (MemoryStore, SQLiteStore, PostgresStore)
// do not, because the in-process path does not use gang preemption signaling.
type preemptReporter interface {
	ReportPreempted(ctx context.Context, id string, epoch int) error
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
//
// For gang preemption: the heartbeat goroutine notices action="preempt"
// on the response, records the epoch, and closes a stop channel that
// the executor watches. The executor sends SIGTERM to its child and
// escalates to SIGKILL after a grace window. Once the executor returns,
// if a preempt was observed, the worker reports /preempted with the
// epoch instead of /done or /fail.
func (w *Worker) process(ctx context.Context, job *store.Job) {
	w.logger.Info("job picked up", "job_id", job.ID, "attempt", job.Attempts, "max_retries", job.MaxRetries, "command", job.Command)

	// preemptCh is closed once (guarded by sync.Once) when the heartbeat
	// goroutine first observes action="preempt". The executor watches it
	// to know when to send SIGTERM. Repeated preempt heartbeats are safe.
	preemptCh := make(chan struct{})
	var preemptOnce sync.Once
	var preemptMu sync.Mutex
	var preemptEpoch int

	recordPreempt := func(epoch int) {
		preemptMu.Lock()
		if preemptEpoch == 0 {
			preemptEpoch = epoch
		}
		preemptMu.Unlock()
		preemptOnce.Do(func() { close(preemptCh) })
	}

	// Heartbeat goroutine, canceled when the job finishes.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go w.sendHeartbeats(hbCtx, job.ID, recordPreempt)

	err := w.executor.Execute(ctx, job.Command, job.Env, preemptCh)

	// If the heartbeat loop ever flagged preemption, take the preempt
	// reporting path instead of treating the executor's non-nil error
	// as a normal failure (the error is "signal: terminated" or similar).
	preempted := false
	select {
	case <-preemptCh:
		preempted = true
	default:
	}

	if preempted {
		preemptMu.Lock()
		epoch := preemptEpoch
		preemptMu.Unlock()
		if rp, ok := w.source.(preemptReporter); ok {
			if rerr := rp.ReportPreempted(ctx, job.ID, epoch); rerr != nil {
				w.logger.Warn("report preempted failed", "job_id", job.ID, "preemption_epoch", epoch, "error", rerr)
				return
			}
			w.logger.Info("job preempted", "job_id", job.ID, "preemption_epoch", epoch)
			return
		}
		// In-process sources don't implement preempt reporting; fall through
		// to the normal failure-report path so the job isn't left running.
		w.logger.Warn("preempt observed but source does not implement ReportPreempted", "job_id", job.ID)
	}

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

// sendHeartbeats periodically pings the source to signal the worker is
// still alive and inspects the response for scheduler actions. When an
// action="preempt" heartbeat arrives, onPreempt is called with the
// scheduler-supplied epoch; the callback is safe to invoke repeatedly
// (the caller guards against double-triggering).
func (w *Worker) sendHeartbeats(ctx context.Context, jobID string, onPreempt func(epoch int)) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res, err := w.source.SendHeartbeat(ctx, jobID)
			if err != nil {
				w.logger.Warn("heartbeat failed", "job_id", jobID, "error", err)
				continue
			}
			if res.Action == "preempt" {
				onPreempt(res.PreemptionEpoch)
			}
		}
	}
}
