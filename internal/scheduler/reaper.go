package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/lrdinsu/workron/internal/metrics"
	"github.com/lrdinsu/workron/internal/store"
)

const (
	reapInterval       = 10 * time.Second
	heartbeatTimeout   = 30 * time.Second
	workerStaleTimeout = 60 * time.Second
)

// StartReaper runs a background goroutine that detects dead workers.
// It checks all running jobs for stale or missing heartbeats and either
// re-queues them (if retries remain) or marks them permanently failed.
// It also calls UnblockReady on each tick as a safety net for DAG
// dependencies that may not have been unblocked through the HTTP handler.
func StartReaper(ctx context.Context, s store.JobStore, logger *slog.Logger, m *metrics.Metrics) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	logger.Info("reaper started")

	for {
		select {
		case <-ctx.Done():
			logger.Info("reaper shutting down")
			return
		case <-ticker.C:
			runReaperTick(ctx, s, logger, m)
		}
	}
}

// runReaperTick executes one reaper cycle. If the store implements ReaperLocker
// (e.g. PostgresStore with advisory locks), only the lock holder runs the reap.
// Otherwise (MemoryStore, SQLiteStore), the reap runs unconditionally.
func runReaperTick(ctx context.Context, s store.JobStore, logger *slog.Logger, m *metrics.Metrics) {
	doReap := func(ctx context.Context) {
		reap(ctx, s, logger, m)
		s.UnblockReady(ctx)

		// Mark workers with stale heartbeats as offline.
		if ws, ok := s.(store.WorkerStore); ok {
			if n := ws.RemoveStaleWorkers(ctx, workerStaleTimeout); n > 0 {
				logger.Info("marked stale workers offline", "count", n)
			}
		}
	}

	locker, ok := s.(store.ReaperLocker)
	if !ok {
		// No coordination needed (single-process mode)
		// this instance is always the reaper leader.
		m.ReaperLeader.Set(1)
		doReap(ctx)
		return
	}

	acquired, err := locker.WithReaperLock(ctx, doReap)
	if err != nil {
		logger.Error("reaper lock error", "error", err)
		return
	}
	if acquired {
		m.ReaperLeader.Set(1)
	} else {
		m.ReaperLeader.Set(0)
		logger.Debug("reaper tick skipped, another instance holds the lock")
	}
}

// reap scans running jobs and re-queues or fails any with stale heartbeats.
// For gang tasks, entering coordinated drain via PreemptGang takes precedence
// over the legacy re-queue/fail path: if any sibling is still running, the
// whole gang goes through preempt/complete instead of leaving live peers behind.
func reap(ctx context.Context, s store.JobStore, logger *slog.Logger, m *metrics.Metrics) {
	now := time.Now()
	gs, _ := s.(store.GangStore)

	for _, job := range s.ListRunningJobs(ctx) {
		// Use heartbeat if available, otherwise fall back to when the job was claimed
		lastContact := job.LastHeartbeat
		if lastContact == nil {
			lastContact = job.StartedAt
		}

		if lastContact != nil && now.Sub(*lastContact) <= heartbeatTimeout {
			continue // healthy heartbeat, skip
		}

		// Gang preemption path: before flipping this task's status, try to
		// enter coordinated drain. If any sibling is still running, the store
		// moves all running tasks (this one included) to preempting and we
		// skip the legacy status flip + FailGang.
		if job.GangID != "" && gs != nil {
			res, err := gs.PreemptGang(ctx, job.GangID, job.ID)
			if err != nil {
				logger.Error("reaper preempt gang failed", "gang_id", job.GangID, "job_id", job.ID, "error", err)
				// Fall through so the dead task doesn't stay running forever.
			} else if res.Entered {
				logger.Info("reaper started gang drain",
					"gang_id", job.GangID,
					"trigger_job_id", job.ID,
					"preemption_epoch", res.Epoch,
					"transitioned", res.Transitioned,
				)
				m.JobsReaped.WithLabelValues("requeued").Inc()
				continue
			}
		}

		// No heartbeat at all, or heartbeat is stale: the worker is assumed dead
		retry := job.Attempts < job.MaxRetries
		if retry {
			logger.Warn("stale heartbeat, re-queuing", "job_id", job.ID, "attempt", job.Attempts, "max_retries", job.MaxRetries)
			s.UpdateJobStatus(ctx, job.ID, store.StatusPending)
			m.JobsReaped.WithLabelValues("requeued").Inc()
		} else {
			logger.Error("stale heartbeat, marking failed", "job_id", job.ID, "attempt", job.Attempts)
			s.UpdateJobStatus(ctx, job.ID, store.StatusFailed)
			m.JobsReaped.WithLabelValues("failed").Inc()
		}

		// Legacy gang failure path: only reached when PreemptGang returned
		// Entered=false (no sibling was running).
		if job.GangID != "" && gs != nil {
			logger.Info("reaper triggering gang failure", "gang_id", job.GangID, "job_id", job.ID)
			if err := gs.FailGang(ctx, job.GangID, retry); err != nil {
				logger.Error("reaper failed to propagate gang failure", "gang_id", job.GangID, "error", err)
			}
		}
	}
}
