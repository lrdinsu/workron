package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/lrdinsu/workron/internal/metrics"
	"github.com/lrdinsu/workron/internal/store"
)

const (
	reapInterval     = 10 * time.Second
	heartbeatTimeout = 30 * time.Second
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
func reap(ctx context.Context, s store.JobStore, logger *slog.Logger, m *metrics.Metrics) {
	now := time.Now()
	for _, job := range s.ListRunningJobs(ctx) {
		// Use heartbeat if available, otherwise fall back to when the job was claimed
		lastContact := job.LastHeartbeat
		if lastContact == nil {
			lastContact = job.StartedAt
		}

		if lastContact != nil && now.Sub(*lastContact) <= heartbeatTimeout {
			continue // healthy heartbeat, skip
		}

		// No heartbeat at all, or heartbeat is stale: the worker is assumed dead
		if job.Attempts < job.MaxRetries {
			logger.Warn("stale heartbeat, re-queuing", "job_id", job.ID, "attempt", job.Attempts, "max_retries", job.MaxRetries)
			s.UpdateJobStatus(ctx, job.ID, store.StatusPending)
			m.JobsReaped.WithLabelValues("requeued").Inc()
		} else {
			logger.Error("stale heartbeat, marking failed", "job_id", job.ID, "attempt", job.Attempts)
			s.UpdateJobStatus(ctx, job.ID, store.StatusFailed)
			m.JobsReaped.WithLabelValues("failed").Inc()
		}
	}
}
