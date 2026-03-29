package scheduler

import (
	"context"
	"log/slog"
	"time"

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
func StartReaper(ctx context.Context, s store.JobStore, logger *slog.Logger) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	logger.Info("reaper started")

	for {
		select {
		case <-ctx.Done():
			logger.Info("reaper shutting down")
			return
		case <-ticker.C:
			reap(ctx, s, logger)
			s.UnblockReady(ctx)
		}
	}
}

// reap scans running jobs and re-queues or fails any with stale heartbeats.
func reap(ctx context.Context, s store.JobStore, logger *slog.Logger) {
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
		} else {
			logger.Error("stale heartbeat, marking failed", "job_id", job.ID, "attempt", job.Attempts)
			s.UpdateJobStatus(ctx, job.ID, store.StatusFailed)
		}
	}
}
