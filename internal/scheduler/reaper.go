package scheduler

import (
	"context"
	"log"
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
func StartReaper(ctx context.Context, s store.JobStore) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()

	log.Println("[reaper] started")

	for {
		select {
		case <-ctx.Done():
			log.Println("[reaper] shutting down")
			return
		case <-ticker.C:
			reap(ctx, s)
			s.UnblockReady(ctx)
		}
	}
}

// reap scans running jobs and re-queues or fails any with stale heartbeats.
func reap(ctx context.Context, s store.JobStore) {
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
			log.Printf("[reaper] job %s has stale heartbeat, re-queuing (attempt %d/%d)", job.ID, job.Attempts, job.MaxRetries)
			s.UpdateJobStatus(ctx, job.ID, store.StatusPending)
		} else {
			log.Printf("[reaper] job %s has stale heartbeat, marking failed after %d attempts", job.ID, job.Attempts)
			s.UpdateJobStatus(ctx, job.ID, store.StatusFailed)
		}
	}
}
