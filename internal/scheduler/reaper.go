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
	// preemptTimeout bounds how long a task may sit in preempting
	// waiting for its worker's /preempted acknowledgement before the
	// reaper force-drains it. Roughly the SIGTERM grace window (15s)
	// plus slack (30s) for network and checkpoint upload.
	preemptTimeout = 45 * time.Second
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
		completeDrainedPreemptions(ctx, s, logger, m)
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
				m.GangsPreempted.Inc()
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

// completeDrainedPreemptions finishes any gang drains that are ready.
// Two passes, ordered:
//
//  1. Force-timeout: tasks stuck in preempting past preemptTimeout are
//     moved to preempted as if the worker had acked. This covers dead
//     or unreachable workers that will never send /preempted, and it
//     must run before the completion pass — otherwise a single stuck
//     task would block its gang's completion indefinitely.
//  2. Completion: for each gang that has at least one task in
//     preempting/preempted and no task still in running/preempting,
//     CompletePreemption is called. The store decides whether the gang
//     goes to blocked (re-admissible) or failed (retries exhausted or
//     partial completion encountered).
//
// No-op when the backing store does not implement GangStore.
func completeDrainedPreemptions(ctx context.Context, s store.JobStore, logger *slog.Logger, m *metrics.Metrics) {
	gs, ok := s.(store.GangStore)
	if !ok {
		return
	}

	now := time.Now()

	// Pass 1: force-timeout stale preempting tasks.
	for _, job := range s.ListJobs(ctx) {
		if job.Status != store.StatusPreempting {
			continue
		}
		if job.DrainStartedAt == nil {
			// Defensive: a preempting row without a stamp shouldn't happen via
			// PreemptGang, but skip to avoid an indeterminate timeout calculation.
			continue
		}
		age := now.Sub(*job.DrainStartedAt)
		if age <= preemptTimeout {
			continue
		}
		if err := gs.ForceDrainPreempting(ctx, job.ID); err != nil {
			logger.Error("force drain failed",
				"job_id", job.ID,
				"gang_id", job.GangID,
				"preemption_epoch", job.PreemptionEpoch,
				"error", err,
			)
			continue
		}
		m.GangPreemptionsForceDrained.Inc()
		logger.Warn("force-drained stuck preempting task",
			"job_id", job.ID,
			"gang_id", job.GangID,
			"preemption_epoch", job.PreemptionEpoch,
			"drain_age", age.Round(time.Second),
		)
	}

	// Pass 2: complete drained gangs.
	// Re-list because pass 1 may have moved tasks from preempting to
	// preempted, changing completion eligibility. Also capture a representative
	// drain-start timestamp for the completion histogram. CompletePreemption
	// clears DrainStartedAt, so we must read it before calling it.
	type gangState struct {
		hasDrained  bool      // any task preempting or preempted
		hasInFlight bool      // any task running or preempting
		epoch       int       // representative drain-round epoch for logging
		drainStart  time.Time // earliest DrainStartedAt across drained tasks
	}
	gangs := make(map[string]*gangState)
	for _, job := range s.ListJobs(ctx) {
		if job.GangID == "" {
			continue
		}
		st, ok := gangs[job.GangID]
		if !ok {
			st = &gangState{}
			gangs[job.GangID] = st
		}
		switch job.Status {
		case store.StatusPreempting:
			st.hasInFlight = true
			st.hasDrained = true
			if job.PreemptionEpoch > st.epoch {
				st.epoch = job.PreemptionEpoch
			}
			if job.DrainStartedAt != nil && (st.drainStart.IsZero() || job.DrainStartedAt.Before(st.drainStart)) {
				st.drainStart = *job.DrainStartedAt
			}
		case store.StatusPreempted:
			st.hasDrained = true
			if job.PreemptionEpoch > st.epoch {
				st.epoch = job.PreemptionEpoch
			}
			if job.DrainStartedAt != nil && (st.drainStart.IsZero() || job.DrainStartedAt.Before(st.drainStart)) {
				st.drainStart = *job.DrainStartedAt
			}
		case store.StatusRunning:
			st.hasInFlight = true
		}
	}

	for gangID, st := range gangs {
		if !st.hasDrained || st.hasInFlight {
			continue
		}
		completed, err := gs.CompletePreemption(ctx, gangID)
		if err != nil {
			logger.Error("complete preemption failed",
				"gang_id", gangID,
				"preemption_epoch", st.epoch,
				"error", err,
			)
			continue
		}
		if !completed {
			// Another reaper instance or a concurrent writer raced;
			// try again next tick.
			continue
		}
		// Re-read the gang to report the verdict.
		outcome := "blocked"
		for _, t := range gs.ListGangTasks(ctx, gangID) {
			if t.Status == store.StatusFailed {
				outcome = "failed"
				break
			}
		}
		m.GangPreemptionsCompleted.WithLabelValues(outcome).Inc()
		if !st.drainStart.IsZero() {
			m.GangPreemptionDrainTime.Observe(now.Sub(st.drainStart).Seconds())
		}
		logger.Info("gang drain completed",
			"gang_id", gangID,
			"preemption_epoch", st.epoch,
			"outcome", outcome,
		)
	}
}
