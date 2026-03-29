package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

func TestReap_RequeuesStaleJob(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx) // attempt 1 of 3, status = running

	s.SetLastHeartbeat(id, time.Now().Add(-60*time.Second))

	reap(ctx, s, slog.Default())

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
}

func TestReap_FailsJobWithExhaustedRetries(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	id := s.AddJob(ctx, "bad command", nil)

	// Exhaust all 3 retries
	for i := 0; i < 3; i++ {
		s.ClaimJob(ctx)
		if i < 2 {
			s.UpdateJobStatus(ctx, id, store.StatusPending)
		}
	}

	s.SetLastHeartbeat(id, time.Now().Add(-60*time.Second))

	reap(ctx, s, slog.Default())

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusFailed {
		t.Errorf("expected status failed, got %s", job.Status)
	}
}

func TestReap_IgnoresHealthyJob(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)
	s.UpdateHeartbeat(ctx, id) // fresh heartbeat

	reap(ctx, s, slog.Default())

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusRunning {
		t.Errorf("expected status running, got %s", job.Status)
	}
}

func TestReap_IgnoresPendingAndDoneJobs(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	id1 := s.AddJob(ctx, "echo one", nil)
	id2 := s.AddJob(ctx, "echo two", nil)

	// Claim both, then mark both as done
	s.ClaimJob(ctx)
	s.ClaimJob(ctx)
	s.UpdateJobStatus(ctx, id1, store.StatusDone)
	s.UpdateJobStatus(ctx, id2, store.StatusDone)

	reap(ctx, s, slog.Default())

	// Neither should be affected, ListRunningJobs returns nothing
	for _, id := range []string{id1, id2} {
		job, _ := s.GetJob(ctx, id)
		if job.Status != store.StatusDone {
			t.Errorf("job %s: expected status donw, got %s", id, job.Status)
		}
	}
}

func TestReap_RequeuesJobWithNilHeartbeat(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx) // running, no heartbeat, but StartedAt is now

	// With a fresh StartedAt, reaper should leave it alone
	reap(ctx, s, slog.Default())

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusRunning {
		t.Errorf("expected status running, got %s", job.Status)
	}
}

func TestStartReaper_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := store.NewMemoryStore()

	done := make(chan struct{})
	go func() {
		StartReaper(ctx, s, slog.Default())
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// reaper exited cleanly
	case <-time.After(2 * time.Second):
		t.Error("reaper did not stop after context was canceled")
	}
}

func TestReap_RequeuesJobWithNilHeartbeatAndStaleStart(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()
	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)

	// Simulate a job that was claimed 60 seconds ago but never sent a heartbeat
	s.SetStartedAt(id, time.Now().Add(-60*time.Second))

	reap(ctx, s, slog.Default())

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
}
