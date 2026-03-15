package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

func TestReap_RequeuesStaleJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")
	s.ClaimJob() // attempt 1 of 3, status = running

	s.SetLastHeartbeat(id, time.Now().Add(-60*time.Second))

	reap(s)

	job, _ := s.GetJob(id)
	if job.Status != store.StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
}

func TestReap_FailsJobWithExhaustedRetries(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("bad command")

	// Exhaust all 3 retries
	for i := 0; i < 3; i++ {
		s.ClaimJob()
		if i < 2 {
			s.UpdateJobStatus(id, store.StatusPending)
		}
	}

	s.SetLastHeartbeat(id, time.Now().Add(-60*time.Second))

	reap(s)

	job, _ := s.GetJob(id)
	if job.Status != store.StatusFailed {
		t.Errorf("expected status failed, got %s", job.Status)
	}
}

func TestReap_IgnoresHealthyJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")
	s.ClaimJob()
	s.UpdateHeartbeat(id) // fresh heartbeat

	reap(s)

	job, _ := s.GetJob(id)
	if job.Status != store.StatusRunning {
		t.Errorf("expected status running, got %s", job.Status)
	}
}

func TestReap_IgnoresPendingAndDoneJobs(t *testing.T) {
	s := store.NewMemoryStore()
	id1 := s.AddJob("echo one")
	id2 := s.AddJob("echo two")

	// Claim both, then mark both as done
	s.ClaimJob()
	s.ClaimJob()
	s.UpdateJobStatus(id1, store.StatusDone)
	s.UpdateJobStatus(id2, store.StatusDone)

	reap(s)

	// Neither should be affected, ListRunningJobs returns nothing
	for _, id := range []string{id1, id2} {
		job, _ := s.GetJob(id)
		if job.Status != store.StatusDone {
			t.Errorf("job %s: expected status donw, got %s", id, job.Status)
		}
	}
}

func TestReap_RequeuesJobWithNilHeartbeat(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")
	s.ClaimJob() // running, no heartbeat, but StartedAt is now

	// With a fresh StartedAt, reaper should leave it alone
	reap(s)

	job, _ := s.GetJob(id)
	if job.Status != store.StatusRunning {
		t.Errorf("expected status running, got %s", job.Status)
	}
}

func TestStartReaper_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := store.NewMemoryStore()

	done := make(chan struct{})
	go func() {
		StartReaper(ctx, s)
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
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")
	s.ClaimJob()

	// Simulate a job that was claimed 60 seconds ago but never sent a heartbeat
	s.SetStartedAt(id, time.Now().Add(-60*time.Second))

	reap(s)

	job, _ := s.GetJob(id)
	if job.Status != store.StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
}
