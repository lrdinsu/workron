package worker

import (
	"context"
	"testing"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

// waitForStatus polls the store until the job reaches the expected status or the timeout is exceeded.
// This is needed because the worker runs in a separate goroutine and we need to wait for it to finish.
func waitForStatus(t *testing.T, s *store.MemoryStore, id string, expected store.JobStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, found := s.GetJob(id)
		if found && job.Status == expected {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	job, _ := s.GetJob(id)
	t.Errorf("job %s: expected status %q, got %q after %v", id, expected, job.Status, timeout)
}

func TestWorker_ProcessesJobSuccessfully(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s)
	go w.Start(ctx)

	waitForStatus(t, s, id, store.StatusDone, 3*time.Second)
}

func TestWorker_MarksFailedJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("thiscommanddoesnotexist")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s)
	go w.Start(ctx)

	waitForStatus(t, s, id, store.StatusFailed, 3*time.Second)
}

func TestWorker_StopsOnContextCancel(t *testing.T) {
	s := store.NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	w := NewWorker(1, s)

	done := make(chan struct{})
	go func() {
		w.Start(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		//worker exited cleanly
	case <-time.After(2 * time.Second):
		t.Error("worker did not stop after context was canceled")
	}

}

func TestWorker_MultipleWorkerNoDuplicates(t *testing.T) {
	s := store.NewMemoryStore()

	// Submit 5 jobs
	ids := make([]string, 5)
	for i := range ids {
		ids[i] = s.AddJob("echo hello")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start 3 workers competing for 5 jobs
	for i := 1; i <= 3; i++ {
		w := NewWorker(i, s)
		go w.Start(ctx)
	}

	// Wait for all jobs to complete
	for _, id := range ids {
		waitForStatus(t, s, id, store.StatusDone, 5*time.Second)
	}

	// Verify every job is done exactly once, none stuck in running or pending
	for _, id := range ids {
		job, found := s.GetJob(id)
		if !found {
			t.Errorf("job %s not found in store", id)
			continue
		}

		if job.Status != store.StatusDone {
			t.Errorf("job %s: expected done, got %s", id, job.Status)
		}

		if job.Attempts != 1 {
			t.Errorf("job %s: expected 1 attempt, got %d, possible double-claim", id, job.Attempts)
		}
	}

}
