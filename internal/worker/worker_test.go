package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

// waitForStatus polls the store until the job reaches the expected status or the timeout is exceeded.
// This is needed because the worker runs in a separate goroutine, and we need to wait for it to finish.
func waitForStatus(t *testing.T, s *store.MemoryStore, id string, expected store.JobStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, found := s.GetJob(context.Background(), id)
		if found && job.Status == expected {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	job, found := s.GetJob(context.Background(), id)
	if !found {
		t.Errorf("job %s: not found in store after %v", id, timeout)
		return
	}
	t.Errorf("job %s: expected status %q, got %q after %v", id, expected, job.Status, timeout)
}

func TestWorker_ProcessesJobSuccessfully(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s, slog.Default())
	go w.Start(ctx)

	waitForStatus(t, s, id, store.StatusDone, 3*time.Second)
}

func TestWorker_MarksFailedJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "thiscommanddoesnotexist"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s, slog.Default())
	go w.Start(ctx)

	// Job should eventually be permanently failed after all retries exhausted
	waitForStatus(t, s, id, store.StatusFailed, 5*time.Second)
}

func TestWorker_RetriesFailedJobBeforeGivingUp(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "thiscommanddoesnotexist"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s, slog.Default())
	go w.Start(ctx)

	waitForStatus(t, s, id, store.StatusFailed, 5*time.Second)

	// Verify it was attempted exactly MaxRetries times, not just once
	job, found := s.GetJob(context.Background(), id)
	if !found {
		t.Fatal("job not found after completion")
	}
	if job.Attempts != job.MaxRetries {
		t.Errorf("expected %d attempts, got %d", job.MaxRetries, job.Attempts)
	}

}

func TestWorker_DoesNotRetryJobThatSucceeds(t *testing.T) {
	s := store.NewMemoryStore()
	// This command should succeed on the first attempt, so the worker
	// should not requeue or retry it.
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s, slog.Default())
	go w.Start(ctx)

	waitForStatus(t, s, id, store.StatusDone, 3*time.Second)

	// A successful job should only have been attempted once
	job, found := s.GetJob(context.Background(), id)
	if !found {
		t.Fatal("job not found after completion")
	}
	if job.Attempts != 1 {
		t.Errorf("expected 1 attempt for successful job, go %d", job.Attempts)
	}

}

func TestWorker_StopsOnContextCancel(t *testing.T) {
	s := store.NewMemoryStore()

	ctx, cancel := context.WithCancel(context.Background())
	w := NewWorker(1, s, slog.Default())

	done := make(chan struct{})
	go func() {
		w.Start(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// worker exited cleanly
	case <-time.After(2 * time.Second):
		t.Error("worker did not stop after context was canceled")
	}

}

func TestWorker_MultipleWorkerNoDuplicates(t *testing.T) {
	s := store.NewMemoryStore()

	// Submit 5 jobs
	ids := make([]string, 5)
	for i := range ids {
		ids[i] = s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start 3 workers competing for 5 jobs
	for i := 1; i <= 3; i++ {
		w := NewWorker(i, s, slog.Default())
		go w.Start(ctx)
	}

	// Wait for all jobs to complete
	for _, id := range ids {
		waitForStatus(t, s, id, store.StatusDone, 5*time.Second)
	}

	// Verify every job is done exactly once, none stuck in running or pending
	for _, id := range ids {
		job, found := s.GetJob(context.Background(), id)
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

func TestWorker_SendsHeartbeatsDuringLongJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "sleep 12"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s, slog.Default())
	go w.Start(ctx)

	// Wait long enough for at least 2 heartbeats (an interval is 5s)
	time.Sleep(12 * time.Second)

	job, found := s.GetJob(context.Background(), id)
	if !found {
		t.Fatal("job not found")
	}

	if job.LastHeartbeat == nil {
		t.Fatal("expected last_heartbeat to be set during long-running job")
	}

	// Heartbeat should have been updated recently (within the last 6 seconds)
	elapsed := time.Since(*job.LastHeartbeat)
	if elapsed > 6*time.Second {
		t.Errorf("last heartbeat was %v ago, expected within 6s", elapsed)
	}
}

func TestWorker_HeartbeatStopsAfterJobCompletes(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"}) // finishes instantly

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := NewWorker(1, s, slog.Default())
	go w.Start(ctx)

	waitForStatus(t, s, id, store.StatusDone, 3*time.Second)

	// Record the heartbeat timestamp (if any) right after completion
	job, _ := s.GetJob(context.Background(), id)
	heartbeatAfterDone := job.LastHeartbeat

	// Wait longer than one heartbeat interval
	time.Sleep(6 * time.Second)

	// Heartbeat should not have advanced, the goroutine should be stopped
	job, _ = s.GetJob(context.Background(), id)
	if job.LastHeartbeat != nil && heartbeatAfterDone != nil && job.LastHeartbeat.After(*heartbeatAfterDone) {
		t.Error("heartbeat continued after job completed, possible goroutine leak")
	}
}

// fakeDemoSource implements the JobSource, preemptReporter, and
// checkpointSaver interfaces just well enough to drive a single demo
// job through preempt + checkpoint emission.
type fakeDemoSource struct {
	mu sync.Mutex

	job *store.Job

	heartbeatCalls    int
	preemptAfterCalls int

	preemptedReported bool
	preemptedEpoch    int

	savedCheckpoint []byte
	savedEpoch      int
}

func (f *fakeDemoSource) ClaimJob(ctx context.Context) (*store.Job, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.job == nil {
		return nil, false
	}
	out := *f.job
	f.job = nil
	return &out, true
}

func (f *fakeDemoSource) UpdateJobStatus(ctx context.Context, id string, status store.JobStatus) {}

func (f *fakeDemoSource) SendHeartbeat(ctx context.Context, id string) (store.HeartbeatResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeatCalls++
	if f.heartbeatCalls >= f.preemptAfterCalls {
		return store.HeartbeatResult{Action: "preempt", PreemptionEpoch: 7}, nil
	}
	return store.HeartbeatResult{}, nil
}

func (f *fakeDemoSource) ReportPreempted(ctx context.Context, id string, epoch int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.preemptedReported = true
	f.preemptedEpoch = epoch
	return nil
}

func (f *fakeDemoSource) SaveCheckpoint(ctx context.Context, id string, epoch int, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.savedCheckpoint = append([]byte(nil), data...)
	f.savedEpoch = epoch
	return nil
}

func TestWorker_DemoJobEmitsCheckpointOnPreempt(t *testing.T) {
	src := &fakeDemoSource{
		job: &store.Job{
			ID:      "job-demo-1",
			Command: "demo:sleep 10",
			Status:  store.StatusRunning,
		},
		preemptAfterCalls: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	w := NewWorker(1, src, slog.Default())
	w.process(ctx, &store.Job{
		ID:      "job-demo-1",
		Command: "demo:sleep 10",
		Status:  store.StatusRunning,
	})

	src.mu.Lock()
	defer src.mu.Unlock()

	if !src.preemptedReported {
		t.Fatal("expected ReportPreempted to be called")
	}
	if src.preemptedEpoch != 7 {
		t.Errorf("preempted epoch = %d, want 7", src.preemptedEpoch)
	}
	if len(src.savedCheckpoint) == 0 {
		t.Fatal("expected SaveCheckpoint to be called with non-empty payload")
	}
	if src.savedEpoch != 7 {
		t.Errorf("checkpoint epoch = %d, want 7", src.savedEpoch)
	}

	var payload map[string]any
	if err := json.Unmarshal(src.savedCheckpoint, &payload); err != nil {
		t.Fatalf("checkpoint payload not valid JSON: %v", err)
	}
	if payload["task_id"] != "job-demo-1" {
		t.Errorf("payload task_id = %v, want job-demo-1", payload["task_id"])
	}
	if payload["demo"] != true {
		t.Errorf("payload demo = %v, want true", payload["demo"])
	}
}

func TestBuildDemoCheckpoint_IsValidJSON(t *testing.T) {
	out := buildDemoCheckpoint("job-x", 3, 1500*time.Millisecond)

	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("not valid JSON: %v\npayload: %s", err, out)
	}
	if got["task_id"] != "job-x" {
		t.Errorf("task_id = %v, want job-x", got["task_id"])
	}
	if got["epoch"] != float64(3) {
		t.Errorf("epoch = %v, want 3", got["epoch"])
	}
	if got["elapsed_ms"] != float64(1500) {
		t.Errorf("elapsed_ms = %v, want 1500", got["elapsed_ms"])
	}
}
