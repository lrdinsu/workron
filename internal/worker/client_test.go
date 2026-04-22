package worker

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lrdinsu/workron/internal/metrics"
	"github.com/lrdinsu/workron/internal/scheduler"
	"github.com/lrdinsu/workron/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// newTestScheduler starts a httptest server backed by a real scheduler + memory store.
// Returns the client, the underlying store (for assertions), and a cleanup function.
func newTestScheduler(t *testing.T) (*SchedulerClient, *store.MemoryStore, func()) {
	t.Helper()
	s := store.NewMemoryStore()
	srv := scheduler.NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	ts := httptest.NewServer(srv)
	client := NewSchedulerClient(ts.URL, "test-worker", slog.Default())
	return client, s, ts.Close
}

func TestSchedulerClient_ClaimJob(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	// No jobs available
	job, found := client.ClaimJob(ctx)
	if found {
		t.Fatal("expected no job, but got one")
	}
	if job != nil {
		t.Fatal("expected nil job")
	}

	// Add a job and claim it
	id := s.AddJob(ctx, store.AddJobParams{Command: "echo hello"})
	job, found = client.ClaimJob(ctx)
	if !found {
		t.Fatal("expected to claim a job")
	}
	if job.ID != id {
		t.Errorf("expected job ID %s, got %s", id, job.ID)
	}
	if job.Status != store.StatusRunning {
		t.Errorf("expected status running, got %s", job.Status)
	}
}

func TestSchedulerClient_ReportDone(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob(ctx, store.AddJobParams{Command: "echo hello"})
	s.ClaimJob(ctx) // move to running

	err := client.ReportDone(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusDone {
		t.Errorf("expected status done, got %s", job.Status)
	}
}

func TestSchedulerClient_ReportFail_Retries(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	s.AddJob(ctx, store.AddJobParams{Command: "bad command"})
	s.ClaimJob(ctx) // attempt 1 of 3

	job, _ := s.GetJob(ctx, s.ListJobs(ctx)[0].ID)
	err := client.ReportFail(ctx, job.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Scheduler should re-queue since attempts(1) < maxRetries(3)
	updated, _ := s.GetJob(ctx, job.ID)
	if updated.Status != store.StatusPending {
		t.Errorf("expected status pending (retry), got %s", updated.Status)
	}
}

func TestSchedulerClient_ReportFail_Permanent(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob(ctx, store.AddJobParams{Command: "bad command"})

	// Exhaust all 3 retries
	for i := 0; i < 3; i++ {
		s.ClaimJob(ctx)
		if i < 2 {
			s.UpdateJobStatus(ctx, id, store.StatusPending)
		}
	}

	err := client.ReportFail(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusFailed {
		t.Errorf("expected status failed, got %s", job.Status)
	}
}

func TestSchedulerClient_UpdateJobStatus(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob(ctx, store.AddJobParams{Command: "echo hello"})
	s.ClaimJob(ctx) // move to running

	// UpdateJobStatus with StatusDone should call ReportDone
	client.UpdateJobStatus(ctx, id, store.StatusDone)

	job, _ := s.GetJob(ctx, id)
	if job.Status != store.StatusDone {
		t.Errorf("expected status done, got %s", job.Status)
	}
}

func TestSchedulerClient_SendHeartbeat(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob(ctx, store.AddJobParams{Command: "echo hello"})
	s.ClaimJob(ctx) // move to running

	res, err := client.SendHeartbeat(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Action != "" {
		t.Errorf("expected empty action, got %q", res.Action)
	}
	if res.PreemptionEpoch != 0 {
		t.Errorf("expected preemption_epoch 0, got %d", res.PreemptionEpoch)
	}

	job, _ := s.GetJob(ctx, id)
	if job.LastHeartbeat == nil {
		t.Error("expected last_heartbeat to be set after heartbeat")
	}
}

func TestSchedulerClient_RegisterWorker(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	w := store.Worker{
		ID:        "test-worker",
		ExecAddr:  "127.0.0.1:9000",
		Resources: store.ResourceSpec{VRAMMB: 16000, MemoryMB: 32000},
	}
	if err := client.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, found := s.GetWorker(ctx, "test-worker")
	if !found {
		t.Fatal("worker was not registered in the store")
	}
	if got.ExecAddr != "127.0.0.1:9000" {
		t.Errorf("exec_addr = %q, want 127.0.0.1:9000", got.ExecAddr)
	}
	if got.Resources.VRAMMB != 16000 {
		t.Errorf("vram_mb = %d, want 16000", got.Resources.VRAMMB)
	}
	if got.Status != store.WorkerActive {
		t.Errorf("status = %q, want active", got.Status)
	}
}

func TestSchedulerClient_SendWorkerHeartbeat(t *testing.T) {
	ctx := context.Background()
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	// Register first, then backdate heartbeat to a known past time.
	if err := client.RegisterWorker(ctx, store.Worker{ID: "test-worker", ExecAddr: "h:1"}); err != nil {
		t.Fatalf("register: %v", err)
	}
	pastTime := time.Now().Add(-1 * time.Hour)
	s.SetWorkerHeartbeat("test-worker", pastTime)

	if err := client.SendWorkerHeartbeat(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := s.GetWorker(ctx, "test-worker")
	if !got.LastHeartbeat.After(pastTime) {
		t.Errorf("last_heartbeat was not refreshed: got %v, want after %v", got.LastHeartbeat, pastTime)
	}
}

func TestSchedulerClient_SendWorkerHeartbeat_UnknownWorker(t *testing.T) {
	ctx := context.Background()
	client, _, cleanup := newTestScheduler(t)
	defer cleanup()

	// Never registered → scheduler returns 404.
	err := client.SendWorkerHeartbeat(ctx)
	if err == nil {
		t.Fatal("expected error for unregistered worker, got nil")
	}
}
