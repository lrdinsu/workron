package worker

import (
	"net/http/httptest"
	"testing"

	"github.com/lrdinsu/workron/internal/scheduler"
	"github.com/lrdinsu/workron/internal/store"
)

// newTestScheduler starts a httptest server backed by a real scheduler + memory store.
// Returns the client, the underlying store (for assertions), and a cleanup function.
func newTestScheduler(t *testing.T) (*SchedulerClient, *store.MemoryStore, func()) {
	t.Helper()
	s := store.NewMemoryStore()
	srv := scheduler.NewServer(s)
	ts := httptest.NewServer(srv)
	client := NewSchedulerClient(ts.URL)
	return client, s, ts.Close
}

func TestSchedulerClient_ClaimJob(t *testing.T) {
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	// No jobs available
	job, found := client.ClaimJob()
	if found {
		t.Fatal("expected no job, but got one")
	}
	if job != nil {
		t.Fatal("expected nil job")
	}

	// Add a job and claim it
	id := s.AddJob("echo hello")
	job, found = client.ClaimJob()
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
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob("echo hello")
	s.ClaimJob() // move to running

	err := client.ReportDone(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	job, _ := s.GetJob(id)
	if job.Status != store.StatusDone {
		t.Errorf("expected status done, got %s", job.Status)
	}
}

func TestSchedulerClient_ReportFail_Retries(t *testing.T) {
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	s.AddJob("bad command")
	s.ClaimJob() // attempt 1 of 3

	job, _ := s.GetJob(s.ListJobs()[0].ID)
	err := client.ReportFail(job.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Scheduler should re-queue since attempts(1) < maxRetries(3)
	updated, _ := s.GetJob(job.ID)
	if updated.Status != store.StatusPending {
		t.Errorf("expected status pending (retry), got %s", updated.Status)
	}
}

func TestSchedulerClient_ReportFail_Permanent(t *testing.T) {
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob("bad command")

	// Exhaust all 3 retries
	for i := 0; i < 3; i++ {
		s.ClaimJob()
		if i < 2 {
			s.UpdateJobStatus(id, store.StatusPending)
		}
	}

	err := client.ReportFail(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	job, _ := s.GetJob(id)
	if job.Status != store.StatusFailed {
		t.Errorf("expected status failed, got %s", job.Status)
	}
}

func TestSchedulerClient_UpdateJobStatus(t *testing.T) {
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob("echo hello")
	s.ClaimJob() // move to running

	// UpdateJobStatus with StatusDone should call ReportDone
	client.UpdateJobStatus(id, store.StatusDone)

	job, _ := s.GetJob(id)
	if job.Status != store.StatusDone {
		t.Errorf("expected status done, got %s", job.Status)
	}
}

func TestSchedulerClient_SendHeartbeat(t *testing.T) {
	client, s, cleanup := newTestScheduler(t)
	defer cleanup()

	id := s.AddJob("echo hello")
	s.ClaimJob() // move to running

	err := client.SendHeartbeat(id)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	job, _ := s.GetJob(id)
	if job.LastHeartbeat == nil {
		t.Error("expected last_heartbeat to be set after heartbeat")
	}
}
