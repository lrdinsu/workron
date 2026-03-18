package store

import (
	"sync"
	"testing"
	"time"
)

// StoreFactory creates a fresh, empty JobStore for each test.
// Each implementation provides its own factory in its test file.
type StoreFactory func(t *testing.T) JobStore

// --- Compliance tests: AddJob + GetJob ---

func testAddJobReturnsID(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id := s.AddJob("echo hello")

	if id == "" {
		t.Fatal("AddJob returned empty ID")
	}
}

func testAddJobUniqueIDs(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id1 := s.AddJob("echo a")
	id2 := s.AddJob("echo b")

	if id1 == id2 {
		t.Errorf("AddJob returned duplicate IDs: %s", id1)
	}
}

func testGetJobRoundTrip(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	before := time.Now()

	id := s.AddJob("echo hello")
	job, found := s.GetJob(id)

	if !found {
		t.Fatal("expected to find job")
	}
	if job.ID != id {
		t.Errorf("ID = %q, want %q", job.ID, id)
	}
	if job.Command != "echo hello" {
		t.Errorf("Command = %q, want %q", job.Command, "echo hello")
	}
	if job.Status != StatusPending {
		t.Errorf("Status = %q, want %q", job.Status, StatusPending)
	}
	if job.CreatedAt.Before(before) {
		t.Errorf("CreatedAt %v is before test start %v", job.CreatedAt, before)
	}
	if job.StartedAt != nil {
		t.Errorf("StartedAt = %v, want nil", job.StartedAt)
	}
	if job.DoneAt != nil {
		t.Errorf("DoneAt = %v, want nil", job.DoneAt)
	}
	if job.LastHeartbeat != nil {
		t.Errorf("LastHeartbeat = %v, want nil", job.LastHeartbeat)
	}
	if job.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", job.MaxRetries)
	}
	if job.Attempts != 0 {
		t.Errorf("Attempts = %d, want 0", job.Attempts)
	}
}

func testGetJobNotFound(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	job, found := s.GetJob("nonexistent")

	if found {
		t.Errorf("expected not found, got %+v", job)
	}
	if job != nil {
		t.Errorf("expected nil job, got %+v", job)
	}
}

func testGetJobReturnsACopy(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id := s.AddJob("echo hello")

	job1, _ := s.GetJob(id)
	job2, _ := s.GetJob(id)

	// Mutating one should not affect the other.
	job1.Command = "mutated"

	if job2.Command == "mutated" {
		t.Error("GetJob returned a shared reference instead of a copy")
	}
}

// --- Compliance tests: ClaimJob ---

func testClaimJobReturnsJob(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id := s.AddJob("echo hello")
	before := time.Now()

	job, ok := s.ClaimJob()

	if !ok {
		t.Fatal("expected to claim a job")
	}
	if job.ID != id {
		t.Errorf("ID = %q, want %q", job.ID, id)
	}
	if job.Status != StatusRunning {
		t.Errorf("Status = %q, want %q", job.Status, StatusRunning)
	}
	if job.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", job.Attempts)
	}
	if job.StartedAt == nil || job.StartedAt.Before(before) {
		t.Errorf("StartedAt not set correctly")
	}
	if job.LastHeartbeat != nil {
		t.Errorf("LastHeartbeat = %v, want nil after fresh claim", job.LastHeartbeat)
	}
}

func testClaimJobEmptyStore(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	job, ok := s.ClaimJob()

	if ok {
		t.Errorf("expected no job, got %+v", job)
	}
	if job != nil {
		t.Errorf("expected nil, got %+v", job)
	}
}

func testClaimJobSkipsNonPending(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	// Add one job and claim it, now it's running.
	s.AddJob("echo hello")
	s.ClaimJob()

	// Second claim should find nothing.
	job, ok := s.ClaimJob()

	if ok {
		t.Errorf("expected no job, got %+v", job)
	}
}
func testClaimJobNoDuplicates(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	// Add 5 jobs.
	for i := 0; i < 5; i++ {
		s.AddJob("echo hello")
	}

	// Claim all 5 from concurrent goroutines.
	claimed := make(chan string, 10)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if job, ok := s.ClaimJob(); ok {
				claimed <- job.ID
			}
		}()
	}
	wg.Wait()
	close(claimed)

	// Verify exactly 5 unique claims.
	seen := make(map[string]bool)
	for id := range claimed {
		if seen[id] {
			t.Errorf("job %s was claimed more than once", id)
		}
		seen[id] = true
	}
	if len(seen) != 5 {
		t.Errorf("claimed %d jobs, want 5", len(seen))
	}
}

// --- Compliance tests: UpdateJobStatus ---

func testUpdateJobStatusDone(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id := s.AddJob("echo hello")
	s.ClaimJob()
	before := time.Now()

	s.UpdateJobStatus(id, StatusDone)

	job, _ := s.GetJob(id)
	if job.Status != StatusDone {
		t.Errorf("Status = %q, want %q", job.Status, StatusDone)
	}
	if job.DoneAt == nil || job.DoneAt.Before(before) {
		t.Error("DoneAt not set correctly")
	}
}

func testUpdateJobStatusFailed(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id := s.AddJob("echo hello")
	s.ClaimJob()
	before := time.Now()

	s.UpdateJobStatus(id, StatusFailed)

	job, _ := s.GetJob(id)
	if job.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", job.Status, StatusFailed)
	}
	if job.DoneAt == nil || job.DoneAt.Before(before) {
		t.Error("DoneAt not set correctly for failed status")
	}
}

func testUpdateJobStatusRequeue(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id := s.AddJob("echo hello")
	s.ClaimJob()

	// Re-queue: set back to pending (retry scenario).
	s.UpdateJobStatus(id, StatusPending)

	job, _ := s.GetJob(id)
	if job.Status != StatusPending {
		t.Errorf("Status = %q, want %q", job.Status, StatusPending)
	}
	if job.DoneAt != nil {
		t.Errorf("DoneAt = %v, want nil for re-queued job", job.DoneAt)
	}
}

// --- Compliance tests: ListJobs ---

func testListJobsEmpty(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	jobs := s.ListJobs()

	if jobs == nil {
		t.Fatal("ListJobs returned nil, want empty slice")
	}
	if len(jobs) != 0 {
		t.Errorf("len = %d, want 0", len(jobs))
	}
}

func testListJobsReturnsAll(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	s.AddJob("echo a")
	s.AddJob("echo b")
	s.AddJob("echo c")

	jobs := s.ListJobs()

	if len(jobs) != 3 {
		t.Errorf("len = %d, want 3", len(jobs))
	}
}

// --- Compliance tests: ListRunningJobs ---

func testListRunningJobsEmpty(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	s.AddJob("echo pending")

	jobs := s.ListRunningJobs()

	if len(jobs) != 0 {
		t.Errorf("len = %d, want 0 (no running jobs)", len(jobs))
	}
}

func testListRunningJobsFilters(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	s.AddJob("echo a")
	s.AddJob("echo b")
	s.AddJob("echo c")
	// Claim 2, they become running.
	s.ClaimJob()
	s.ClaimJob()

	jobs := s.ListRunningJobs()

	if len(jobs) != 2 {
		t.Errorf("len = %d, want 2", len(jobs))
	}
	for _, j := range jobs {
		if j.Status != StatusRunning {
			t.Errorf("got non-running job in ListRunningJobs: %+v", j)
		}
	}
}

// --- Compliance tests: UpdateHeartbeat ---

func testUpdateHeartbeat(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)

	id := s.AddJob("echo hello")
	s.ClaimJob()
	before := time.Now()

	s.UpdateHeartbeat(id)

	job, _ := s.GetJob(id)
	if job.LastHeartbeat == nil {
		t.Fatal("LastHeartbeat is nil after UpdateHeartbeat")
	}
	if job.LastHeartbeat.Before(before) {
		t.Errorf("LastHeartbeat %v is before call time %v", job.LastHeartbeat, before)
	}
}
