package store

import (
	"context"
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
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)

	if id == "" {
		t.Fatal("AddJob returned empty ID")
	}
}

func testAddJobUniqueIDs(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id1 := s.AddJob(ctx, "echo a", nil)
	id2 := s.AddJob(ctx, "echo b", nil)

	if id1 == id2 {
		t.Errorf("AddJob returned duplicate IDs: %s", id1)
	}
}

func testGetJobRoundTrip(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	before := time.Now()

	id := s.AddJob(ctx, "echo hello", nil)
	job, found := s.GetJob(ctx, id)

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
	ctx := context.Background()

	job, found := s.GetJob(ctx, "nonexistent")

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
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)

	job1, _ := s.GetJob(ctx, id)
	job2, _ := s.GetJob(ctx, id)

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
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)
	before := time.Now()

	job, ok := s.ClaimJob(ctx)

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
	ctx := context.Background()

	job, ok := s.ClaimJob(ctx)

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
	ctx := context.Background()

	// Add one job and claim it, now it's running.
	s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)

	// Second claim should find nothing.
	job, ok := s.ClaimJob(ctx)

	if ok {
		t.Errorf("expected no job, got %+v", job)
	}
}

func testClaimJobNoDuplicates(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	// Add 5 jobs.
	for i := 0; i < 5; i++ {
		s.AddJob(ctx, "echo hello", nil)
	}

	// Claim all 5 from concurrent goroutines.
	claimed := make(chan string, 10)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if job, ok := s.ClaimJob(ctx); ok {
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
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)
	before := time.Now()

	s.UpdateJobStatus(ctx, id, StatusDone)

	job, _ := s.GetJob(ctx, id)
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
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)
	before := time.Now()

	s.UpdateJobStatus(ctx, id, StatusFailed)

	job, _ := s.GetJob(ctx, id)
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
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)

	// Re-queue: set back to pending (retry scenario).
	s.UpdateJobStatus(ctx, id, StatusPending)

	job, _ := s.GetJob(ctx, id)
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
	ctx := context.Background()

	jobs := s.ListJobs(ctx)

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
	ctx := context.Background()

	s.AddJob(ctx, "echo a", nil)
	s.AddJob(ctx, "echo b", nil)
	s.AddJob(ctx, "echo c", nil)

	jobs := s.ListJobs(ctx)

	if len(jobs) != 3 {
		t.Errorf("len = %d, want 3", len(jobs))
	}
}

// --- Compliance tests: ListRunningJobs ---

func testListRunningJobsEmpty(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	s.AddJob(ctx, "echo pending", nil)

	jobs := s.ListRunningJobs(ctx)

	if len(jobs) != 0 {
		t.Errorf("len = %d, want 0 (no running jobs)", len(jobs))
	}
}

func testListRunningJobsFilters(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	s.AddJob(ctx, "echo a", nil)
	s.AddJob(ctx, "echo b", nil)
	s.AddJob(ctx, "echo c", nil)
	// Claim 2, they become running.
	s.ClaimJob(ctx)
	s.ClaimJob(ctx)

	jobs := s.ListRunningJobs(ctx)

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
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)
	before := time.Now()

	s.UpdateHeartbeat(ctx, id)

	job, _ := s.GetJob(ctx, id)
	if job.LastHeartbeat == nil {
		t.Fatal("LastHeartbeat is nil after UpdateHeartbeat")
	}
	if job.LastHeartbeat.Before(before) {
		t.Errorf("LastHeartbeat %v is before call time %v", job.LastHeartbeat, before)
	}
}

// --- Compliance tests: DependsOn ---

func testAddJobNoDependencies(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)

	job, _ := s.GetJob(ctx, id)
	if job.Status != StatusPending {
		t.Errorf("Status = %q, want %q for job without dependencies", job.Status, StatusPending)
	}
}

func testAddJobWithDependenciesStartsBlocked(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	depID := s.AddJob(ctx, "echo dep", nil)
	id := s.AddJob(ctx, "echo child", []string{depID})

	job, _ := s.GetJob(ctx, id)
	if job.Status != StatusBlocked {
		t.Errorf("Status = %q, want %q for job with dependencies", job.Status, StatusBlocked)
	}
	if len(job.DependsOn) != 1 || job.DependsOn[0] != depID {
		t.Errorf("DependsOn = %v, want [%s]", job.DependsOn, depID)
	}
}

func testClaimJobSkipsBlocked(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	depID := s.AddJob(ctx, "echo dep", nil)
	s.AddJob(ctx, "echo child", []string{depID})

	// Claim should only return the dependency, not the blocked child.
	job, ok := s.ClaimJob(ctx)
	if !ok {
		t.Fatal("expected to claim a job")
	}
	if job.ID != depID {
		t.Errorf("claimed %q, want %q (the pending job, not the blocked one)", job.ID, depID)
	}

	// The second claim should find nothing, one pending is now running, one is blocked.
	_, ok = s.ClaimJob(ctx)
	if ok {
		t.Error("expected no claimable job, but got one")
	}
}

func testDependsOnRoundTrip(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	dep1 := s.AddJob(ctx, "echo a", nil)
	dep2 := s.AddJob(ctx, "echo b", nil)
	id := s.AddJob(ctx, "echo c", []string{dep1, dep2})

	job, _ := s.GetJob(ctx, id)
	if len(job.DependsOn) != 2 {
		t.Fatalf("DependsOn length = %d, want 2", len(job.DependsOn))
	}

	deps := make(map[string]bool)
	for _, d := range job.DependsOn {
		deps[d] = true
	}
	if !deps[dep1] || !deps[dep2] {
		t.Errorf("DependsOn = %v, want [%s, %s]", job.DependsOn, dep1, dep2)
	}
}

// --- Compliance tests: UnblockReady ---

func testUnblockReadyAfterAllDepsDone(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	depID := s.AddJob(ctx, "echo dep", nil)
	childID := s.AddJob(ctx, "echo child", []string{depID})

	// Child should be blocked.
	child, _ := s.GetJob(ctx, childID)
	if child.Status != StatusBlocked {
		t.Fatalf("expected blocked, got %s", child.Status)
	}

	// Complete the dependency.
	s.ClaimJob(ctx)
	s.UpdateJobStatus(ctx, depID, StatusDone)
	s.UnblockReady(ctx)

	// Child should now be pending.
	child, _ = s.GetJob(ctx, childID)
	if child.Status != StatusPending {
		t.Errorf("Status = %q, want %q after dependency completed", child.Status, StatusPending)
	}
}

func testUnblockReadyPartialDeps(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	dep1 := s.AddJob(ctx, "echo a", nil)
	dep2 := s.AddJob(ctx, "echo b", nil)
	childID := s.AddJob(ctx, "echo child", []string{dep1, dep2})

	// Complete only the first dependency.
	s.ClaimJob(ctx)
	s.UpdateJobStatus(ctx, dep1, StatusDone)
	s.UnblockReady(ctx)

	// Child should stay blocked, dep2 is still pending.
	child, _ := s.GetJob(ctx, childID)
	if child.Status != StatusBlocked {
		t.Errorf("Status = %q, want %q (not all deps done)", child.Status, StatusBlocked)
	}

	// Now complete the second dependency.
	s.ClaimJob(ctx)
	s.UpdateJobStatus(ctx, dep2, StatusDone)
	s.UnblockReady(ctx)

	// Now child should be pending.
	child, _ = s.GetJob(ctx, childID)
	if child.Status != StatusPending {
		t.Errorf("Status = %q, want %q after all deps done", child.Status, StatusPending)
	}
}

func testUnblockReadyChain(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	// A <- B <- C (linear chain)
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", []string{idA})
	idC := s.AddJob(ctx, "echo c", []string{idB})

	// Complete A -> B unblocks, C stays blocked.
	s.ClaimJob(ctx)
	s.UpdateJobStatus(ctx, idA, StatusDone)
	s.UnblockReady(ctx)

	b, _ := s.GetJob(ctx, idB)
	c, _ := s.GetJob(ctx, idC)
	if b.Status != StatusPending {
		t.Errorf("B status = %q, want pending", b.Status)
	}
	if c.Status != StatusBlocked {
		t.Errorf("C status = %q, want blocked (B not done yet)", c.Status)
	}

	// Complete B -> C unblocks.
	s.ClaimJob(ctx)
	s.UpdateJobStatus(ctx, idB, StatusDone)
	s.UnblockReady(ctx)

	c, _ = s.GetJob(ctx, idC)
	if c.Status != StatusPending {
		t.Errorf("C status = %q, want pending after B done", c.Status)
	}
}

func testUnblockReadyIgnoresNonBlocked(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id := s.AddJob(ctx, "echo hello", nil)
	s.ClaimJob(ctx)

	// Job is running. UnblockReady should not touch it.
	s.UnblockReady(ctx)

	job, _ := s.GetJob(ctx, id)
	if job.Status != StatusRunning {
		t.Errorf("Status = %q, want running (UnblockReady should not touch non-blocked jobs)", job.Status)
	}
}

func testUnblockReadyFullPipeline(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	// Submit a diamond DAG: A, B depend on nothing; C depends on A and B.
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", nil)
	idC := s.AddJob(ctx, "echo c", []string{idA, idB})

	// Claim both A and B so we can complete them independently.
	s.ClaimJob(ctx)
	s.ClaimJob(ctx)

	// Complete A. C should still be blocked.
	s.UpdateJobStatus(ctx, idA, StatusDone)
	s.UnblockReady(ctx)

	c, _ := s.GetJob(ctx, idC)
	if c.Status != StatusBlocked {
		t.Errorf("C should still be blocked, got %s", c.Status)
	}

	// Complete B. C should now be pending.
	s.UpdateJobStatus(ctx, idB, StatusDone)
	s.UnblockReady(ctx)

	c, _ = s.GetJob(ctx, idC)
	if c.Status != StatusPending {
		t.Errorf("C should be pending now, got %s", c.Status)
	}

	// C should now be claimable.
	job, ok := s.ClaimJob(ctx)
	if !ok {
		t.Fatal("expected to claim C")
	}
	if job.ID != idC {
		t.Errorf("claimed %q, want %q", job.ID, idC)
	}
}
