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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})

	if id == "" {
		t.Fatal("AddJob returned empty ID")
	}
}

func testAddJobUniqueIDs(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id1 := s.AddJob(ctx, AddJobParams{Command: "echo a"})
	id2 := s.AddJob(ctx, AddJobParams{Command: "echo b"})

	if id1 == id2 {
		t.Errorf("AddJob returned duplicate IDs: %s", id1)
	}
}

func testGetJobRoundTrip(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	before := time.Now()

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})

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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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
	s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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
		s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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

	s.AddJob(ctx, AddJobParams{Command: "echo a"})
	s.AddJob(ctx, AddJobParams{Command: "echo b"})
	s.AddJob(ctx, AddJobParams{Command: "echo c"})

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

	s.AddJob(ctx, AddJobParams{Command: "echo pending"})

	jobs := s.ListRunningJobs(ctx)

	if len(jobs) != 0 {
		t.Errorf("len = %d, want 0 (no running jobs)", len(jobs))
	}
}

func testListRunningJobsFilters(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	s.AddJob(ctx, AddJobParams{Command: "echo a"})
	s.AddJob(ctx, AddJobParams{Command: "echo b"})
	s.AddJob(ctx, AddJobParams{Command: "echo c"})
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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})

	job, _ := s.GetJob(ctx, id)
	if job.Status != StatusPending {
		t.Errorf("Status = %q, want %q for job without dependencies", job.Status, StatusPending)
	}
}

func testAddJobWithDependenciesStartsBlocked(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	depID := s.AddJob(ctx, AddJobParams{Command: "echo dep"})
	id := s.AddJob(ctx, AddJobParams{Command: "echo child", DependsOn: []string{depID}})

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

	depID := s.AddJob(ctx, AddJobParams{Command: "echo dep"})
	s.AddJob(ctx, AddJobParams{Command: "echo child", DependsOn: []string{depID}})

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

	dep1 := s.AddJob(ctx, AddJobParams{Command: "echo a"})
	dep2 := s.AddJob(ctx, AddJobParams{Command: "echo b"})
	id := s.AddJob(ctx, AddJobParams{Command: "echo c", DependsOn: []string{dep1, dep2}})

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

	depID := s.AddJob(ctx, AddJobParams{Command: "echo dep"})
	childID := s.AddJob(ctx, AddJobParams{Command: "echo child", DependsOn: []string{depID}})

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

	dep1 := s.AddJob(ctx, AddJobParams{Command: "echo a"})
	dep2 := s.AddJob(ctx, AddJobParams{Command: "echo b"})
	childID := s.AddJob(ctx, AddJobParams{Command: "echo child", DependsOn: []string{dep1, dep2}})

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
	idA := s.AddJob(ctx, AddJobParams{Command: "echo a"})
	idB := s.AddJob(ctx, AddJobParams{Command: "echo b", DependsOn: []string{idA}})
	idC := s.AddJob(ctx, AddJobParams{Command: "echo c", DependsOn: []string{idB}})

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

	id := s.AddJob(ctx, AddJobParams{Command: "echo hello"})
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
	idA := s.AddJob(ctx, AddJobParams{Command: "echo a"})
	idB := s.AddJob(ctx, AddJobParams{Command: "echo b"})
	idC := s.AddJob(ctx, AddJobParams{Command: "echo c", DependsOn: []string{idA, idB}})

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

// --- WorkerStore compliance tests ---

// WorkerStoreFactory creates a fresh WorkerStore for each test.
type WorkerStoreFactory func(t *testing.T) WorkerStore

func testRegisterWorker(t *testing.T, factory WorkerStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	err := s.RegisterWorker(ctx, Worker{
		ID:        "w-1",
		ExecAddr:  "localhost:9000",
		Resources: ResourceSpec{VRAMMB: 8192, MemoryMB: 4096},
		Tags:      []string{"gpu"},
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}

	w, found := s.GetWorker(ctx, "w-1")
	if !found {
		t.Fatal("expected to find worker w-1")
	}
	if w.ExecAddr != "localhost:9000" {
		t.Errorf("ExecAddr = %q, want %q", w.ExecAddr, "localhost:9000")
	}
	if w.Resources.VRAMMB != 8192 {
		t.Errorf("VRAMMB = %d, want 8192", w.Resources.VRAMMB)
	}
	if w.Status != WorkerActive {
		t.Errorf("Status = %q, want %q", w.Status, WorkerActive)
	}
	if w.LastHeartbeat.IsZero() {
		t.Error("LastHeartbeat should be set")
	}
	if w.RegisteredAt.IsZero() {
		t.Error("RegisteredAt should be set")
	}
}

func testRegisterWorkerUpsert(t *testing.T, factory WorkerStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	_ = s.RegisterWorker(ctx, Worker{ID: "w-1", ExecAddr: "host:1000"})
	first, _ := s.GetWorker(ctx, "w-1")
	origRegisteredAt := first.RegisteredAt

	time.Sleep(10 * time.Millisecond)

	// Re-register same ID with different addr
	_ = s.RegisterWorker(ctx, Worker{ID: "w-1", ExecAddr: "host:2000"})
	second, _ := s.GetWorker(ctx, "w-1")

	if second.ExecAddr != "host:2000" {
		t.Errorf("ExecAddr = %q, want %q after upsert", second.ExecAddr, "host:2000")
	}
	if !second.RegisteredAt.Equal(origRegisteredAt) {
		t.Error("RegisteredAt should be preserved on upsert")
	}

	// Should still be only 1 worker
	all := s.ListWorkers(ctx)
	if len(all) != 1 {
		t.Errorf("ListWorkers len = %d, want 1", len(all))
	}
}

func testWorkerHeartbeat(t *testing.T, factory WorkerStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	_ = s.RegisterWorker(ctx, Worker{ID: "w-1", ExecAddr: "host:1000"})
	before, _ := s.GetWorker(ctx, "w-1")

	time.Sleep(10 * time.Millisecond)

	err := s.WorkerHeartbeat(ctx, "w-1")
	if err != nil {
		t.Fatalf("WorkerHeartbeat: %v", err)
	}

	after, _ := s.GetWorker(ctx, "w-1")
	if !after.LastHeartbeat.After(before.LastHeartbeat) {
		t.Error("LastHeartbeat should advance after heartbeat")
	}
}

func testWorkerHeartbeatNotFound(t *testing.T, factory WorkerStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	err := s.WorkerHeartbeat(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for heartbeat on unregistered worker")
	}
}

func testListWorkers(t *testing.T, factory WorkerStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	_ = s.RegisterWorker(ctx, Worker{ID: "w-1", ExecAddr: "host:1"})
	_ = s.RegisterWorker(ctx, Worker{ID: "w-2", ExecAddr: "host:2"})
	_ = s.RegisterWorker(ctx, Worker{ID: "w-3", ExecAddr: "host:3"})

	all := s.ListWorkers(ctx)
	if len(all) != 3 {
		t.Errorf("ListWorkers len = %d, want 3", len(all))
	}
}

// workerHeartbeatSetter is a test-only interface for backdating worker heartbeats.
// All three store backends (MemoryStore, SQLiteStore, PostgresStore) implement this.
type workerHeartbeatSetter interface {
	SetWorkerHeartbeat(id string, t time.Time)
}

func testListActiveWorkers(t *testing.T, factory WorkerStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	setter, ok := s.(workerHeartbeatSetter)
	if !ok {
		t.Skip("store does not support SetWorkerHeartbeat")
	}

	_ = s.RegisterWorker(ctx, Worker{ID: "w-1", ExecAddr: "host:1"})
	_ = s.RegisterWorker(ctx, Worker{ID: "w-2", ExecAddr: "host:2"})

	// Make w-1 stale and remove it
	setter.SetWorkerHeartbeat("w-1", time.Now().Add(-2*time.Minute))
	s.RemoveStaleWorkers(ctx, 60*time.Second)

	active := s.ListActiveWorkers(ctx)
	if len(active) != 1 {
		t.Errorf("ListActiveWorkers len = %d, want 1", len(active))
	}
	if active[0].ID != "w-2" {
		t.Errorf("active worker ID = %q, want %q", active[0].ID, "w-2")
	}
}

func testRemoveStaleWorkers(t *testing.T, factory WorkerStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	setter, ok := s.(workerHeartbeatSetter)
	if !ok {
		t.Skip("store does not support SetWorkerHeartbeat")
	}

	_ = s.RegisterWorker(ctx, Worker{ID: "w-1", ExecAddr: "host:1"})
	_ = s.RegisterWorker(ctx, Worker{ID: "w-2", ExecAddr: "host:2"})

	// Make both stale
	staleTime := time.Now().Add(-5 * time.Minute)
	setter.SetWorkerHeartbeat("w-1", staleTime)
	setter.SetWorkerHeartbeat("w-2", staleTime)

	count := s.RemoveStaleWorkers(ctx, 60*time.Second)
	if count != 2 {
		t.Errorf("RemoveStaleWorkers = %d, want 2", count)
	}

	w1, _ := s.GetWorker(ctx, "w-1")
	if w1.Status != WorkerOffline {
		t.Errorf("w-1 status = %q, want %q", w1.Status, WorkerOffline)
	}

	// Calling again should return 0 (already offline)
	count = s.RemoveStaleWorkers(ctx, 60*time.Second)
	if count != 0 {
		t.Errorf("second RemoveStaleWorkers = %d, want 0", count)
	}
}
