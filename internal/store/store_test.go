package store

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
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

// --- Compliance tests: new Job fields round-trip ---

func testAddJobWithResources(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id := s.AddJob(ctx, AddJobParams{
		Command:   "echo gpu-job",
		Resources: &ResourceSpec{VRAMMB: 8192, MemoryMB: 4096},
	})

	job, found := s.GetJob(ctx, id)
	if !found {
		t.Fatal("expected to find job")
	}
	if job.Resources == nil {
		t.Fatal("Resources is nil, want non-nil")
	}
	if job.Resources.VRAMMB != 8192 {
		t.Errorf("VRAMMB = %d, want 8192", job.Resources.VRAMMB)
	}
	if job.Resources.MemoryMB != 4096 {
		t.Errorf("MemoryMB = %d, want 4096", job.Resources.MemoryMB)
	}
}

func testAddJobWithPriority(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id := s.AddJob(ctx, AddJobParams{
		Command:  "echo priority-job",
		Priority: 5,
	})

	job, _ := s.GetJob(ctx, id)
	if job.Priority != 5 {
		t.Errorf("Priority = %d, want 5", job.Priority)
	}
}

func testAddJobWithQueueName(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id := s.AddJob(ctx, AddJobParams{
		Command:   "echo queued-job",
		QueueName: "training",
	})

	job, _ := s.GetJob(ctx, id)
	if job.QueueName != "training" {
		t.Errorf("QueueName = %q, want %q", job.QueueName, "training")
	}
}

func testAddJobWithGangFields(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id := s.AddJob(ctx, AddJobParams{
		Command:   "echo gang-job",
		GangID:    "gang-abc",
		GangSize:  4,
		GangIndex: 2,
	})

	job, _ := s.GetJob(ctx, id)
	if job.GangID != "gang-abc" {
		t.Errorf("GangID = %q, want %q", job.GangID, "gang-abc")
	}
	if job.GangSize != 4 {
		t.Errorf("GangSize = %d, want 4", job.GangSize)
	}
	if job.GangIndex != 2 {
		t.Errorf("GangIndex = %d, want 2", job.GangIndex)
	}
}

func testAddJobDefaultFields(t *testing.T, factory StoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	id := s.AddJob(ctx, AddJobParams{Command: "echo minimal"})

	job, _ := s.GetJob(ctx, id)
	if job.Resources != nil {
		t.Errorf("Resources = %+v, want nil", job.Resources)
	}
	if job.Priority != 0 {
		t.Errorf("Priority = %d, want 0", job.Priority)
	}
	if job.QueueName != "" {
		t.Errorf("QueueName = %q, want empty", job.QueueName)
	}
	if job.GangID != "" {
		t.Errorf("GangID = %q, want empty", job.GangID)
	}
	if job.GangSize != 0 {
		t.Errorf("GangSize = %d, want 0", job.GangSize)
	}
	if job.GangIndex != 0 {
		t.Errorf("GangIndex = %d, want 0", job.GangIndex)
	}
}

// --- Compliance tests: GangStore ---

// gangJobStore combines GangStore and JobStore — all our backends implement both.
type gangJobStore interface {
	GangStore
	JobStore
}

// GangJobStoreFactory creates a fresh store that implements both interfaces.
type GangJobStoreFactory func(t *testing.T) gangJobStore

func testAddGang(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{
		Command:   "torchrun train.py",
		GangSize:  3,
		Priority:  5,
		Resources: &ResourceSpec{VRAMMB: 8192},
	})

	if gangID == "" {
		t.Fatal("AddGang returned empty gangID")
	}
	if len(taskIDs) != 3 {
		t.Fatalf("expected 3 task IDs, got %d", len(taskIDs))
	}

	for i, id := range taskIDs {
		job, found := s.GetJob(ctx, id)
		if !found {
			t.Fatalf("task %d not found", i)
		}
		if job.Status != StatusBlocked {
			t.Errorf("task %d status = %q, want blocked", i, job.Status)
		}
		if job.GangID != gangID {
			t.Errorf("task %d gang_id = %q, want %q", i, job.GangID, gangID)
		}
		if job.GangSize != 3 {
			t.Errorf("task %d gang_size = %d, want 3", i, job.GangSize)
		}
		if job.GangIndex != i {
			t.Errorf("task %d gang_index = %d, want %d", i, job.GangIndex, i)
		}
		if job.Command != "torchrun train.py" {
			t.Errorf("task %d command = %q, want torchrun train.py", i, job.Command)
		}
		if job.Priority != 5 {
			t.Errorf("task %d priority = %d, want 5", i, job.Priority)
		}
		if job.Resources == nil || job.Resources.VRAMMB != 8192 {
			t.Errorf("task %d resources mismatch", i)
		}
	}
}

func testListGangTasks(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, _ := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 4})

	tasks := s.ListGangTasks(ctx, gangID)
	if len(tasks) != 4 {
		t.Fatalf("expected 4 tasks, got %d", len(tasks))
	}
	for i, task := range tasks {
		if task.GangIndex != i {
			t.Errorf("task[%d].GangIndex = %d, want %d (not sorted?)", i, task.GangIndex, i)
		}
	}
}

func testListGangTasksEmpty(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	tasks := s.ListGangTasks(ctx, "nonexistent-gang")
	if len(tasks) != 0 {
		t.Errorf("expected 0 tasks for nonexistent gang, got %d", len(tasks))
	}
}

func testReserveGang(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})

	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
		taskIDs[2]: "worker-c",
	}

	err := s.ReserveGang(ctx, gangID, assignments, 1)
	if err != nil {
		t.Fatalf("ReserveGang: %v", err)
	}

	for i, id := range taskIDs {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusReserved {
			t.Errorf("task %d status = %q, want reserved", i, job.Status)
		}
		if job.WorkerID != assignments[id] {
			t.Errorf("task %d worker_id = %q, want %q", i, job.WorkerID, assignments[id])
		}
		if job.ReservationEpoch != 1 {
			t.Errorf("task %d reservation_epoch = %d, want 1", i, job.ReservationEpoch)
		}
		if job.ReservedAt == nil {
			t.Errorf("task %d reserved_at is nil, want non-nil", i)
		}
	}
}

func testReserveGangNotBlocked(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 2})

	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)

	// Try to reserve again, tasks are now reserved, not blocked
	err := s.ReserveGang(ctx, gangID, assignments, 2)
	if err == nil {
		t.Fatal("expected error when reserving non-blocked tasks")
	}
}

func testClaimReservedJob(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 2})

	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)

	job, found := s.ClaimReservedJob(ctx, "worker-a")
	if !found {
		t.Fatal("expected to claim reserved job for worker-a")
	}
	if job.Status != StatusRunning {
		t.Errorf("status = %q, want running", job.Status)
	}
	if job.WorkerID != "worker-a" {
		t.Errorf("worker_id = %q, want worker-a", job.WorkerID)
	}
	if job.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", job.Attempts)
	}
	if job.StartedAt == nil {
		t.Error("started_at is nil, want non-nil")
	}

	// Claiming again for same worker should return nothing
	_, found2 := s.ClaimReservedJob(ctx, "worker-a")
	if found2 {
		t.Error("expected no second claim for worker-a")
	}
}

func testClaimReservedJobWrongWorker(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 2})

	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)

	_, found := s.ClaimReservedJob(ctx, "worker-x")
	if found {
		t.Fatal("expected no job for unassigned worker")
	}
}

func testRollbackGang(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})

	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
		taskIDs[2]: "worker-c",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)

	err := s.RollbackGang(ctx, gangID)
	if err != nil {
		t.Fatalf("RollbackGang: %v", err)
	}

	for i, id := range taskIDs {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusBlocked {
			t.Errorf("task %d status = %q, want blocked after rollback", i, job.Status)
		}
		if job.WorkerID != "" {
			t.Errorf("task %d worker_id = %q, want empty after rollback", i, job.WorkerID)
		}
		if job.ReservedAt != nil {
			t.Errorf("task %d reserved_at should be nil after rollback", i)
		}
	}
}

func testRollbackGangSkipsRunning(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})

	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
		taskIDs[2]: "worker-c",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)

	// Claim task 0 (now running)
	_, _ = s.ClaimReservedJob(ctx, "worker-a")

	_ = s.RollbackGang(ctx, gangID)

	job0, _ := s.GetJob(ctx, taskIDs[0])
	if job0.Status != StatusRunning {
		t.Errorf("running task should be untouched after rollback, got %q", job0.Status)
	}

	job1, _ := s.GetJob(ctx, taskIDs[1])
	if job1.Status != StatusBlocked {
		t.Errorf("reserved task should be blocked after rollback, got %q", job1.Status)
	}
}

func testFailGangRetry(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})

	// Reserve and claim task 0
	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
		taskIDs[2]: "worker-c",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)
	_, _ = s.ClaimReservedJob(ctx, "worker-a")

	err := s.FailGang(ctx, gangID, true)
	if err != nil {
		t.Fatalf("FailGang: %v", err)
	}

	// Running task 0 should be untouched
	job0, _ := s.GetJob(ctx, taskIDs[0])
	if job0.Status != StatusRunning {
		t.Errorf("running task should be untouched, got %q", job0.Status)
	}

	// Reserved tasks 1 and 2 should become blocked (retry)
	for _, id := range taskIDs[1:] {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusBlocked {
			t.Errorf("task %s status = %q, want blocked (retry)", id, job.Status)
		}
	}
}

func testFailGangPermanent(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})

	// Reserve, claim task 0, mark it done
	assignments := map[string]string{
		taskIDs[0]: "worker-a",
		taskIDs[1]: "worker-b",
		taskIDs[2]: "worker-c",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)
	_, _ = s.ClaimReservedJob(ctx, "worker-a")
	s.UpdateJobStatus(ctx, taskIDs[0], StatusDone)

	err := s.FailGang(ctx, gangID, false)
	if err != nil {
		t.Fatalf("FailGang: %v", err)
	}

	// Done task 0 should be untouched
	job0, _ := s.GetJob(ctx, taskIDs[0])
	if job0.Status != StatusDone {
		t.Errorf("done task should be untouched, got %q", job0.Status)
	}

	// Reserved tasks 1 and 2 should become failed
	for _, id := range taskIDs[1:] {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusFailed {
			t.Errorf("task %s status = %q, want failed", id, job.Status)
		}
	}
}

// --- Preemption helpers ---

// reserveAndClaimGang reserves all tasks and claims them into running for each
// assigned worker. Returns the assignments it used.
func reserveAndClaimGang(t *testing.T, s gangJobStore, gangID string, taskIDs []string) map[string]string {
	t.Helper()
	ctx := context.Background()
	assignments := make(map[string]string, len(taskIDs))
	for i, id := range taskIDs {
		assignments[id] = fmt.Sprintf("worker-%d", i)
	}
	if err := s.ReserveGang(ctx, gangID, assignments, 1); err != nil {
		t.Fatalf("ReserveGang: %v", err)
	}
	for _, w := range assignments {
		if _, ok := s.ClaimReservedJob(ctx, w); !ok {
			t.Fatalf("ClaimReservedJob(%s) not found", w)
		}
	}
	return assignments
}

func testPreemptGangNoRunning(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, _ := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})

	res, err := s.PreemptGang(ctx, gangID, "nonexistent-trigger")
	if err != nil {
		t.Fatalf("PreemptGang: %v", err)
	}
	if res.Entered {
		t.Errorf("Entered = true, want false when no task is running")
	}
	if res.Epoch != 0 || res.Transitioned != 0 {
		t.Errorf("result should be zero-valued when not entered, got %+v", res)
	}
}

func testPreemptGangHomogeneousRunning(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})
	reserveAndClaimGang(t, s, gangID, taskIDs)

	trigger := taskIDs[0]
	res, err := s.PreemptGang(ctx, gangID, trigger)
	if err != nil {
		t.Fatalf("PreemptGang: %v", err)
	}
	if !res.Entered {
		t.Fatalf("Entered = false, want true")
	}
	if res.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1", res.Epoch)
	}
	if res.Transitioned != 3 {
		t.Errorf("Transitioned = %d, want 3", res.Transitioned)
	}

	for _, id := range taskIDs {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusPreempting {
			t.Errorf("task %s status = %q, want preempting", id, job.Status)
		}
		if job.PreemptionEpoch != 1 {
			t.Errorf("task %s preemption_epoch = %d, want 1", id, job.PreemptionEpoch)
		}
		if job.DrainStartedAt == nil {
			t.Errorf("task %s drain_started_at = nil, want set", id)
		}
	}

	// Retry-budget refund: trigger keeps its claim-time attempts=1 (real failure),
	// siblings are refunded back to 0 (not counted against MaxRetries).
	triggerJob, _ := s.GetJob(ctx, trigger)
	if triggerJob.Attempts != 1 {
		t.Errorf("trigger attempts = %d, want 1 (not refunded)", triggerJob.Attempts)
	}
	for _, id := range taskIDs[1:] {
		job, _ := s.GetJob(ctx, id)
		if job.Attempts != 0 {
			t.Errorf("sibling %s attempts = %d, want 0 (refunded)", id, job.Attempts)
		}
	}
}

func testPreemptGangMixedStates(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})

	// Reserve all 3; claim only 2 into running. Third stays reserved.
	assignments := map[string]string{
		taskIDs[0]: "worker-0",
		taskIDs[1]: "worker-1",
		taskIDs[2]: "worker-2",
	}
	_ = s.ReserveGang(ctx, gangID, assignments, 1)
	_, _ = s.ClaimReservedJob(ctx, "worker-0")
	_, _ = s.ClaimReservedJob(ctx, "worker-1")

	res, err := s.PreemptGang(ctx, gangID, taskIDs[0])
	if err != nil {
		t.Fatalf("PreemptGang: %v", err)
	}
	if !res.Entered {
		t.Fatalf("Entered = false, want true")
	}
	if res.Transitioned != 2 {
		t.Errorf("Transitioned = %d, want 2", res.Transitioned)
	}

	// Running ones -> preempting
	for _, id := range taskIDs[:2] {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusPreempting {
			t.Errorf("task %s status = %q, want preempting", id, job.Status)
		}
	}
	// Reserved one → blocked: a reserved task has no running process to
	// signal, so it skips preempting and is parked directly in blocked.
	reservedTask, _ := s.GetJob(ctx, taskIDs[2])
	if reservedTask.Status != StatusBlocked {
		t.Errorf("reserved task %s status = %q, want blocked", taskIDs[2], reservedTask.Status)
	}
	if reservedTask.WorkerID != "" {
		t.Errorf("reserved task worker_id = %q, want cleared", reservedTask.WorkerID)
	}
	if reservedTask.ReservedAt != nil {
		t.Errorf("reserved task reserved_at should be nil after blocked transition")
	}
}

func testMarkPreemptedEpochMismatch(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 2})
	reserveAndClaimGang(t, s, gangID, taskIDs)
	_, _ = s.PreemptGang(ctx, gangID, taskIDs[0])

	// Wrong epoch.
	if err := s.MarkPreempted(ctx, taskIDs[0], 999); err == nil {
		t.Error("MarkPreempted: expected error on epoch mismatch, got nil")
	}
	// Correct epoch succeeds.
	if err := s.MarkPreempted(ctx, taskIDs[0], 1); err != nil {
		t.Errorf("MarkPreempted(correct epoch): %v", err)
	}
	// Wrong status (already preempted).
	if err := s.MarkPreempted(ctx, taskIDs[0], 1); err == nil {
		t.Error("MarkPreempted: expected error on non-preempting status, got nil")
	}
}

func testForceDrainPreempting(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 2})
	reserveAndClaimGang(t, s, gangID, taskIDs)
	_, _ = s.PreemptGang(ctx, gangID, taskIDs[0])

	// Force drain (bypasses epoch).
	if err := s.ForceDrainPreempting(ctx, taskIDs[0]); err != nil {
		t.Fatalf("ForceDrainPreempting: %v", err)
	}
	job, _ := s.GetJob(ctx, taskIDs[0])
	if job.Status != StatusPreempted {
		t.Errorf("status = %q, want preempted", job.Status)
	}
	// Idempotent: second call on already-preempted is a no-op, not an error.
	if err := s.ForceDrainPreempting(ctx, taskIDs[0]); err != nil {
		t.Errorf("ForceDrainPreempting (idempotent): %v", err)
	}
}

func testCompletePreemptionBlocked(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})
	reserveAndClaimGang(t, s, gangID, taskIDs)
	_, _ = s.PreemptGang(ctx, gangID, taskIDs[0])

	// Drain not yet complete: all tasks in preempting.
	completed, _ := s.CompletePreemption(ctx, gangID)
	if completed {
		t.Errorf("CompletePreemption returned true while tasks are still preempting")
	}

	// Ack all tasks.
	for _, id := range taskIDs {
		if err := s.MarkPreempted(ctx, id, 1); err != nil {
			t.Fatalf("MarkPreempted(%s): %v", id, err)
		}
	}

	completed, err := s.CompletePreemption(ctx, gangID)
	if err != nil {
		t.Fatalf("CompletePreemption: %v", err)
	}
	if !completed {
		t.Fatalf("CompletePreemption returned false after full drain")
	}

	// All tasks -> blocked (MaxRetries=3 default, trigger has attempts=1, siblings=0).
	for _, id := range taskIDs {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusBlocked {
			t.Errorf("task %s status = %q, want blocked", id, job.Status)
		}
		if job.WorkerID != "" {
			t.Errorf("task %s worker_id = %q, want cleared", id, job.WorkerID)
		}
		if job.DrainStartedAt != nil {
			t.Errorf("task %s drain_started_at should be cleared", id)
		}
	}
}

// testPreemptionRetryConvergence drives a gang through 3 rounds of
// same-trigger failure. Because PreemptGang refunds siblings but keeps the
// trigger's +1, only the trigger's Attempts grows across rounds. At
// round 3 the trigger hits MaxRetries and option B in CompletePreemption
// routes the gang to failed. Guards against the infinite-retry bug.
func testPreemptionRetryConvergence(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})
	trigger := taskIDs[0]

	for round := 1; round <= 3; round++ {
		// Reserve and claim all 3.
		assignments := map[string]string{
			taskIDs[0]: "worker-a",
			taskIDs[1]: "worker-b",
			taskIDs[2]: "worker-c",
		}
		if err := s.ReserveGang(ctx, gangID, assignments, round); err != nil {
			t.Fatalf("round %d ReserveGang: %v", round, err)
		}
		for _, w := range []string{"worker-a", "worker-b", "worker-c"} {
			if _, ok := s.ClaimReservedJob(ctx, w); !ok {
				t.Fatalf("round %d ClaimReservedJob(%s) not found", round, w)
			}
		}

		// Fail via preempt.
		res, err := s.PreemptGang(ctx, gangID, trigger)
		if err != nil {
			t.Fatalf("round %d PreemptGang: %v", round, err)
		}
		if !res.Entered {
			t.Fatalf("round %d PreemptGang.Entered = false", round)
		}
		for _, id := range taskIDs {
			if err := s.MarkPreempted(ctx, id, res.Epoch); err != nil {
				t.Fatalf("round %d MarkPreempted(%s): %v", round, id, err)
			}
		}
		if _, err := s.CompletePreemption(ctx, gangID); err != nil {
			t.Fatalf("round %d CompletePreemption: %v", round, err)
		}

		triggerJob, _ := s.GetJob(ctx, trigger)
		if round < 3 {
			// Gang still has retry budget: all blocked, trigger attempts grows each round.
			if triggerJob.Status != StatusBlocked {
				t.Errorf("round %d trigger status = %q, want blocked", round, triggerJob.Status)
			}
			if triggerJob.Attempts != round {
				t.Errorf("round %d trigger attempts = %d, want %d", round, triggerJob.Attempts, round)
			}
			// Siblings' attempts refunded to 0 every round.
			for _, id := range taskIDs[1:] {
				job, _ := s.GetJob(ctx, id)
				if job.Attempts != 0 {
					t.Errorf("round %d sibling %s attempts = %d, want 0", round, id, job.Attempts)
				}
			}
		} else {
			// Round 3: trigger has attempts=3=MaxRetries → option B routes gang to failed.
			for _, id := range taskIDs {
				job, _ := s.GetJob(ctx, id)
				if job.Status != StatusFailed {
					t.Errorf("round 3 task %s status = %q, want failed (gang exhausted)", id, job.Status)
				}
			}
		}
	}
}

// testCompletePreemptionPartialCompletion covers the edge case where one
// task of a gang is already done (or failed) when a peer triggers drain.
// Restart-all semantics cannot cleanly re-admit such a gang, so
// CompletePreemption must route the whole gang to failed — not blocked —
// even though retries would otherwise remain. The already-done task's
// status is preserved on the row.
func testCompletePreemptionPartialCompletion(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 3})
	reserveAndClaimGang(t, s, gangID, taskIDs)

	// Task 0 finishes successfully while 1 and 2 are still running.
	s.UpdateJobStatus(ctx, taskIDs[0], StatusDone)

	// Task 1 fails — gang drain should begin for the remaining running tasks.
	res, err := s.PreemptGang(ctx, gangID, taskIDs[1])
	if err != nil {
		t.Fatalf("PreemptGang: %v", err)
	}
	if !res.Entered {
		t.Fatalf("Entered = false, want true (1 and 2 are running)")
	}
	// Only 1 and 2 should be preempting; 0 is done and stays done.
	if res.Transitioned != 2 {
		t.Errorf("Transitioned = %d, want 2", res.Transitioned)
	}

	// Ack the two drained tasks.
	for _, id := range taskIDs[1:] {
		if err := s.MarkPreempted(ctx, id, res.Epoch); err != nil {
			t.Fatalf("MarkPreempted(%s): %v", id, err)
		}
	}

	completed, err := s.CompletePreemption(ctx, gangID)
	if err != nil {
		t.Fatalf("CompletePreemption: %v", err)
	}
	if !completed {
		t.Fatalf("CompletePreemption returned false after full drain")
	}

	// Partial-completion rule: gang fails even though retries remain.
	// Task 0 stays done; tasks 1 and 2 become failed.
	job0, _ := s.GetJob(ctx, taskIDs[0])
	if job0.Status != StatusDone {
		t.Errorf("task 0 status = %q, want done (preserved)", job0.Status)
	}
	for _, id := range taskIDs[1:] {
		job, _ := s.GetJob(ctx, id)
		if job.Status != StatusFailed {
			t.Errorf("task %s status = %q, want failed (partial completion)", id, job.Status)
		}
	}
}

func testSaveCheckpointEpochGuard(t *testing.T, factory GangJobStoreFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()
	gangID, taskIDs := s.AddGang(ctx, AddJobParams{Command: "train", GangSize: 2})
	reserveAndClaimGang(t, s, gangID, taskIDs)

	// Running status: reject.
	if err := s.SaveCheckpoint(ctx, taskIDs[0], 0, []byte(`{"step":1}`)); err == nil {
		t.Error("SaveCheckpoint: expected error in running status, got nil")
	}

	_, _ = s.PreemptGang(ctx, gangID, taskIDs[0])

	// Wrong epoch: reject.
	if err := s.SaveCheckpoint(ctx, taskIDs[0], 999, []byte(`{"step":1}`)); err == nil {
		t.Error("SaveCheckpoint: expected error on epoch mismatch, got nil")
	}

	// Correct epoch: succeed.
	data := []byte(`{"step":42}`)
	if err := s.SaveCheckpoint(ctx, taskIDs[0], 1, data); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	got, found := s.GetCheckpoint(ctx, taskIDs[0])
	if !found {
		t.Fatal("GetCheckpoint: not found after save")
	}
	// Compare JSON semantically, not byte-for-byte. Postgres stores the
	// checkpoint in a JSONB column, which parses the payload on insert
	// and canonicalizes whitespace on read (e.g. {"step":42} becomes
	// {"step": 42}). The scheduler treats checkpoints as opaque JSON;
	// whitespace fidelity is not part of the contract.
	var gotVal, wantVal any
	if err := json.Unmarshal(got, &gotVal); err != nil {
		t.Fatalf("GetCheckpoint: unmarshal got: %v", err)
	}
	if err := json.Unmarshal(data, &wantVal); err != nil {
		t.Fatalf("GetCheckpoint: unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(gotVal, wantVal) {
		t.Errorf("GetCheckpoint = %s, want (semantically) %s", got, data)
	}
}
