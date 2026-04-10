//go:build postgres

package store

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

func newTestPostgresStore(t *testing.T) JobStore {
	t.Helper()
	url := os.Getenv("WORKRON_PG_URL")
	if url == "" {
		t.Skip("WORKRON_PG_URL not set, skipping PostgreSQL tests")
	}
	ctx := context.Background()
	s, err := NewPostgresStore(ctx, url, 5)
	if err != nil {
		t.Fatal(err)
	}
	// Clean tables before each test for isolation.
	_, err = s.pool.Exec(ctx, "DELETE FROM jobs")
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.pool.Exec(ctx, "DELETE FROM workers")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// --- Compliance tests ---

func TestPostgres_AddJobReturnsID(t *testing.T) {
	testAddJobReturnsID(t, newTestPostgresStore)
}

func TestPostgres_AddJobUniqueIDs(t *testing.T) {
	testAddJobUniqueIDs(t, newTestPostgresStore)
}

func TestPostgres_GetJobRoundTrip(t *testing.T) {
	testGetJobRoundTrip(t, newTestPostgresStore)
}

func TestPostgres_GetJobNotFound(t *testing.T) {
	testGetJobNotFound(t, newTestPostgresStore)
}

func TestPostgres_GetJobReturnsACopy(t *testing.T) {
	testGetJobReturnsACopy(t, newTestPostgresStore)
}

func TestPostgres_ClaimJobReturnsJob(t *testing.T) {
	testClaimJobReturnsJob(t, newTestPostgresStore)
}

func TestPostgres_ClaimJobEmptyStore(t *testing.T) {
	testClaimJobEmptyStore(t, newTestPostgresStore)
}

func TestPostgres_ClaimJobSkipsNonPending(t *testing.T) {
	testClaimJobSkipsNonPending(t, newTestPostgresStore)
}

func TestPostgres_ClaimJobNoDuplicates(t *testing.T) {
	testClaimJobNoDuplicates(t, newTestPostgresStore)
}

func TestPostgres_UpdateJobStatusDone(t *testing.T) {
	testUpdateJobStatusDone(t, newTestPostgresStore)
}

func TestPostgres_UpdateJobStatusFailed(t *testing.T) {
	testUpdateJobStatusFailed(t, newTestPostgresStore)
}

func TestPostgres_UpdateJobStatusRequeue(t *testing.T) {
	testUpdateJobStatusRequeue(t, newTestPostgresStore)
}

func TestPostgres_ListJobsEmpty(t *testing.T) {
	testListJobsEmpty(t, newTestPostgresStore)
}

func TestPostgres_ListJobsReturnsAll(t *testing.T) {
	testListJobsReturnsAll(t, newTestPostgresStore)
}

func TestPostgres_ListRunningJobsEmpty(t *testing.T) {
	testListRunningJobsEmpty(t, newTestPostgresStore)
}

func TestPostgres_ListRunningJobsFilters(t *testing.T) {
	testListRunningJobsFilters(t, newTestPostgresStore)
}

func TestPostgres_UpdateHeartbeat(t *testing.T) {
	testUpdateHeartbeat(t, newTestPostgresStore)
}

func TestPostgres_AddJobNoDependencies(t *testing.T) {
	testAddJobNoDependencies(t, newTestPostgresStore)
}

func TestPostgres_AddJobWithDependenciesStartsBlocked(t *testing.T) {
	testAddJobWithDependenciesStartsBlocked(t, newTestPostgresStore)
}

func TestPostgres_ClaimJobSkipsBlocked(t *testing.T) {
	testClaimJobSkipsBlocked(t, newTestPostgresStore)
}

func TestPostgres_DependsOnRoundTrip(t *testing.T) {
	testDependsOnRoundTrip(t, newTestPostgresStore)
}

func TestPostgres_UnblockReadyAfterAllDepsDone(t *testing.T) {
	testUnblockReadyAfterAllDepsDone(t, newTestPostgresStore)
}

func TestPostgres_UnblockReadyPartialDeps(t *testing.T) {
	testUnblockReadyPartialDeps(t, newTestPostgresStore)
}

func TestPostgres_UnblockReadyChain(t *testing.T) {
	testUnblockReadyChain(t, newTestPostgresStore)
}

func TestPostgres_UnblockReadyIgnoresNonBlocked(t *testing.T) {
	testUnblockReadyIgnoresNonBlocked(t, newTestPostgresStore)
}

func TestPostgres_UnblockReadyFullPipeline(t *testing.T) {
	testUnblockReadyFullPipeline(t, newTestPostgresStore)
}

// --- New Job fields compliance tests ---

func TestPostgres_AddJobWithResources(t *testing.T) {
	testAddJobWithResources(t, newTestPostgresStore)
}

func TestPostgres_AddJobWithPriority(t *testing.T) {
	testAddJobWithPriority(t, newTestPostgresStore)
}

func TestPostgres_AddJobWithQueueName(t *testing.T) {
	testAddJobWithQueueName(t, newTestPostgresStore)
}

func TestPostgres_AddJobWithGangFields(t *testing.T) {
	testAddJobWithGangFields(t, newTestPostgresStore)
}

func TestPostgres_AddJobDefaultFields(t *testing.T) {
	testAddJobDefaultFields(t, newTestPostgresStore)
}

// --- PostgreSQL-specific tests ---

// TestPostgres_ConcurrentClaimSkipLocked verifies that FOR UPDATE SKIP LOCKED
// allows multiple connections to claim different jobs concurrently without
// blocking each other or double-claiming.
func TestPostgres_ConcurrentClaimSkipLocked(t *testing.T) {
	url := os.Getenv("WORKRON_PG_URL")
	if url == "" {
		t.Skip("WORKRON_PG_URL not set, skipping PostgreSQL tests")
	}
	ctx := context.Background()

	// Create a store with a larger pool to enable true concurrent connections.
	s, err := NewPostgresStore(ctx, url, 10)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.pool.Exec(ctx, "DELETE FROM jobs")
	defer s.Close()

	// Add 20 jobs.
	const numJobs = 20
	for i := 0; i < numJobs; i++ {
		s.AddJob(ctx, AddJobParams{Command: "echo hello"})
	}

	// Claim all 20 from 20 concurrent goroutines (more goroutines than jobs).
	const numWorkers = 30
	claimed := make(chan string, numWorkers)
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
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

	// Verify exactly numJobs unique claims.
	seen := make(map[string]bool)
	for id := range claimed {
		if seen[id] {
			t.Errorf("job %s was claimed more than once (SKIP LOCKED failed)", id)
		}
		seen[id] = true
	}
	if len(seen) != numJobs {
		t.Errorf("claimed %d unique jobs, want %d", len(seen), numJobs)
	}
}

// TestPostgres_PersistenceAcrossReconnect verifies that jobs survive a pool close and reconnect.
func TestPostgres_PersistenceAcrossReconnect(t *testing.T) {
	url := os.Getenv("WORKRON_PG_URL")
	if url == "" {
		t.Skip("WORKRON_PG_URL not set, skipping PostgreSQL tests")
	}
	ctx := context.Background()

	// First connection: add a job.
	s1, err := NewPostgresStore(ctx, url, 5)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s1.pool.Exec(ctx, "DELETE FROM jobs")
	id := s1.AddJob(ctx, AddJobParams{Command: "echo persist"})
	s1.Close()

	// Second connection: verify job exists.
	s2, err := NewPostgresStore(ctx, url, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	job, found := s2.GetJob(ctx, id)
	if !found {
		t.Fatal("job not found after reconnecting to PostgreSQL")
	}
	if job.Command != "echo persist" {
		t.Errorf("Command = %q, want %q", job.Command, "echo persist")
	}
	if job.Status != StatusPending {
		t.Errorf("Status = %q, want %q", job.Status, StatusPending)
	}

	// Clean up.
	_, _ = s2.pool.Exec(ctx, "DELETE FROM jobs")
}

// TestPostgres_WithReaperLock_ConcurrentInstances verifies that only
// one of two concurrent callers acquires the advisory lock.
func TestPostgres_WithReaperLock_ConcurrentInstances(t *testing.T) {
	url := os.Getenv("WORKRON_PG_URL")
	if url == "" {
		t.Skip("WORKRON_PG_URL not set, skipping PostgreSQL tests")
	}
	ctx := context.Background()

	// Two separate stores simulate two scheduler instances.
	s1, err := NewPostgresStore(ctx, url, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()

	s2, err := NewPostgresStore(ctx, url, 5)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	// Both attempt to acquire the lock concurrently.
	// One holds the lock for 200ms so both attempts overlap.
	var wg sync.WaitGroup
	results := make(chan bool, 2)

	for _, s := range []*PostgresStore{s1, s2} {
		wg.Add(1)
		go func(store *PostgresStore) {
			defer wg.Done()
			acquired, err := store.WithReaperLock(ctx, func(_ context.Context) {
				time.Sleep(200 * time.Millisecond) // hold the lock briefly
			})
			if err != nil {
				t.Errorf("WithReaperLock error: %v", err)
				return
			}
			results <- acquired
		}(s)
	}

	wg.Wait()
	close(results)

	acquiredCount := 0
	for acquired := range results {
		if acquired {
			acquiredCount++
		}
	}

	if acquiredCount != 1 {
		t.Errorf("expected exactly 1 lock acquisition, got %d", acquiredCount)
	}
}

// --- WorkerStore compliance tests ---

func newTestPostgresWorkerStore(t *testing.T) WorkerStore {
	t.Helper()
	// Reuse the JobStore factory which already cleans both tables.
	return newTestPostgresStore(t).(*PostgresStore)
}

func TestPostgres_RegisterWorker(t *testing.T) {
	testRegisterWorker(t, newTestPostgresWorkerStore)
}

func TestPostgres_RegisterWorkerUpsert(t *testing.T) {
	testRegisterWorkerUpsert(t, newTestPostgresWorkerStore)
}

func TestPostgres_WorkerHeartbeat(t *testing.T) {
	testWorkerHeartbeat(t, newTestPostgresWorkerStore)
}

func TestPostgres_WorkerHeartbeatNotFound(t *testing.T) {
	testWorkerHeartbeatNotFound(t, newTestPostgresWorkerStore)
}

func TestPostgres_ListWorkers(t *testing.T) {
	testListWorkers(t, newTestPostgresWorkerStore)
}

func TestPostgres_ListActiveWorkers(t *testing.T) {
	testListActiveWorkers(t, newTestPostgresWorkerStore)
}

func TestPostgres_RemoveStaleWorkers(t *testing.T) {
	testRemoveStaleWorkers(t, newTestPostgresWorkerStore)
}

// --- GangStore compliance tests ---

func newTestPostgresGangJobStore(t *testing.T) gangJobStore {
	t.Helper()
	return newTestPostgresStore(t).(*PostgresStore)
}

func TestPostgres_AddGang(t *testing.T) {
	testAddGang(t, newTestPostgresGangJobStore)
}

func TestPostgres_ListGangTasks(t *testing.T) {
	testListGangTasks(t, newTestPostgresGangJobStore)
}

func TestPostgres_ListGangTasksEmpty(t *testing.T) {
	testListGangTasksEmpty(t, newTestPostgresGangJobStore)
}

func TestPostgres_ReserveGang(t *testing.T) {
	testReserveGang(t, newTestPostgresGangJobStore)
}

func TestPostgres_ReserveGangNotBlocked(t *testing.T) {
	testReserveGangNotBlocked(t, newTestPostgresGangJobStore)
}

func TestPostgres_ClaimReservedJob(t *testing.T) {
	testClaimReservedJob(t, newTestPostgresGangJobStore)
}

func TestPostgres_ClaimReservedJobWrongWorker(t *testing.T) {
	testClaimReservedJobWrongWorker(t, newTestPostgresGangJobStore)
}

func TestPostgres_RollbackGang(t *testing.T) {
	testRollbackGang(t, newTestPostgresGangJobStore)
}

func TestPostgres_RollbackGangSkipsRunning(t *testing.T) {
	testRollbackGangSkipsRunning(t, newTestPostgresGangJobStore)
}

func TestPostgres_FailGangRetry(t *testing.T) {
	testFailGangRetry(t, newTestPostgresGangJobStore)
}

func TestPostgres_FailGangPermanent(t *testing.T) {
	testFailGangPermanent(t, newTestPostgresGangJobStore)
}
