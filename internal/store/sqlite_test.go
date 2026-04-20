package store

import (
	"context"
	"path/filepath"
	"testing"
)

func newTestSQLiteStore(t *testing.T) JobStore {
	t.Helper()
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// --- Compliance tests ---

func TestSQLite_AddJobReturnsID(t *testing.T) {
	testAddJobReturnsID(t, newTestSQLiteStore)
}

func TestSQLite_AddJobUniqueIDs(t *testing.T) {
	testAddJobUniqueIDs(t, newTestSQLiteStore)
}

func TestSQLite_GetJobRoundTrip(t *testing.T) {
	testGetJobRoundTrip(t, newTestSQLiteStore)
}

func TestSQLite_GetJobNotFound(t *testing.T) {
	testGetJobNotFound(t, newTestSQLiteStore)
}

func TestSQLite_GetJobReturnsACopy(t *testing.T) {
	testGetJobReturnsACopy(t, newTestSQLiteStore)
}

func TestSQLite_ClaimJobReturnsJob(t *testing.T) {
	testClaimJobReturnsJob(t, newTestSQLiteStore)
}

func TestSQLite_ClaimJobEmptyStore(t *testing.T) {
	testClaimJobEmptyStore(t, newTestSQLiteStore)
}

func TestSQLite_ClaimJobSkipsNonPending(t *testing.T) {
	testClaimJobSkipsNonPending(t, newTestSQLiteStore)
}

func TestSQLite_ClaimJobNoDuplicates(t *testing.T) {
	testClaimJobNoDuplicates(t, newTestSQLiteStore)
}

func TestSQLite_UpdateJobStatusDone(t *testing.T) {
	testUpdateJobStatusDone(t, newTestSQLiteStore)
}

func TestSQLite_UpdateJobStatusFailed(t *testing.T) {
	testUpdateJobStatusFailed(t, newTestSQLiteStore)
}

func TestSQLite_UpdateJobStatusRequeue(t *testing.T) {
	testUpdateJobStatusRequeue(t, newTestSQLiteStore)
}

func TestSQLite_ListJobsEmpty(t *testing.T) {
	testListJobsEmpty(t, newTestSQLiteStore)
}

func TestSQLite_ListJobsReturnsAll(t *testing.T) {
	testListJobsReturnsAll(t, newTestSQLiteStore)
}

func TestSQLite_ListRunningJobsEmpty(t *testing.T) {
	testListRunningJobsEmpty(t, newTestSQLiteStore)
}

func TestSQLite_ListRunningJobsFilters(t *testing.T) {
	testListRunningJobsFilters(t, newTestSQLiteStore)
}

func TestSQLite_UpdateHeartbeat(t *testing.T) {
	testUpdateHeartbeat(t, newTestSQLiteStore)
}

func TestSQLite_AddJobNoDependencies(t *testing.T) {
	testAddJobNoDependencies(t, newTestSQLiteStore)
}

func TestSQLite_AddJobWithDependenciesStartsBlocked(t *testing.T) {
	testAddJobWithDependenciesStartsBlocked(t, newTestSQLiteStore)
}

func TestSQLite_ClaimJobSkipsBlocked(t *testing.T) {
	testClaimJobSkipsBlocked(t, newTestSQLiteStore)
}

func TestSQLite_DependsOnRoundTrip(t *testing.T) {
	testDependsOnRoundTrip(t, newTestSQLiteStore)
}

func TestSQLite_UnblockReadyAfterAllDepsDone(t *testing.T) {
	testUnblockReadyAfterAllDepsDone(t, newTestSQLiteStore)
}

func TestSQLite_UnblockReadyPartialDeps(t *testing.T) {
	testUnblockReadyPartialDeps(t, newTestSQLiteStore)
}

func TestSQLite_UnblockReadyChain(t *testing.T) {
	testUnblockReadyChain(t, newTestSQLiteStore)
}

func TestSQLite_UnblockReadyIgnoresNonBlocked(t *testing.T) {
	testUnblockReadyIgnoresNonBlocked(t, newTestSQLiteStore)
}

func TestSQLite_UnblockReadyFullPipeline(t *testing.T) {
	testUnblockReadyFullPipeline(t, newTestSQLiteStore)
}

// --- New Job fields compliance tests ---

func TestSQLite_AddJobWithResources(t *testing.T) {
	testAddJobWithResources(t, newTestSQLiteStore)
}

func TestSQLite_AddJobWithPriority(t *testing.T) {
	testAddJobWithPriority(t, newTestSQLiteStore)
}

func TestSQLite_AddJobWithQueueName(t *testing.T) {
	testAddJobWithQueueName(t, newTestSQLiteStore)
}

func TestSQLite_AddJobWithGangFields(t *testing.T) {
	testAddJobWithGangFields(t, newTestSQLiteStore)
}

func TestSQLite_AddJobDefaultFields(t *testing.T) {
	testAddJobDefaultFields(t, newTestSQLiteStore)
}

// --- SQLite-specific tests ---

func TestSQLite_PersistenceAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()

	// Open, add a job, close.
	s1, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	id := s1.AddJob(ctx, AddJobParams{Command: "echo persist"})
	_ = s1.Close()

	// Reopen the same file.
	s2, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	job, found := s2.GetJob(ctx, id)
	if !found {
		t.Fatal("job not found after reopening database")
	}
	if job.Command != "echo persist" {
		t.Errorf("Command = %q, want %q", job.Command, "echo persist")
	}
	if job.Status != StatusPending {
		t.Errorf("Status = %q, want %q", job.Status, StatusPending)
	}
}

func TestSQLite_PersistenceDependsOnSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()

	s1, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	depID := s1.AddJob(ctx, AddJobParams{Command: "echo dep"})
	childID := s1.AddJob(ctx, AddJobParams{Command: "echo child", DependsOn: []string{depID}})
	_ = s1.Close()

	s2, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	job, found := s2.GetJob(ctx, childID)
	if !found {
		t.Fatal("child job not found after reopening database")
	}
	if job.Status != StatusBlocked {
		t.Errorf("Status = %q, want %q", job.Status, StatusBlocked)
	}
	if len(job.DependsOn) != 1 || job.DependsOn[0] != depID {
		t.Errorf("DependsOn = %v, want [%s]", job.DependsOn, depID)
	}
}

// --- WorkerStore compliance tests ---

func newTestSQLiteWorkerStore(t *testing.T) WorkerStore {
	t.Helper()
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLite_RegisterWorker(t *testing.T) {
	testRegisterWorker(t, newTestSQLiteWorkerStore)
}

func TestSQLite_RegisterWorkerUpsert(t *testing.T) {
	testRegisterWorkerUpsert(t, newTestSQLiteWorkerStore)
}

func TestSQLite_WorkerHeartbeat(t *testing.T) {
	testWorkerHeartbeat(t, newTestSQLiteWorkerStore)
}

func TestSQLite_WorkerHeartbeatNotFound(t *testing.T) {
	testWorkerHeartbeatNotFound(t, newTestSQLiteWorkerStore)
}

func TestSQLite_ListWorkers(t *testing.T) {
	testListWorkers(t, newTestSQLiteWorkerStore)
}

func TestSQLite_ListActiveWorkers(t *testing.T) {
	testListActiveWorkers(t, newTestSQLiteWorkerStore)
}

func TestSQLite_RemoveStaleWorkers(t *testing.T) {
	testRemoveStaleWorkers(t, newTestSQLiteWorkerStore)
}

// --- GangStore compliance tests ---

func newTestSQLiteGangJobStore(t *testing.T) gangJobStore {
	t.Helper()
	s, err := NewSQLiteStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLite_AddGang(t *testing.T) {
	testAddGang(t, newTestSQLiteGangJobStore)
}

func TestSQLite_ListGangTasks(t *testing.T) {
	testListGangTasks(t, newTestSQLiteGangJobStore)
}

func TestSQLite_ListGangTasksEmpty(t *testing.T) {
	testListGangTasksEmpty(t, newTestSQLiteGangJobStore)
}

func TestSQLite_ReserveGang(t *testing.T) {
	testReserveGang(t, newTestSQLiteGangJobStore)
}

func TestSQLite_ReserveGangNotBlocked(t *testing.T) {
	testReserveGangNotBlocked(t, newTestSQLiteGangJobStore)
}

func TestSQLite_ClaimReservedJob(t *testing.T) {
	testClaimReservedJob(t, newTestSQLiteGangJobStore)
}

func TestSQLite_ClaimReservedJobWrongWorker(t *testing.T) {
	testClaimReservedJobWrongWorker(t, newTestSQLiteGangJobStore)
}

func TestSQLite_RollbackGang(t *testing.T) {
	testRollbackGang(t, newTestSQLiteGangJobStore)
}

func TestSQLite_RollbackGangSkipsRunning(t *testing.T) {
	testRollbackGangSkipsRunning(t, newTestSQLiteGangJobStore)
}

func TestSQLite_FailGangRetry(t *testing.T) {
	testFailGangRetry(t, newTestSQLiteGangJobStore)
}

func TestSQLite_FailGangPermanent(t *testing.T) {
	testFailGangPermanent(t, newTestSQLiteGangJobStore)
}

func TestSQLite_PreemptGangNoRunning(t *testing.T) {
	testPreemptGangNoRunning(t, newTestSQLiteGangJobStore)
}

func TestSQLite_PreemptGangHomogeneousRunning(t *testing.T) {
	testPreemptGangHomogeneousRunning(t, newTestSQLiteGangJobStore)
}

func TestSQLite_PreemptGangMixedStates(t *testing.T) {
	testPreemptGangMixedStates(t, newTestSQLiteGangJobStore)
}

func TestSQLite_MarkPreemptedEpochMismatch(t *testing.T) {
	testMarkPreemptedEpochMismatch(t, newTestSQLiteGangJobStore)
}

func TestSQLite_ForceDrainPreempting(t *testing.T) {
	testForceDrainPreempting(t, newTestSQLiteGangJobStore)
}

func TestSQLite_CompletePreemptionBlocked(t *testing.T) {
	testCompletePreemptionBlocked(t, newTestSQLiteGangJobStore)
}

func TestSQLite_PreemptionRetryConvergence(t *testing.T) {
	testPreemptionRetryConvergence(t, newTestSQLiteGangJobStore)
}

func TestSQLite_CompletePreemptionPartialCompletion(t *testing.T) {
	testCompletePreemptionPartialCompletion(t, newTestSQLiteGangJobStore)
}

func TestSQLite_SaveCheckpointEpochGuard(t *testing.T) {
	testSaveCheckpointEpochGuard(t, newTestSQLiteGangJobStore)
}
