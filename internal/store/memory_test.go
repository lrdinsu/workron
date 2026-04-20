package store

import (
	"context"
	"testing"
)

func newTestMemoryStore(t *testing.T) JobStore {
	t.Helper()
	return NewMemoryStore()
}

// --- Compliance tests ---

func TestMemory_AddJobReturnsID(t *testing.T) {
	testAddJobReturnsID(t, newTestMemoryStore)
}

func TestMemory_AddJobUniqueIDs(t *testing.T) {
	testAddJobUniqueIDs(t, newTestMemoryStore)
}

func TestMemory_GetJobRoundTrip(t *testing.T) {
	testGetJobRoundTrip(t, newTestMemoryStore)
}

func TestMemory_GetJobNotFound(t *testing.T) {
	testGetJobNotFound(t, newTestMemoryStore)
}

func TestMemory_GetJobReturnsACopy(t *testing.T) {
	testGetJobReturnsACopy(t, newTestMemoryStore)
}

func TestMemory_ClaimJobReturnsJob(t *testing.T) {
	testClaimJobReturnsJob(t, newTestMemoryStore)
}

func TestMemory_ClaimJobEmptyStore(t *testing.T) {
	testClaimJobEmptyStore(t, newTestMemoryStore)
}

func TestMemory_ClaimJobSkipsNonPending(t *testing.T) {
	testClaimJobSkipsNonPending(t, newTestMemoryStore)
}

func TestMemory_ClaimJobNoDuplicates(t *testing.T) {
	testClaimJobNoDuplicates(t, newTestMemoryStore)
}

func TestMemory_UpdateJobStatusDone(t *testing.T) {
	testUpdateJobStatusDone(t, newTestMemoryStore)
}

func TestMemory_UpdateJobStatusFailed(t *testing.T) {
	testUpdateJobStatusFailed(t, newTestMemoryStore)
}

func TestMemory_UpdateJobStatusRequeue(t *testing.T) {
	testUpdateJobStatusRequeue(t, newTestMemoryStore)
}

func TestMemory_ListJobsEmpty(t *testing.T) {
	testListJobsEmpty(t, newTestMemoryStore)
}

func TestMemory_ListJobsReturnsAll(t *testing.T) {
	testListJobsReturnsAll(t, newTestMemoryStore)
}

func TestMemory_ListRunningJobsEmpty(t *testing.T) {
	testListRunningJobsEmpty(t, newTestMemoryStore)
}

func TestMemory_ListRunningJobsFilters(t *testing.T) {
	testListRunningJobsFilters(t, newTestMemoryStore)
}

func TestMemory_UpdateHeartbeat(t *testing.T) {
	testUpdateHeartbeat(t, newTestMemoryStore)
}

func TestMemory_AddJobNoDependencies(t *testing.T) {
	testAddJobNoDependencies(t, newTestMemoryStore)
}

func TestMemory_AddJobWithDependenciesStartsBlocked(t *testing.T) {
	testAddJobWithDependenciesStartsBlocked(t, newTestMemoryStore)
}

func TestMemory_ClaimJobSkipsBlocked(t *testing.T) {
	testClaimJobSkipsBlocked(t, newTestMemoryStore)
}

func TestMemory_DependsOnRoundTrip(t *testing.T) {
	testDependsOnRoundTrip(t, newTestMemoryStore)
}

func TestMemory_UnblockReadyAfterAllDepsDone(t *testing.T) {
	testUnblockReadyAfterAllDepsDone(t, newTestMemoryStore)
}

func TestMemory_UnblockReadyPartialDeps(t *testing.T) {
	testUnblockReadyPartialDeps(t, newTestMemoryStore)
}

func TestMemory_UnblockReadyChain(t *testing.T) {
	testUnblockReadyChain(t, newTestMemoryStore)
}

func TestMemory_UnblockReadyIgnoresNonBlocked(t *testing.T) {
	testUnblockReadyIgnoresNonBlocked(t, newTestMemoryStore)
}

func TestMemory_UnblockReadyFullPipeline(t *testing.T) {
	testUnblockReadyFullPipeline(t, newTestMemoryStore)
}

// --- WorkerStore compliance tests ---

func newTestMemoryWorkerStore(t *testing.T) WorkerStore {
	t.Helper()
	return NewMemoryStore()
}

func TestMemory_RegisterWorker(t *testing.T) {
	testRegisterWorker(t, newTestMemoryWorkerStore)
}

func TestMemory_RegisterWorkerUpsert(t *testing.T) {
	testRegisterWorkerUpsert(t, newTestMemoryWorkerStore)
}

func TestMemory_WorkerHeartbeat(t *testing.T) {
	testWorkerHeartbeat(t, newTestMemoryWorkerStore)
}

func TestMemory_WorkerHeartbeatNotFound(t *testing.T) {
	testWorkerHeartbeatNotFound(t, newTestMemoryWorkerStore)
}

func TestMemory_ListWorkers(t *testing.T) {
	testListWorkers(t, newTestMemoryWorkerStore)
}

func TestMemory_ListActiveWorkers(t *testing.T) {
	testListActiveWorkers(t, newTestMemoryWorkerStore)
}

func TestMemory_RemoveStaleWorkers(t *testing.T) {
	testRemoveStaleWorkers(t, newTestMemoryWorkerStore)
}

// --- New Job fields compliance tests ---

func TestMemory_AddJobWithResources(t *testing.T) {
	testAddJobWithResources(t, newTestMemoryStore)
}

func TestMemory_AddJobWithPriority(t *testing.T) {
	testAddJobWithPriority(t, newTestMemoryStore)
}

func TestMemory_AddJobWithQueueName(t *testing.T) {
	testAddJobWithQueueName(t, newTestMemoryStore)
}

func TestMemory_AddJobWithGangFields(t *testing.T) {
	testAddJobWithGangFields(t, newTestMemoryStore)
}

func TestMemory_AddJobDefaultFields(t *testing.T) {
	testAddJobDefaultFields(t, newTestMemoryStore)
}

// --- GangStore compliance tests ---

func newTestMemoryGangJobStore(t *testing.T) gangJobStore {
	t.Helper()
	return NewMemoryStore()
}

func TestMemory_AddGang(t *testing.T) {
	testAddGang(t, newTestMemoryGangJobStore)
}

func TestMemory_ListGangTasks(t *testing.T) {
	testListGangTasks(t, newTestMemoryGangJobStore)

}
func TestMemory_ListGangTasksEmpty(t *testing.T) {
	testListGangTasksEmpty(t, newTestMemoryGangJobStore)
}

func TestMemory_ReserveGang(t *testing.T) {
	testReserveGang(t, newTestMemoryGangJobStore)
}

func TestMemory_ReserveGangNotBlocked(t *testing.T) {
	testReserveGangNotBlocked(t, newTestMemoryGangJobStore)
}

func TestMemory_ClaimReservedJob(t *testing.T) {
	testClaimReservedJob(t, newTestMemoryGangJobStore)
}

func TestMemory_ClaimReservedJobWrongWorker(t *testing.T) {
	testClaimReservedJobWrongWorker(t, newTestMemoryGangJobStore)
}

func TestMemory_RollbackGang(t *testing.T) {
	testRollbackGang(t, newTestMemoryGangJobStore)
}

func TestMemory_RollbackGangSkipsRunning(t *testing.T) {
	testRollbackGangSkipsRunning(t, newTestMemoryGangJobStore)
}

func TestMemory_FailGangRetry(t *testing.T) {
	testFailGangRetry(t, newTestMemoryGangJobStore)
}

func TestMemory_FailGangPermanent(t *testing.T) {
	testFailGangPermanent(t, newTestMemoryGangJobStore)
}

func TestMemory_PreemptGangNoRunning(t *testing.T) {
	testPreemptGangNoRunning(t, newTestMemoryGangJobStore)
}

func TestMemory_PreemptGangHomogeneousRunning(t *testing.T) {
	testPreemptGangHomogeneousRunning(t, newTestMemoryGangJobStore)
}

func TestMemory_PreemptGangMixedStates(t *testing.T) {
	testPreemptGangMixedStates(t, newTestMemoryGangJobStore)
}

func TestMemory_MarkPreemptedEpochMismatch(t *testing.T) {
	testMarkPreemptedEpochMismatch(t, newTestMemoryGangJobStore)
}

func TestMemory_ForceDrainPreempting(t *testing.T) {
	testForceDrainPreempting(t, newTestMemoryGangJobStore)
}

func TestMemory_CompletePreemptionBlocked(t *testing.T) {
	testCompletePreemptionBlocked(t, newTestMemoryGangJobStore)
}

func TestMemory_PreemptionRetryConvergence(t *testing.T) {
	testPreemptionRetryConvergence(t, newTestMemoryGangJobStore)
}

func TestMemory_CompletePreemptionPartialCompletion(t *testing.T) {
	testCompletePreemptionPartialCompletion(t, newTestMemoryGangJobStore)
}

func TestMemory_SaveCheckpointEpochGuard(t *testing.T) {
	testSaveCheckpointEpochGuard(t, newTestMemoryGangJobStore)
}

func TestMemoryStore_AddAndClaimJob(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()

	// Test adding a Job
	id := store.AddJob(ctx, AddJobParams{Command: "echo test"})
	if id == "" {
		t.Fatalf("Expected a valid ID, got an empty string")
	}

	// Test claiming a Job
	job, found := store.ClaimJob(ctx)
	if !found {
		t.Fatalf("Expected a Job to be found, got none")
	}

	// Verify the state changed correctly
	if job.Status != StatusRunning {
		t.Errorf("Expected Job status to be %s, got %s", StatusRunning, job.Status)
	}

	if job.Attempts != 1 {
		t.Errorf("Expected Job attempts to be 1, got %d", job.Attempts)
	}

	// Ensure we can't claim the same Job again
	_, foundAgain := store.ClaimJob(ctx)
	if foundAgain {
		t.Errorf("Expected no pending jobs to claim, but found one")
	}
}

func TestMemoryStore_ListRunningJobs(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	// All pending
	s.AddJob(ctx, AddJobParams{Command: "echo one"})
	s.AddJob(ctx, AddJobParams{Command: "echo two"})
	s.AddJob(ctx, AddJobParams{Command: "echo three"})

	// One and two become running
	s.ClaimJob(ctx)
	s.ClaimJob(ctx)

	running := s.ListRunningJobs(ctx)
	if len(running) != 2 {
		t.Errorf("expected 2 running jobs, got %d", len(running))
	}

	for _, j := range running {
		if j.Status != StatusRunning {
			t.Errorf("expected status running, got %s", j.Status)
		}
	}
}
