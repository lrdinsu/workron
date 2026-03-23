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

// --- SQLite-specific tests ---

func TestSQLite_PersistenceAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()

	// Open, add a job, close.
	s1, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	id := s1.AddJob(ctx, "echo persist", nil)
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
	depID := s1.AddJob(ctx, "echo dep", nil)
	childID := s1.AddJob(ctx, "echo child", []string{depID})
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
