package store

import "testing"

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

func TestMemoryStore_AddAndClaimJob(t *testing.T) {
	store := NewMemoryStore()

	// Test adding a Job
	id := store.AddJob("echo test")
	if id == "" {
		t.Fatalf("Expected a valid ID, got an empty string")
	}

	// Test claiming a Job
	job, found := store.ClaimJob()
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
	_, foundAgain := store.ClaimJob()
	if foundAgain {
		t.Errorf("Expected no pending jobs to claim, but found one")
	}
}

func TestMemoryStore_ListRunningJobs(t *testing.T) {
	s := NewMemoryStore()
	// All pending
	s.AddJob("echo one")
	s.AddJob("echo two")
	s.AddJob("echo three")

	// One and two become running
	s.ClaimJob()
	s.ClaimJob()

	running := s.ListRunningJobs()
	if len(running) != 2 {
		t.Errorf("expected 2 running jobs, got %d", len(running))
	}

	for _, j := range running {
		if j.Status != StatusRunning {
			t.Errorf("expected status running, got %s", j.Status)
		}
	}
}
