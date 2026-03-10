package store

import "testing"

func TestMemoryStore_AddAndClaimJob(t *testing.T) {
	store := newMemoryStore()

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
