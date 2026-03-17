package store

import (
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

// --- SQLite-specific tests ---

func TestSQLite_PersistenceAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	// Open, add a job, close.
	s1, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	id := s1.AddJob("echo persist")
	_ = s1.Close()

	// Reopen the same file.
	s2, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	job, found := s2.GetJob(id)
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
