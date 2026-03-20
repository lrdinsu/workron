package store

import "testing"

// Note on cycle testing:
// In normal usage, cycles are structurally impossible, you can only depend
// on jobs that already exist, and those were validated at their own submission
// time. To test the cycle detection algorithm, we bypass AddJob validation
// by directly modifying the MemoryStore's internal map to create circular
// dependency states. This is defensive: if a bug or future API change allows
// invalid states, the detection catches it.

// --- Valid cases ---

func TestValidateDeps_NoDependencies(t *testing.T) {
	s := NewMemoryStore()

	err := ValidateDependencies(s, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateDeps_EmptySlice(t *testing.T) {
	s := NewMemoryStore()

	err := ValidateDependencies(s, []string{})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateDeps_LinearChain(t *testing.T) {
	s := NewMemoryStore()
	// A <- B <- C <- new
	idA := s.AddJob("echo a", nil)
	idB := s.AddJob("echo b", []string{idA})
	idC := s.AddJob("echo c", []string{idB})

	err := ValidateDependencies(s, []string{idC})

	if err != nil {
		t.Errorf("unexpected error for valid linear chain: %v", err)
	}
}

func TestValidateDeps_Diamond(t *testing.T) {
	s := NewMemoryStore()
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D ← new
	idA := s.AddJob("echo a", nil)
	idB := s.AddJob("echo b", []string{idA})
	idC := s.AddJob("echo c", []string{idA})
	idD := s.AddJob("echo d", []string{idB, idC})

	err := ValidateDependencies(s, []string{idD})

	if err != nil {
		t.Errorf("unexpected error for valid diamond DAG: %v", err)
	}
}

func TestValidateDeps_MultipleDependencies(t *testing.T) {
	s := NewMemoryStore()
	idA := s.AddJob("echo a", nil)
	idB := s.AddJob("echo b", nil)
	idC := s.AddJob("echo c", nil)

	// New job depends on all three independent jobs.
	err := ValidateDependencies(s, []string{idA, idB, idC})

	if err != nil {
		t.Errorf("unexpected error for multiple independent deps: %v", err)
	}
}

// --- Missing dependency ---

func TestValidateDeps_NonexistentDependency(t *testing.T) {
	s := NewMemoryStore()
	s.AddJob("echo a", nil)

	err := ValidateDependencies(s, []string{"nonexistent"})

	if err == nil {
		t.Error("expected error for nonexistent dependency")
	}
}

func TestValidateDeps_OneValidOneMissing(t *testing.T) {
	s := NewMemoryStore()
	idA := s.AddJob("echo a", nil)

	err := ValidateDependencies(s, []string{idA, "nonexistent"})

	if err == nil {
		t.Error("expected error when one dependency is missing")
	}
}

// --- Cycle cases (bypassing normal submission to create invalid state) ---

func TestValidateDeps_SimpleCycle(t *testing.T) {
	s := NewMemoryStore()
	idA := s.AddJob("echo a", nil)
	idB := s.AddJob("echo b", nil)

	// Manually create A -> B -> A cycle in the store.
	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idB}
	s.jobs[idB].DependsOn = []string{idA}
	s.mu.Unlock()

	// New job depends on A, traversal hits A -> B -> A.
	err := ValidateDependencies(s, []string{idA})

	if err == nil {
		t.Error("expected cycle error")
	}
}

func TestValidateDeps_TransitiveCycle(t *testing.T) {
	s := NewMemoryStore()
	idA := s.AddJob("echo a", nil)
	idB := s.AddJob("echo b", nil)
	idC := s.AddJob("echo c", nil)

	// Manually create A -> B -> C -> A cycle.
	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idB}
	s.jobs[idB].DependsOn = []string{idC}
	s.jobs[idC].DependsOn = []string{idA}
	s.mu.Unlock()

	err := ValidateDependencies(s, []string{idA})

	if err == nil {
		t.Error("expected cycle error for transitive cycle")
	}
}

func TestValidateDeps_SelfDependency(t *testing.T) {
	s := NewMemoryStore()
	idA := s.AddJob("echo a", nil)

	// Manually make A depend on itself.
	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idA}
	s.mu.Unlock()

	err := ValidateDependencies(s, []string{idA})

	if err == nil {
		t.Error("expected cycle error for self-dependency")
	}
}

func TestValidateDeps_CycleErrorMessage(t *testing.T) {
	s := NewMemoryStore()
	idA := s.AddJob("echo a", nil)
	idB := s.AddJob("echo b", nil)

	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idB}
	s.jobs[idB].DependsOn = []string{idA}
	s.mu.Unlock()

	err := ValidateDependencies(s, []string{idA})

	if err == nil {
		t.Fatal("expected cycle error")
	}

	// Error should contain "cycle detected" and the involved job IDs.
	msg := err.Error()
	if !testContains(msg, "cycle detected") {
		t.Errorf("error message %q does not contain 'cycle detected'", msg)
	}
}

// testContains is a simple substring check for test assertions.
func testContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
