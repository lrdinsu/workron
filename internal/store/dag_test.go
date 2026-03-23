package store

import (
	"context"
	"testing"
)

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
	ctx := context.Background()

	err := ValidateDependencies(ctx, s, nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateDeps_EmptySlice(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	err := ValidateDependencies(ctx, s, []string{})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateDeps_LinearChain(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	// A <- B <- C <- new
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", []string{idA})
	idC := s.AddJob(ctx, "echo c", []string{idB})

	err := ValidateDependencies(ctx, s, []string{idC})

	if err != nil {
		t.Errorf("unexpected error for valid linear chain: %v", err)
	}
}

func TestValidateDeps_Diamond(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	//     A
	//    / \
	//   B   C
	//    \ /
	//     D ← new
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", []string{idA})
	idC := s.AddJob(ctx, "echo c", []string{idA})
	idD := s.AddJob(ctx, "echo d", []string{idB, idC})

	err := ValidateDependencies(ctx, s, []string{idD})

	if err != nil {
		t.Errorf("unexpected error for valid diamond DAG: %v", err)
	}
}

func TestValidateDeps_MultipleDependencies(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", nil)
	idC := s.AddJob(ctx, "echo c", nil)

	// New job depends on all three independent jobs.
	err := ValidateDependencies(ctx, s, []string{idA, idB, idC})

	if err != nil {
		t.Errorf("unexpected error for multiple independent deps: %v", err)
	}
}

// --- Missing dependency ---

func TestValidateDeps_NonexistentDependency(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	s.AddJob(ctx, "echo a", nil)

	err := ValidateDependencies(ctx, s, []string{"nonexistent"})

	if err == nil {
		t.Error("expected error for nonexistent dependency")
	}
}

func TestValidateDeps_OneValidOneMissing(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	idA := s.AddJob(ctx, "echo a", nil)

	err := ValidateDependencies(ctx, s, []string{idA, "nonexistent"})

	if err == nil {
		t.Error("expected error when one dependency is missing")
	}
}

// --- Cycle cases (bypassing normal submission to create invalid state) ---

func TestValidateDeps_SimpleCycle(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", nil)

	// Manually create A -> B -> A cycle in the store.
	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idB}
	s.jobs[idB].DependsOn = []string{idA}
	s.mu.Unlock()

	// New job depends on A, traversal hits A -> B -> A.
	err := ValidateDependencies(ctx, s, []string{idA})

	if err == nil {
		t.Error("expected cycle error")
	}
}

func TestValidateDeps_TransitiveCycle(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", nil)
	idC := s.AddJob(ctx, "echo c", nil)

	// Manually create A -> B -> C -> A cycle.
	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idB}
	s.jobs[idB].DependsOn = []string{idC}
	s.jobs[idC].DependsOn = []string{idA}
	s.mu.Unlock()

	err := ValidateDependencies(ctx, s, []string{idA})

	if err == nil {
		t.Error("expected cycle error for transitive cycle")
	}
}

func TestValidateDeps_SelfDependency(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	idA := s.AddJob(ctx, "echo a", nil)

	// Manually make A depend on itself.
	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idA}
	s.mu.Unlock()

	err := ValidateDependencies(ctx, s, []string{idA})

	if err == nil {
		t.Error("expected cycle error for self-dependency")
	}
}

func TestValidateDeps_CycleErrorMessage(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	idA := s.AddJob(ctx, "echo a", nil)
	idB := s.AddJob(ctx, "echo b", nil)

	s.mu.Lock()
	s.jobs[idA].DependsOn = []string{idB}
	s.jobs[idB].DependsOn = []string{idA}
	s.mu.Unlock()

	err := ValidateDependencies(ctx, s, []string{idA})

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
