package scheduler

import (
	"testing"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

func TestSortGangCandidates(t *testing.T) {
	now := time.Now()
	gangs := []GangCandidate{
		{GangID: "small-low", GangSize: 2, Priority: 1, CreatedAt: now},
		{GangID: "big-high", GangSize: 4, Priority: 10, CreatedAt: now},
		{GangID: "big-low", GangSize: 4, Priority: 1, CreatedAt: now},
		{GangID: "big-low-old", GangSize: 4, Priority: 1, CreatedAt: now.Add(-time.Hour)},
	}

	SortGangCandidates(gangs)

	// Expected order: biggest first, then highest priority, then oldest
	expected := []string{"big-high", "big-low-old", "big-low", "small-low"}
	for i, g := range gangs {
		if g.GangID != expected[i] {
			t.Errorf("position %d: got %s, want %s", i, g.GangID, expected[i])
		}
	}
}

func TestCanPlaceGang_Sufficient(t *testing.T) {
	gang := GangCandidate{
		GangSize:  3,
		Resources: &store.ResourceSpec{VRAMMB: 4000},
		TaskIDs:   []string{"t0", "t1", "t2"},
	}
	workers := []WorkerCapacity{
		{WorkerID: "w1", Total: store.ResourceSpec{VRAMMB: 8000}},
		{WorkerID: "w2", Total: store.ResourceSpec{VRAMMB: 8000}},
		{WorkerID: "w3", Total: store.ResourceSpec{VRAMMB: 8000}},
	}

	assignments := CanPlaceGang(gang, workers)
	if assignments == nil {
		t.Fatal("expected placement to succeed")
	}
	if len(assignments) != 3 {
		t.Fatalf("expected 3 assignments, got %d", len(assignments))
	}

	// Each task should be assigned to a different worker
	usedWorkers := make(map[string]bool)
	for _, wid := range assignments {
		if usedWorkers[wid] {
			t.Errorf("worker %s assigned twice", wid)
		}
		usedWorkers[wid] = true
	}
}

func TestCanPlaceGang_Insufficient(t *testing.T) {
	gang := GangCandidate{
		GangSize:  4,
		Resources: &store.ResourceSpec{VRAMMB: 4000},
		TaskIDs:   []string{"t0", "t1", "t2", "t3"},
	}
	workers := []WorkerCapacity{
		{WorkerID: "w1", Total: store.ResourceSpec{VRAMMB: 8000}},
		{WorkerID: "w2", Total: store.ResourceSpec{VRAMMB: 8000}},
		{WorkerID: "w3", Total: store.ResourceSpec{VRAMMB: 2000}}, // not enough
	}

	assignments := CanPlaceGang(gang, workers)
	if assignments != nil {
		t.Fatal("expected placement to fail with insufficient workers")
	}
}

func TestCanPlaceGang_NoResourceRequirements(t *testing.T) {
	gang := GangCandidate{
		GangSize:  2,
		Resources: nil,
		TaskIDs:   []string{"t0", "t1"},
	}
	workers := []WorkerCapacity{
		{WorkerID: "w1"},
		{WorkerID: "w2"},
	}

	assignments := CanPlaceGang(gang, workers)
	if assignments == nil {
		t.Fatal("expected placement to succeed with nil resources")
	}
	if len(assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(assignments))
	}
}

func TestCanPlaceGang_MixedCapacity(t *testing.T) {
	gang := GangCandidate{
		GangSize:  2,
		Resources: &store.ResourceSpec{VRAMMB: 4000, MemoryMB: 2000},
		TaskIDs:   []string{"t0", "t1"},
	}
	workers := []WorkerCapacity{
		{WorkerID: "w1", Total: store.ResourceSpec{VRAMMB: 8000, MemoryMB: 4000}},                                                      // enough
		{WorkerID: "w2", Total: store.ResourceSpec{VRAMMB: 8000, MemoryMB: 4000}, Used: store.ResourceSpec{VRAMMB: 5000, MemoryMB: 0}}, // not enough VRAM
		{WorkerID: "w3", Total: store.ResourceSpec{VRAMMB: 8000, MemoryMB: 4000}},                                                      // enough
	}

	assignments := CanPlaceGang(gang, workers)
	if assignments == nil {
		t.Fatal("expected placement to succeed")
	}

	// w2 should not be assigned (insufficient VRAM after used subtraction)
	for _, wid := range assignments {
		if wid == "w2" {
			t.Error("w2 should not be assigned: insufficient available VRAM")
		}
	}
}

func TestComputeWorkerCapacities(t *testing.T) {
	workers := []*store.Worker{
		{ID: "w1", ExecAddr: "host1:8000", Resources: store.ResourceSpec{VRAMMB: 16000, MemoryMB: 32000}},
		{ID: "w2", ExecAddr: "host2:8000", Resources: store.ResourceSpec{VRAMMB: 16000, MemoryMB: 32000}},
	}

	jobs := []*store.Job{
		{WorkerID: "w1", Status: store.StatusRunning, Resources: &store.ResourceSpec{VRAMMB: 4000, MemoryMB: 8000}},
		{WorkerID: "w1", Status: store.StatusReserved, Resources: &store.ResourceSpec{VRAMMB: 4000, MemoryMB: 8000}},
		{WorkerID: "w2", Status: store.StatusRunning, Resources: &store.ResourceSpec{VRAMMB: 8000, MemoryMB: 16000}},
		{WorkerID: "w1", Status: store.StatusDone, Resources: &store.ResourceSpec{VRAMMB: 4000, MemoryMB: 8000}},    // done — should not count
		{WorkerID: "w2", Status: store.StatusPending, Resources: &store.ResourceSpec{VRAMMB: 4000, MemoryMB: 8000}}, // pending — should not count
		{WorkerID: "w1", Status: store.StatusRunning, Resources: nil},                                               // nil resources — should not count
	}

	capacities := ComputeWorkerCapacities(workers, jobs)

	capMap := make(map[string]WorkerCapacity)
	for _, c := range capacities {
		capMap[c.WorkerID] = c
	}

	// w1: 4000 (running) + 4000 (reserved) = 8000 VRAM used
	w1 := capMap["w1"]
	if w1.Used.VRAMMB != 8000 {
		t.Errorf("w1 used VRAM = %d, want 8000", w1.Used.VRAMMB)
	}
	if w1.Used.MemoryMB != 16000 {
		t.Errorf("w1 used Memory = %d, want 16000", w1.Used.MemoryMB)
	}
	if w1.Available().VRAMMB != 8000 {
		t.Errorf("w1 available VRAM = %d, want 8000", w1.Available().VRAMMB)
	}

	// w2: 8000 (running) = 8000 VRAM used
	w2 := capMap["w2"]
	if w2.Used.VRAMMB != 8000 {
		t.Errorf("w2 used VRAM = %d, want 8000", w2.Used.VRAMMB)
	}
	if w2.Available().VRAMMB != 8000 {
		t.Errorf("w2 available VRAM = %d, want 8000", w2.Available().VRAMMB)
	}
}

func TestComputeWorkerCapacities_Empty(t *testing.T) {
	capacities := ComputeWorkerCapacities(nil, nil)
	if len(capacities) != 0 {
		t.Errorf("expected 0 capacities, got %d", len(capacities))
	}
}

func TestSubtractPlacement(t *testing.T) {
	capacities := []WorkerCapacity{
		{WorkerID: "w1", Total: store.ResourceSpec{VRAMMB: 16000}, Used: store.ResourceSpec{VRAMMB: 4000}},
		{WorkerID: "w2", Total: store.ResourceSpec{VRAMMB: 16000}, Used: store.ResourceSpec{VRAMMB: 0}},
		{WorkerID: "w3", Total: store.ResourceSpec{VRAMMB: 16000}, Used: store.ResourceSpec{VRAMMB: 0}},
	}

	assignments := map[string]string{
		"t0": "w1",
		"t1": "w2",
	}
	resources := &store.ResourceSpec{VRAMMB: 8000}

	capacities = SubtractPlacement(capacities, assignments, resources)

	if capacities[0].Used.VRAMMB != 12000 {
		t.Errorf("w1 used = %d, want 12000 (4000 + 8000)", capacities[0].Used.VRAMMB)
	}
	if capacities[1].Used.VRAMMB != 8000 {
		t.Errorf("w2 used = %d, want 8000", capacities[1].Used.VRAMMB)
	}
	if capacities[2].Used.VRAMMB != 0 {
		t.Errorf("w3 used = %d, want 0 (unassigned)", capacities[2].Used.VRAMMB)
	}
}

func TestSubtractPlacement_NilResources(t *testing.T) {
	capacities := []WorkerCapacity{
		{WorkerID: "w1", Used: store.ResourceSpec{VRAMMB: 4000}},
	}

	result := SubtractPlacement(capacities, map[string]string{"t0": "w1"}, nil)

	if result[0].Used.VRAMMB != 4000 {
		t.Errorf("used should be unchanged with nil resources, got %d", result[0].Used.VRAMMB)
	}
}
