package scheduler

import (
	"cmp"
	"slices"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

// GangCandidate represents a blocked gang eligible for placement.
type GangCandidate struct {
	GangID    string
	GangSize  int
	Priority  int
	CreatedAt time.Time
	Resources *store.ResourceSpec // for each task in a gang, including VRAM and memory.
	TaskIDs   []string
}

// WorkerCapacity tracks a worker's total and currently used resources.
type WorkerCapacity struct {
	WorkerID string
	ExecAddr string
	Total    store.ResourceSpec
	Used     store.ResourceSpec
}

// Available returns the resources not yet consumed by running/reserved jobs.
func (w WorkerCapacity) Available() store.ResourceSpec {
	return store.ResourceSpec{
		VRAMMB:   w.Total.VRAMMB - w.Used.VRAMMB,
		MemoryMB: w.Total.MemoryMB - w.Used.MemoryMB,
	}
}

// SortGangCandidates sorts gangs for placement priority:
// 1. Largest gang_size first (bigger gangs are harder to place, try first)
// 2. Highest priority as tiebreaker
// 3. Oldest submission (earliest CreatedAt) as final tiebreaker
func SortGangCandidates(gangs []GangCandidate) {
	slices.SortFunc(gangs, func(a, b GangCandidate) int {
		if c := cmp.Compare(b.GangSize, a.GangSize); c != 0 {
			return c
		}
		if c := cmp.Compare(b.Priority, a.Priority); c != 0 {
			return c
		}
		return a.CreatedAt.Compare(b.CreatedAt)
	})
}

// CanPlaceGang tries to assign each task in the gang to a distinct worker
// with sufficient available resources. Returns a map of taskID -> workerID
// if placement succeeds, or nil if there aren't enough qualifying workers.
//
// The workers slice is NOT modified, callers must subtract used capacity
// themselves after a successful placement.
func CanPlaceGang(gang GangCandidate, workers []WorkerCapacity) map[string]string {
	// Find workers with enough available resources
	var eligible []WorkerCapacity
	for _, w := range workers {
		if gang.Resources == nil {
			eligible = append(eligible, w)
			continue
		}
		avail := w.Available()
		if avail.VRAMMB >= gang.Resources.VRAMMB && avail.MemoryMB >= gang.Resources.MemoryMB {
			eligible = append(eligible, w)
		}
	}

	if len(eligible) < gang.GangSize {
		return nil
	}

	// Assign one task per worker (first N eligible workers)
	assignments := make(map[string]string, gang.GangSize)
	for i := 0; i < gang.GangSize; i++ {
		assignments[gang.TaskIDs[i]] = eligible[i].WorkerID
	}
	return assignments
}

// ComputeWorkerCapacities builds a capacity snapshot from active workers
// and all running + reserved jobs. Both running and reserved jobs consume
// capacity because a reserved worker is committed to that gang.
func ComputeWorkerCapacities(workers []*store.Worker, jobs []*store.Job) []WorkerCapacity {
	// Index workers by ID
	capMap := make(map[string]*WorkerCapacity, len(workers))
	for _, w := range workers {
		capMap[w.ID] = &WorkerCapacity{
			WorkerID: w.ID,
			ExecAddr: w.ExecAddr,
			Total:    w.Resources,
		}
	}

	// Accumulate used resources from running and reserved jobs
	for _, j := range jobs {
		if j.WorkerID == "" || j.Resources == nil {
			continue
		}
		if j.Status != store.StatusRunning && j.Status != store.StatusReserved {
			continue
		}
		capacity, ok := capMap[j.WorkerID]
		if !ok {
			continue
		}
		capacity.Used.VRAMMB += j.Resources.VRAMMB
		capacity.Used.MemoryMB += j.Resources.MemoryMB
	}

	// Collect into slice
	result := make([]WorkerCapacity, 0, len(capMap))
	for _, capacity := range capMap {
		result = append(result, *capacity)
	}
	return result
}

// SubtractPlacement reduces the available capacity on workers that were
// assigned tasks in a placement. Call after a successful CanPlaceGang +
// ReserveGang so the next gang sees reduced availability.
func SubtractPlacement(capacities []WorkerCapacity, assignments map[string]string, resources *store.ResourceSpec) []WorkerCapacity {
	if resources == nil {
		return capacities
	}

	// Build set of assigned worker IDs
	assignedWorkers := make(map[string]bool, len(assignments))
	for _, workerID := range assignments {
		assignedWorkers[workerID] = true
	}

	for i := range capacities {
		if assignedWorkers[capacities[i].WorkerID] {
			capacities[i].Used.VRAMMB += resources.VRAMMB
			capacities[i].Used.MemoryMB += resources.MemoryMB
		}
	}
	return capacities
}
