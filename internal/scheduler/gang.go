package scheduler

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/lrdinsu/workron/internal/store"
)

const (
	gangAdmissionInterval = 5 * time.Second
	reservationTimeout    = 30 * time.Second
)

// GangCandidate represents a blocked gang eligible for placement.
type GangCandidate struct {
	GangID    string
	GangSize  int
	Priority  int
	CreatedAt time.Time
	Resources *store.ResourceSpec
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

// --- Gang Admission Goroutine ---

// StartGangAdmission runs the gang admission cycle every gangAdmissionInterval.
// It rolls back stale reservations, then tries to place blocked gangs on workers.
// Uses GangAdmissionLocker (Postgres advisory lock) for multi-instance safety;
// MemoryStore/SQLiteStore run unconditionally.
func StartGangAdmission(ctx context.Context, js store.JobStore, logger *slog.Logger) {
	gangStore, ok := js.(store.GangStore)
	if !ok {
		logger.Info("store does not support gang scheduling, admission disabled")
		return
	}
	workerStore, ok := js.(store.WorkerStore)
	if !ok {
		logger.Info("store does not support workers, admission disabled")
		return
	}

	ticker := time.NewTicker(gangAdmissionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("gang admission shutting down")
			return
		case <-ticker.C:
			runAdmission := func() {
				RunGangAdmissionOnce(ctx, js, gangStore, workerStore, logger)
			}

			if locker, ok := js.(store.GangAdmissionLocker); ok {
				acquired, err := locker.WithGangAdmissionLock(ctx, func(_ context.Context) {
					runAdmission()
				})
				if err != nil {
					logger.Error("gang admission lock error", "error", err)
				}
				if !acquired {
					logger.Debug("gang admission lock not acquired, skipping")
				}
			} else {
				runAdmission()
			}
		}
	}
}

// RunGangAdmissionOnce runs a single admission cycle. Exported for testing.
func RunGangAdmissionOnce(ctx context.Context, js store.JobStore, gangStore store.GangStore, workerStore store.WorkerStore, logger *slog.Logger) {
	// 1. Rollback stale reservations
	rollbackStaleReservations(ctx, js, gangStore, logger)

	// 2. Find complete blocked gangs
	allJobs := js.ListJobs(ctx)
	candidates := findBlockedGangs(allJobs)
	if len(candidates) == 0 {
		return
	}

	// 3. Sort by placement priority
	SortGangCandidates(candidates)

	// 4. Compute worker capacities (running + reserved jobs both count)
	activeWorkers := workerStore.ListActiveWorkers(ctx)
	if len(activeWorkers) == 0 {
		return
	}
	capacities := ComputeWorkerCapacities(activeWorkers, allJobs)

	// 5. Try to place each gang
	epoch := int(time.Now().Unix())
	for _, gang := range candidates {
		assignments := CanPlaceGang(gang, capacities)
		if assignments == nil {
			logger.Debug("gang cannot be placed", "gang_id", gang.GangID, "gang_size", gang.GangSize)
			continue
		}

		if err := gangStore.ReserveGang(ctx, gang.GangID, assignments, epoch); err != nil {
			logger.Warn("failed to reserve gang", "gang_id", gang.GangID, "error", err)
			continue
		}

		// Subtract placed resources so next gang sees reduced availability
		capacities = SubtractPlacement(capacities, assignments, gang.Resources)
		logger.Info("gang reserved", "gang_id", gang.GangID, "gang_size", gang.GangSize)
	}
}

// rollbackStaleReservations finds gangs with expired reserved_at timestamps and rolls them back to blocked.
func rollbackStaleReservations(ctx context.Context, js store.JobStore, gangStore store.GangStore, logger *slog.Logger) {
	allJobs := js.ListJobs(ctx)
	now := time.Now()

	// Group reserved jobs by gang_id, check if any are stale
	staleGangs := make(map[string]bool)
	for _, j := range allJobs {
		if j.Status == store.StatusReserved && j.ReservedAt != nil {
			if now.Sub(*j.ReservedAt) > reservationTimeout {
				staleGangs[j.GangID] = true
			}
		}
	}

	for gangID := range staleGangs {
		if err := gangStore.RollbackGang(ctx, gangID); err != nil {
			logger.Warn("failed to rollback stale gang", "gang_id", gangID, "error", err)
		} else {
			logger.Info("rolled back stale gang reservation", "gang_id", gangID)
		}
	}
}

// findBlockedGangs scans all jobs and returns GangCandidates for gangs
// where ALL tasks are in blocked status (complete blocked gangs ready for placement).
func findBlockedGangs(jobs []*store.Job) []GangCandidate {
	type gangInfo struct {
		gangID     string
		gangSize   int
		priority   int
		createdAt  time.Time
		resources  *store.ResourceSpec
		taskIDs    []string
		allBlocked bool
	}

	gangs := make(map[string]*gangInfo)
	for _, j := range jobs {
		if j.GangID == "" || j.GangSize == 0 {
			continue
		}

		g, ok := gangs[j.GangID]
		if !ok {
			g = &gangInfo{
				gangID:     j.GangID,
				gangSize:   j.GangSize,
				priority:   j.Priority,
				createdAt:  j.CreatedAt,
				resources:  j.Resources,
				allBlocked: true,
			}
			gangs[j.GangID] = g
		}

		g.taskIDs = append(g.taskIDs, j.ID)
		if j.Status != store.StatusBlocked {
			g.allBlocked = false
		}
	}

	var candidates []GangCandidate
	for _, g := range gangs {
		// Only include gangs where all N tasks are present and blocked
		if g.allBlocked && len(g.taskIDs) == g.gangSize {
			candidates = append(candidates, GangCandidate{
				GangID:    g.gangID,
				GangSize:  g.gangSize,
				Priority:  g.priority,
				CreatedAt: g.createdAt,
				Resources: g.resources,
				TaskIDs:   g.taskIDs,
			})
		}
	}

	return candidates
}
