package store

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"
)

// MemoryStore holds jobs and workers in memory safely
type MemoryStore struct {
	mu      sync.RWMutex
	jobs    map[string]*Job
	workers map[string]*Worker
}

// NewMemoryStore initializes the store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs:    make(map[string]*Job),
		workers: make(map[string]*Worker),
	}
}

// AddJob safely creates a new job and adds it to the map.
// Jobs with dependencies start as blocked; jobs without start as pending.
func (s *MemoryStore) AddJob(_ context.Context, params AddJobParams) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := generateID()

	status := StatusPending
	if len(params.DependsOn) > 0 {
		status = StatusBlocked
	}

	s.jobs[id] = &Job{
		ID:         id,
		Command:    params.Command,
		Status:     status,
		CreatedAt:  time.Now(),
		MaxRetries: 3, // Defaulting to 3 max retries
		DependsOn:  params.DependsOn,
		Resources:  params.Resources,
		Priority:   params.Priority,
		QueueName:  params.QueueName,
		GangID:     params.GangID,
		GangSize:   params.GangSize,
		GangIndex:  params.GangIndex,
	}

	return id
}

// ClaimJob atomically finds a pending job and marks it as running.
// This ensures two workers don't grab the same job.
func (s *MemoryStore) ClaimJob(_ context.Context) (*Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, job := range s.jobs {
		if job.Status == StatusPending {
			job.Status = StatusRunning
			t := time.Now()
			job.StartedAt = &t
			job.LastHeartbeat = nil
			job.Attempts++
			jobCopy := *job
			return &jobCopy, true
		}
	}

	return nil, false
}

// GetJob safely retrieves a job by its ID
func (s *MemoryStore) GetJob(_ context.Context, id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[id]
	if !exists {
		return nil, false
	}

	jobCopy := *job
	return &jobCopy, true
}

// UpdateJobStatus safely updates a job's completion state
func (s *MemoryStore) UpdateJobStatus(_ context.Context, id string, status JobStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		job.Status = status
		if status == StatusDone || status == StatusFailed {
			t := time.Now()
			job.DoneAt = &t
		}
	}
}

// ListJobs returns all jobs currently in the store in no particular order
func (s *MemoryStore) ListJobs(_ context.Context) []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]*Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobCopy := *job
		list = append(list, &jobCopy)
	}
	return list
}

// ListRunningJobs returns all jobs currently in running status
func (s *MemoryStore) ListRunningJobs(_ context.Context) []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var jobs []*Job
	for _, j := range s.jobs {
		if j.Status == StatusRunning {
			jobCopy := *j
			jobs = append(jobs, &jobCopy)
		}
	}
	if len(jobs) == 0 {
		return []*Job{}
	}
	return jobs
}

// UpdateHeartbeat records the current time as the last known sign of life for a running job.
// Used by the timeout checker to detect dead workers.
func (s *MemoryStore) UpdateHeartbeat(_ context.Context, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		t := time.Now()
		job.LastHeartbeat = &t
	}
}

// SendHeartbeat wraps UpdateHeartbeat to satisfy the worker.JobSource interface.
// In-process, this never fails. The returned string is a heartbeat action
func (s *MemoryStore) SendHeartbeat(ctx context.Context, id string) (string, error) {
	s.UpdateHeartbeat(ctx, id)
	return "", nil
}

// UnblockReady transitions blocked jobs to pending when all their
// dependencies have completed. A job is unblocked only if every ID
// in its DependsOn list has status done.
func (s *MemoryStore) UnblockReady(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, job := range s.jobs {
		if job.Status != StatusBlocked {
			continue
		}

		ready := true
		for _, depID := range job.DependsOn {
			dep, exists := s.jobs[depID]
			if !exists || dep.Status != StatusDone {
				ready = false
				break
			}
		}

		if ready {
			job.Status = StatusPending
		}
	}
}

// SetLastHeartbeat sets the heartbeat to a specific time. Used for testing the reaper.
func (s *MemoryStore) SetLastHeartbeat(id string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		job.LastHeartbeat = &t
	}
}

// SetStartedAt sets the start time to a specific time. Used for testing the reaper.
func (s *MemoryStore) SetStartedAt(id string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		job.StartedAt = &t
	}
}

// --- WorkerStore implementation ---

// RegisterWorker upserts a worker into the store.
// If the worker ID already exists, it updates the existing entry.
func (s *MemoryStore) RegisterWorker(_ context.Context, w Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	w.Status = WorkerActive
	w.LastHeartbeat = now
	w.RegisteredAt = now

	// If worker already exists, preserve the original registration time.
	if existing, ok := s.workers[w.ID]; ok {
		w.RegisteredAt = existing.RegisteredAt
	}

	s.workers[w.ID] = &w
	return nil
}

// WorkerHeartbeat updates the last heartbeat time for a worker.
// Returns an error if the worker is not registered.
func (s *MemoryStore) WorkerHeartbeat(_ context.Context, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %q not found", workerID)
	}

	w.LastHeartbeat = time.Now()
	w.Status = WorkerActive
	return nil
}

// GetWorker retrieves a worker by ID. Returns a copy to prevent mutations.
func (s *MemoryStore) GetWorker(_ context.Context, workerID string) (*Worker, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	w, exists := s.workers[workerID]
	if !exists {
		return nil, false
	}

	wCopy := *w
	return &wCopy, true
}

// ListWorkers returns copies of all registered workers.
func (s *MemoryStore) ListWorkers(_ context.Context) []*Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]*Worker, 0, len(s.workers))
	for _, w := range s.workers {
		wCopy := *w
		list = append(list, &wCopy)
	}
	return list
}

// ListActiveWorkers returns copies of workers with status active.
func (s *MemoryStore) ListActiveWorkers(_ context.Context) []*Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []*Worker
	for _, w := range s.workers {
		if w.Status == WorkerActive {
			wCopy := *w
			list = append(list, &wCopy)
		}
	}
	if list == nil {
		return []*Worker{}
	}
	return list
}

// RemoveStaleWorkers marks workers whose last heartbeat is older than
// timeout as offline. Returns the number of workers marked offline.
func (s *MemoryStore) RemoveStaleWorkers(_ context.Context, timeout time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	count := 0
	for _, w := range s.workers {
		if w.Status == WorkerActive && now.Sub(w.LastHeartbeat) > timeout {
			w.Status = WorkerOffline
			count++
		}
	}
	return count
}

// SetWorkerHeartbeat sets a worker's heartbeat to a specific time. Used for testing.
func (s *MemoryStore) SetWorkerHeartbeat(id string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if w, exists := s.workers[id]; exists {
		w.LastHeartbeat = t
	}
}

// --- GangStore implementation ---

// AddGang creates N tasks sharing the same gang_id. All start as blocked.
func (s *MemoryStore) AddGang(_ context.Context, params AddJobParams) (string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	gangID := generateGangID()
	taskIDs := make([]string, params.GangSize)

	for i := 0; i < params.GangSize; i++ {
		id := generateID()
		s.jobs[id] = &Job{
			ID:         id,
			Command:    params.Command,
			Status:     StatusBlocked,
			CreatedAt:  time.Now(),
			MaxRetries: 3,
			Resources:  params.Resources,
			Priority:   params.Priority,
			QueueName:  params.QueueName,
			GangID:     gangID,
			GangSize:   params.GangSize,
			GangIndex:  i,
		}
		taskIDs[i] = id
	}

	return gangID, taskIDs
}

// ListGangTasks returns all tasks for a gang, sorted by gang_index.
func (s *MemoryStore) ListGangTasks(_ context.Context, gangID string) []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*Job, 0)

	for _, job := range s.jobs {
		if job.GangID == gangID {
			jobCopy := *job
			tasks = append(tasks, &jobCopy)
		}
	}

	// Sort by GangIndex
	slices.SortFunc(tasks, func(a, b *Job) int {
		return cmp.Compare(a.GangIndex, b.GangIndex)
	})

	return tasks
}

// ReserveGang atomically transitions all blocked gang tasks to reserved.
func (s *MemoryStore) ReserveGang(_ context.Context, gangID string, assignments map[string]string, epoch int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect gang tasks and verify all are blocked
	var tasks []*Job
	for _, job := range s.jobs {
		if job.GangID == gangID {
			tasks = append(tasks, job)
		}
	}

	for _, task := range tasks {
		if task.Status != StatusBlocked {
			return fmt.Errorf("gang %s: task %s has status %s, expected blocked", gangID, task.ID, task.Status)
		}
	}

	// All checks passed, apply atomically
	now := time.Now()
	for _, task := range tasks {
		workerID, ok := assignments[task.ID]
		if !ok {
			return fmt.Errorf("gang %s: no assignment for task %s", gangID, task.ID)
		}
		task.Status = StatusReserved
		task.WorkerID = workerID
		task.ReservationEpoch = epoch
		task.ReservedAt = &now
	}

	return nil
}

// ClaimReservedJob finds one reserved job assigned to workerID and transitions it to running.
func (s *MemoryStore) ClaimReservedJob(_ context.Context, workerID string) (*Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, job := range s.jobs {
		if job.Status == StatusReserved && job.WorkerID == workerID {
			job.Status = StatusRunning
			t := time.Now()
			job.StartedAt = &t
			job.LastHeartbeat = nil
			job.Attempts++
			jobCopy := *job
			return &jobCopy, true
		}
	}

	return nil, false
}

// RollbackGang moves all reserved tasks in a gang back to blocked.
func (s *MemoryStore) RollbackGang(_ context.Context, gangID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, job := range s.jobs {
		if job.GangID == gangID && job.Status == StatusReserved {
			job.Status = StatusBlocked
			job.WorkerID = ""
			job.ReservedAt = nil
		}
	}

	return nil
}

// FailGang handles gang failure. Only changes blocked/reserved/pending siblings.
// Running and done siblings are left untouched.
func (s *MemoryStore) FailGang(_ context.Context, gangID string, retry bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	targetStatus := StatusFailed
	if retry {
		targetStatus = StatusBlocked
	}

	for _, job := range s.jobs {
		if job.GangID != gangID {
			continue
		}
		switch job.Status {
		case StatusBlocked, StatusReserved, StatusPending:
			job.Status = targetStatus
			if job.Status == StatusBlocked {
				job.WorkerID = ""
				job.ReservedAt = nil
			}
			if job.Status == StatusFailed {
				t := time.Now()
				job.DoneAt = &t
			}
		}
	}

	return nil
}

// SetReservedAt sets a job's reserved_at to a specific time. Used for testing.
func (s *MemoryStore) SetReservedAt(id string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		job.ReservedAt = &t
	}
}

// SetDrainStartedAt sets a job's drain_started_at to a specific time.
// Used for testing force-drain timeout behavior.
func (s *MemoryStore) SetDrainStartedAt(id string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		job.DrainStartedAt = &t
	}
}

// --- Preemption ---

// PreemptGang enters the gang into coordinated drain if any task is running.
// See GangStore.PreemptGang for the full contract.
func (s *MemoryStore) PreemptGang(_ context.Context, gangID string, triggerJobID string) (PreemptGangResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect gang tasks and determine whether any is currently running.
	var tasks []*Job
	var maxEpoch int
	anyRunning := false
	for _, job := range s.jobs {
		if job.GangID != gangID {
			continue
		}
		tasks = append(tasks, job)
		if job.Status == StatusRunning {
			anyRunning = true
		}
		if job.PreemptionEpoch > maxEpoch {
			maxEpoch = job.PreemptionEpoch
		}
	}

	if !anyRunning {
		return PreemptGangResult{Entered: false}, nil
	}

	newEpoch := maxEpoch + 1
	now := time.Now()
	transitioned := 0

	for _, job := range tasks {
		switch job.Status {
		case StatusRunning:
			job.Status = StatusPreempting
			job.PreemptionEpoch = newEpoch
			job.DrainStartedAt = &now
			// Retry-budget refund: innocent siblings get their claim-time
			// attempt increment returned, so scheduler-driven preemption
			// does not count against MaxRetries. The trigger keeps its +1
			// because that represents the real failure that caused the drain.
			if job.ID != triggerJobID && job.Attempts > 0 {
				job.Attempts--
			}
			transitioned++
		case StatusReserved:
			job.Status = StatusBlocked
			job.WorkerID = ""
			job.ReservedAt = nil
		case StatusPending:
			job.Status = StatusBlocked
		}
		// StatusBlocked stays; StatusPreempting/StatusPreempted/StatusDone/StatusFailed untouched.
	}

	return PreemptGangResult{
		Entered:      true,
		Epoch:        newEpoch,
		Transitioned: transitioned,
	}, nil
}

// MarkPreempted is worker acknowledgement: preempting -> preempted.
// See GangStore.MarkPreempted for the full contract.
func (s *MemoryStore) MarkPreempted(_ context.Context, jobID string, epoch int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %q not found", jobID)
	}
	if job.Status != StatusPreempting {
		return fmt.Errorf("job %q: status %q, expected preempting", jobID, job.Status)
	}
	if job.PreemptionEpoch != epoch {
		return fmt.Errorf("job %q: epoch %d, expected %d", jobID, epoch, job.PreemptionEpoch)
	}

	job.Status = StatusPreempted
	return nil
}

// ForceDrainPreempting is reaper timeout: preempting → preempted, no epoch check.
// See GangStore.ForceDrainPreempting for the full contract.
func (s *MemoryStore) ForceDrainPreempting(_ context.Context, jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %q not found", jobID)
	}
	if job.Status != StatusPreempting {
		// Idempotent: if the worker already acked, do nothing.
		return nil
	}

	job.Status = StatusPreempted
	return nil
}

// CompletePreemption normalizes a drained gang to blocked or failed.
// See GangStore.CompletePreemption for the full contract.
func (s *MemoryStore) CompletePreemption(_ context.Context, gangID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Collect gang tasks and verify drain is complete.
	var tasks []*Job
	anyExhausted := false
	anyTerminal := false
	for _, job := range s.jobs {
		if job.GangID != gangID {
			continue
		}
		if job.Status == StatusRunning || job.Status == StatusPreempting {
			// Still draining.
			return false, nil
		}
		tasks = append(tasks, job)
		if job.Attempts >= job.MaxRetries {
			anyExhausted = true
		}
		if job.Status == StatusDone || job.Status == StatusFailed {
			anyTerminal = true
		}
	}

	if len(tasks) == 0 {
		return false, fmt.Errorf("gang %q: no tasks found", gangID)
	}

	// Gang fails if retries are exhausted OR if any task has already
	// reached a terminal status, partial-completion cannot be re-run
	// under restart-all semantics.
	targetStatus := StatusBlocked
	if anyExhausted || anyTerminal {
		targetStatus = StatusFailed
	}

	now := time.Now()
	for _, job := range tasks {
		// Only transition tasks that participated in drain or are waiting.
		// done/failed stay as-is.
		switch job.Status {
		case StatusPreempted, StatusBlocked:
			job.Status = targetStatus
			job.WorkerID = ""
			job.ReservedAt = nil
			job.DrainStartedAt = nil
			if targetStatus == StatusFailed {
				job.DoneAt = &now
			}
		}
	}

	return true, nil
}

// SaveCheckpoint stores raw bytes on the job, epoch-guarded.
// See GangStore.SaveCheckpoint for the full contract.
func (s *MemoryStore) SaveCheckpoint(_ context.Context, jobID string, epoch int, data json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, exists := s.jobs[jobID]
	if !exists {
		return fmt.Errorf("job %q not found", jobID)
	}
	if job.Status != StatusPreempting {
		return fmt.Errorf("job %q: status %q, expected preempting for checkpoint", jobID, job.Status)
	}
	if job.PreemptionEpoch != epoch {
		return fmt.Errorf("job %q: checkpoint epoch %d, expected %d", jobID, epoch, job.PreemptionEpoch)
	}

	// Copy to avoid aliasing caller-owned slice.
	copied := make(json.RawMessage, len(data))
	copy(copied, data)
	job.Checkpoint = copied
	return nil
}

// GetCheckpoint returns the stored checkpoint bytes for a job, if any.
func (s *MemoryStore) GetCheckpoint(_ context.Context, jobID string) (json.RawMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[jobID]
	if !exists || len(job.Checkpoint) == 0 {
		return nil, false
	}
	// Copy to avoid aliasing.
	copied := make(json.RawMessage, len(job.Checkpoint))
	copy(copied, job.Checkpoint)
	return copied, true
}
