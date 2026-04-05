package store

import (
	"context"
	"fmt"
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
// In-process, this never fails.
func (s *MemoryStore) SendHeartbeat(ctx context.Context, id string) error {
	s.UpdateHeartbeat(ctx, id)
	return nil
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
