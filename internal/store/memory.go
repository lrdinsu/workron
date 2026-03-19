package store

import (
	"sync"
	"time"
)

// MemoryStore holds jobs in memory safely
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

// NewMemoryStore initializes the store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs: make(map[string]*Job),
	}
}

// AddJob safely creates a new job and adds it to the map.
// Jobs with dependencies start as blocked; jobs without start as pending.
func (s *MemoryStore) AddJob(command string, dependsOn []string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := generateID()

	status := StatusPending
	if len(dependsOn) > 0 {
		status = StatusBlocked
	}

	s.jobs[id] = &Job{
		ID:         id,
		Command:    command,
		Status:     status,
		CreatedAt:  time.Now(),
		MaxRetries: 3, // Defaulting to 3 max retries
		DependsOn:  dependsOn,
	}

	return id
}

// ClaimJob atomically finds a pending job and marks it as running.
// This ensures two workers don't grab the same job.
func (s *MemoryStore) ClaimJob() (*Job, bool) {
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
func (s *MemoryStore) GetJob(id string) (*Job, bool) {
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
func (s *MemoryStore) UpdateJobStatus(id string, status JobStatus) {
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
func (s *MemoryStore) ListJobs() []*Job {
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
func (s *MemoryStore) ListRunningJobs() []*Job {
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
func (s *MemoryStore) UpdateHeartbeat(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		t := time.Now()
		job.LastHeartbeat = &t
	}
}

// SendHeartbeat wraps UpdateHeartbeat to satisfy the worker.JobSource interface.
// In-process, this never fails.
func (s *MemoryStore) SendHeartbeat(id string) error {
	s.UpdateHeartbeat(id)
	return nil
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
