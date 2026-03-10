package store

import (
	"fmt"
	"sync"
	"time"
)

// JobStore defines the behavior any storage backend must have
type JobStore interface {
	AddJob(command string) string
	ClaimJob(id string) (*Job, bool)
	GetJob(id string) (*Job, bool)
	UpdateJobStatus(id string, status JobStatus)
}

// JobStatus defines the valid states for a job
type JobStatus string

const (
	StatusPending JobStatus = "pending"
	StatusRunning JobStatus = "running"
	StatusDone    JobStatus = "done"
	StatusFailed  JobStatus = "failed"
)

// Job represents a single command to be executed
type Job struct {
	ID         string
	Command    string
	Status     JobStatus
	CreatedAt  time.Time
	StartedAt  *time.Time
	DoneAt     *time.Time
	Retries    int
	MaxRetries int
	Attempts   int
}

// MemoryStore holds jobs in memory safely
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

// NewMemoryStore initializes the store
func newMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs: make(map[string]*Job),
	}
}

// AddJob safely creates a new job and adds it to the map
func (s *MemoryStore) AddJob(command string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate a unique ID using UnixNano
	id := fmt.Sprintf("job-%d", time.Now().UnixNano())

	s.jobs[id] = &Job{
		ID:         id,
		Command:    command,
		Status:     StatusPending,
		CreatedAt:  time.Now(),
		MaxRetries: 3, // Defaulting to 3 max retries
	}

	return id
}

// ClaimJob atomically finds a pending job and marks it as running
// This ensures two workers don't grab the same job
func (s *MemoryStore) ClaimJob() (*Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, job := range s.jobs {
		if job.Status == StatusPending {
			job.Status = StatusRunning
			job.StartedAt = new(time.Now())
			job.Attempts++
			return job, true
		}
	}

	return nil, false
}

// GetJob safely retrieves a job by its ID
func (s *MemoryStore) GetJob(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[id]
	return job, exists
}

// UpdateJobStatus safely updates a job's completion state
func (s *MemoryStore) UpdateJobStatus(id string, status JobStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if job, exists := s.jobs[id]; exists {
		job.Status = status
		job.DoneAt = new(time.Now())
	}
}
