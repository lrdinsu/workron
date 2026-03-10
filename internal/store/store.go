package store

import "time"

// JobStore defines the behavior any storage backend must have
type JobStore interface {
	AddJob(command string) string
	ClaimJob() (*Job, bool)
	GetJob(id string) (*Job, bool)
	ListJobs() []*Job
	UpdateJobStatus(id string, status JobStatus)
	UpdateHeartbeat(id string)
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
	ID            string
	Command       string
	Status        JobStatus
	CreatedAt     time.Time
	StartedAt     *time.Time
	DoneAt        *time.Time
	LastHeartbeat *time.Time
	MaxRetries    int
	Attempts      int
}
