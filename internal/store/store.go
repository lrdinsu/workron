package store

import (
	"fmt"
	"time"
)

// JobStore defines the behavior any storage backend must have
type JobStore interface {
	AddJob(command string, dependsOn []string) string
	ClaimJob() (*Job, bool)
	GetJob(id string) (*Job, bool)
	ListJobs() []*Job
	ListRunningJobs() []*Job
	UpdateJobStatus(id string, status JobStatus)
	UpdateHeartbeat(id string)
	SendHeartbeat(id string) error
	UnblockReady()
}

// JobStatus defines the valid states for a job
type JobStatus string

const (
	StatusPending JobStatus = "pending"
	StatusBlocked JobStatus = "blocked"
	StatusRunning JobStatus = "running"
	StatusDone    JobStatus = "done"
	StatusFailed  JobStatus = "failed"
)

// Job represents a single command to be executed
type Job struct {
	ID            string     `json:"id"`
	Command       string     `json:"command"`
	Status        JobStatus  `json:"status"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	DoneAt        *time.Time `json:"done_at,omitempty"`
	LastHeartbeat *time.Time `json:"last_heart_beat,omitempty"`
	MaxRetries    int        `json:"max_retries"`
	Attempts      int        `json:"attempts"`
	DependsOn     []string   `json:"depends_on,omitempty"`
}

// Generate a unique ID using UnixNano
func generateID() string {
	return fmt.Sprintf("job-%d", time.Now().UnixNano())
}
