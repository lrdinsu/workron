package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// JobStore defines the behavior any storage backend must have
type JobStore interface {
	AddJob(ctx context.Context, command string, dependsOn []string) string
	ClaimJob(ctx context.Context) (*Job, bool)
	GetJob(ctx context.Context, id string) (*Job, bool)
	ListJobs(ctx context.Context) []*Job
	ListRunningJobs(ctx context.Context) []*Job
	UpdateJobStatus(ctx context.Context, id string, status JobStatus)
	UpdateHeartbeat(ctx context.Context, id string)
	SendHeartbeat(ctx context.Context, id string) error
	UnblockReady(ctx context.Context)
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

// ReaperLocker is an optional interface that storage backends can implement
// to coordinate reaper execution across multiple scheduler instances.
// When multiple schedulers share the same database, only one should run the
// reaper at a time. The reaper checks for this interface via type assertion;
// stores that don't implement it (MemoryStore, SQLiteStore) run the reaper
// unconditionally, which is correct for single-process deployments.
type ReaperLocker interface {
	// WithReaperLock attempts to acquire an exclusive reaper lock and,
	// if successful, executes fn while holding it. Returns true if the lock
	// was acquired (and fn was called), false if another instance holds it.
	WithReaperLock(ctx context.Context, fn func(ctx context.Context)) (acquired bool, err error)
}

// generateID returns a unique job ID using a UUID v4.
func generateID() string {
	return fmt.Sprintf("job-%s", uuid.New().String())
}
