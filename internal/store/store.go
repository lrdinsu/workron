package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// JobStore defines the behavior any storage backend must have
type JobStore interface {
	AddJob(ctx context.Context, params AddJobParams) string
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
	StatusPending    JobStatus = "pending"
	StatusBlocked    JobStatus = "blocked"
	StatusRunning    JobStatus = "running"
	StatusDone       JobStatus = "done"
	StatusFailed     JobStatus = "failed"
	StatusReserved   JobStatus = "reserved"
	StatusPreempting JobStatus = "preempting"
	StatusPreempted  JobStatus = "preempted"
)

// ResourceSpec describes the resources a job requires or a worker provides.
type ResourceSpec struct {
	VRAMMB   int `json:"vram_mb,omitempty"`
	MemoryMB int `json:"memory_mb,omitempty"`
}

// AddJobParams holds all parameters for creating a new job.
// Only Command is required; everything else defaults to zero values.
type AddJobParams struct {
	Command   string        `json:"command"`
	DependsOn []string      `json:"depends_on,omitempty"`
	Resources *ResourceSpec `json:"resources,omitempty"`
	Priority  int           `json:"priority,omitempty"`
	QueueName string        `json:"queue_name,omitempty"`
	GangID    string        `json:"gang_id,omitempty"`
	GangSize  int           `json:"gang_size,omitempty"`
	GangIndex int           `json:"gang_index,omitempty"`
}

// WorkerStatus defines the valid states for a worker node.
type WorkerStatus string

const (
	WorkerActive  WorkerStatus = "active"
	WorkerOffline WorkerStatus = "offline"
)

// Worker represents a registered worker node in the cluster.
type Worker struct {
	ID            string       `json:"id"`
	ExecAddr      string       `json:"exec_addr"`
	Resources     ResourceSpec `json:"resources"`
	Tags          []string     `json:"tags,omitempty"`
	Status        WorkerStatus `json:"status"`
	LastHeartbeat time.Time    `json:"last_heartbeat"`
	RegisteredAt  time.Time    `json:"registered_at"`
}

// Job represents a single command to be executed
type Job struct {
	ID               string          `json:"id"`
	Command          string          `json:"command"`
	Status           JobStatus       `json:"status"`
	CreatedAt        time.Time       `json:"created_at"`
	StartedAt        *time.Time      `json:"started_at,omitempty"`
	DoneAt           *time.Time      `json:"done_at,omitempty"`
	LastHeartbeat    *time.Time      `json:"last_heart_beat,omitempty"`
	MaxRetries       int             `json:"max_retries"`
	Attempts         int             `json:"attempts"`
	DependsOn        []string        `json:"depends_on,omitempty"`
	Resources        *ResourceSpec   `json:"resources,omitempty"`
	WorkerID         string          `json:"worker_id,omitempty"`
	Priority         int             `json:"priority,omitempty"`
	QueueName        string          `json:"queue_name,omitempty"`
	GangID           string          `json:"gang_id,omitempty"`
	GangSize         int             `json:"gang_size,omitempty"`
	GangIndex        int             `json:"gang_index,omitempty"`
	Checkpoint       json.RawMessage `json:"checkpoint,omitempty"`
	Outputs          json.RawMessage `json:"outputs,omitempty"`
	ReservationEpoch int             `json:"reservation_epoch,omitempty"`
	PreemptionEpoch  int             `json:"preemption_epoch,omitempty"`
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

// WorkerStore defines operations for managing worker nodes.
// Like ReaperLocker, this is an optional interface discovered via type assertion.
// All three store backends (Memory, SQLite, Postgres) implement it.
type WorkerStore interface {
	RegisterWorker(ctx context.Context, w Worker) error
	WorkerHeartbeat(ctx context.Context, workerID string) error
	GetWorker(ctx context.Context, workerID string) (*Worker, bool)
	ListWorkers(ctx context.Context) []*Worker
	ListActiveWorkers(ctx context.Context) []*Worker
	RemoveStaleWorkers(ctx context.Context, timeout time.Duration) int
}

// generateID returns a unique job ID using a UUID v4.
func generateID() string {
	return fmt.Sprintf("job-%s", uuid.New().String())
}
