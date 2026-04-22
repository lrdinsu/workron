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
	SendHeartbeat(ctx context.Context, id string) (HeartbeatResult, error)
	UnblockReady(ctx context.Context)
}

// HeartbeatResult carries any action the scheduler wants the worker to
// take in response to a heartbeat. For an ordinary running job the zero
// value is returned (Action is empty). When a job is in preempting,
// Action is "preempt" and PreemptionEpoch is the round epoch the worker
// must echo back to /jobs/{id}/preempted when its process exits.
type HeartbeatResult struct {
	Action          string
	PreemptionEpoch int
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
	ID               string            `json:"id"`
	Command          string            `json:"command"`
	Status           JobStatus         `json:"status"`
	CreatedAt        time.Time         `json:"created_at"`
	StartedAt        *time.Time        `json:"started_at,omitempty"`
	DoneAt           *time.Time        `json:"done_at,omitempty"`
	LastHeartbeat    *time.Time        `json:"last_heart_beat,omitempty"`
	MaxRetries       int               `json:"max_retries"`
	Attempts         int               `json:"attempts"`
	DependsOn        []string          `json:"depends_on,omitempty"`
	Resources        *ResourceSpec     `json:"resources,omitempty"`
	WorkerID         string            `json:"worker_id,omitempty"`
	Priority         int               `json:"priority,omitempty"`
	QueueName        string            `json:"queue_name,omitempty"`
	GangID           string            `json:"gang_id,omitempty"`
	GangSize         int               `json:"gang_size,omitempty"`
	GangIndex        int               `json:"gang_index,omitempty"`
	Checkpoint       json.RawMessage   `json:"checkpoint,omitempty"`
	Outputs          json.RawMessage   `json:"outputs,omitempty"`
	ReservationEpoch int               `json:"reservation_epoch,omitempty"`
	ReservedAt       *time.Time        `json:"reserved_at,omitempty"`
	PreemptionEpoch  int               `json:"preemption_epoch,omitempty"`
	DrainStartedAt   *time.Time        `json:"drain_started_at,omitempty"`
	Env              map[string]string `json:"env,omitempty"` // not persisted; populated in HTTP responses
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

// GangAdmissionLocker is an optional interface for coordinating gang admission
// across multiple scheduler instances. Uses a separate advisory lock from the
// reaper (different lock ID). Only PostgresStore implements this;
// MemoryStore and SQLiteStore run admission unconditionally.
type GangAdmissionLocker interface {
	WithGangAdmissionLock(ctx context.Context, fn func(ctx context.Context)) (acquired bool, err error)
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

// GangStore defines operations for gang scheduling.
// Like WorkerStore, this is an optional interface discovered via type assertion.
type GangStore interface {
	// AddGang creates N tasks (N = params.GangSize) sharing the same gang_id.
	// All tasks start as blocked and have gang_index 0..N-1.
	AddGang(ctx context.Context, params AddJobParams) (gangID string, taskIDs []string)

	// ListGangTasks returns all tasks for a gang, sorted by gang_index.
	ListGangTasks(ctx context.Context, gangID string) []*Job

	// ReserveGang atomically transitions all blocked gang tasks to reserved,
	// sets each task's worker_id from assignments (taskID -> workerID),
	// increments reservation_epoch, and sets reserved_at to now.
	ReserveGang(ctx context.Context, gangID string, assignments map[string]string, epoch int) error

	// ClaimReservedJob finds one reserved job assigned to workerID and
	// transitions it to running. Returns (nil, false) if none found.
	ClaimReservedJob(ctx context.Context, workerID string) (*Job, bool)

	// RollbackGang moves all reserved tasks in a gang back to blocked,
	// clearing worker_id and reserved_at.
	RollbackGang(ctx context.Context, gangID string) error

	// FailGang propagates a gang task's failure to its non-executing
	// siblings. It is the fallback path taken by handleJobFail and the
	// reaper when PreemptGang returns Entered=false — i.e. when no
	// sibling is currently running, so there is no live execution
	// attempt to drain. When any sibling is still running at failure
	// time, the caller goes through PreemptGang instead, which moves
	// the running tasks into coordinated drain.
	//
	// Status transitions (retry flag decides blocked-vs-failed target):
	//   - blocked / reserved / pending -> blocked (retry=true) or failed (retry=false)
	//   - running / preempting / preempted / done / failed → untouched
	//
	// When a sibling moves to blocked, WorkerID and ReservedAt are
	// cleared. When moving to failed, DoneAt is stamped. Retry=true is
	// chosen by the caller when the triggering task still has
	// attempts remaining; retry=false when retries are exhausted.
	FailGang(ctx context.Context, gangID string, retry bool) error

	// PreemptGang initiates a coordinated drain for a gang.
	//
	// Prerequisites:
	// At least one task in the gang must be actively 'running'. If no tasks
	// are running, this returns Entered=false and makes no changes, signaling
	// the caller to fall back to the instant-abort FailGang path.
	//
	// State Mutations (executed atomically):
	//   - 'running' tasks: Moved to 'preempting'. Bumps PreemptionEpoch and
	//     stamps DrainStartedAt.
	//     *Retry Refund:* Decrements Attempts by 1 for all running tasks EXCEPT
	//     the triggerJobID. The trigger keeps the +1 it received on claim (representing
	//     the real failure). Innocent siblings are refunded so scheduler-driven
	//     preemption does not unfairly burn their retry budget.
	//   - 'reserved' tasks: Moved to 'blocked'. Clears WorkerID and ReservedAt.
	//   - 'pending' tasks: Moved to 'blocked'.
	//   - 'blocked' tasks: Left as 'blocked'.
	//   - 'done' / 'failed' tasks: Left untouched. Their presence will later force
	//     CompletePreemption to route the entire gang to failed, preserving
	//     restart-all semantics.
	//   - 'preempting' / 'preempted' tasks: Left untouched defensively. These
	//     should not appear during normal operation, as a gang cannot re-enter
	//     admission until the previous drain completes.
	PreemptGang(ctx context.Context, gangID string, triggerJobID string) (PreemptGangResult, error)

	// MarkPreempted is worker acknowledgement: preempting -> preempted.
	// Requires the job's current status to be preempting and the supplied
	// epoch to match the current PreemptionEpoch.
	MarkPreempted(ctx context.Context, jobID string, epoch int) error

	// ForceDrainPreempting is reaper timeout: preempting -> preempted.
	// Valid only when the job is currently preempting; does not touch
	// preempted/done/failed. No epoch check. Idempotent.
	ForceDrainPreempting(ctx context.Context, jobID string) error

	// CompletePreemption finalizes the drain cycle for a gang.
	//
	// Prerequisites:
	// This is called once no task in the gang is actively executing or shutting down
	// (i.e., zero tasks are currently in 'running' or 'preempting').
	// Returns completed=false if the gang is still actively draining.
	//
	// Decision Rules:
	// The final state of the gang is decided strictly based on its members:
	//   1. gang → failed: If any task has Attempts >= MaxRetries, OR if any
	//      task is already 'done' or 'failed'. (Because Workron uses restart-all
	//      semantics, a gang cannot be cleanly re-admitted if one of its tasks
	//      has already permanently terminated).
	//   2. gang → blocked: If none of the above are true, the gang becomes
	//      eligible for re-admission in the next scheduling cycle.
	//
	// State Mutations:
	// When applying the decided target state (blocked or failed):
	//   - 'preempted' and 'blocked' tasks are moved to the target state.
	//   - 'done' and 'failed' tasks are left as-is (preserving their historical outcomes).
	//   - Attempts counters are NOT modified. The retry refund for innocent siblings was
	//   handled during PreemptGang, so current values accurately reflect real failures.
	//   - WorkerID, ReservedAt, and DrainStartedAt are cleared.
	//   - Checkpoints are preserved.
	CompletePreemption(ctx context.Context, gangID string) (completed bool, err error)

	// SaveCheckpoint stores raw checkpoint bytes for a job.
	//
	// Valid only when the job's current status is 'preempting' and the supplied
	// epoch matches the job's PreemptionEpoch. This strict epoch binding prevents
	// a stale checkpoint from an earlier drain round from overwriting a later one.
	//
	// Note: This is a single slot per job. A successful save completely overwrites
	// any previously stored checkpoint data.
	SaveCheckpoint(ctx context.Context, jobID string, epoch int, data json.RawMessage) error

	// GetCheckpoint returns the stored bytes (if any).
	GetCheckpoint(ctx context.Context, jobID string) (data json.RawMessage, found bool)
}

// PreemptGangResult summarizes the outcome of a PreemptGang call.
type PreemptGangResult struct {
	// Entered is true if the store transitioned the gang into drain
	// (i.e., at least one running task was moved to preempting).
	Entered bool
	// Epoch is the new shared PreemptionEpoch stamped on all tasks
	// moved into preempting in this call. Zero if Entered is false.
	Epoch int
	// Transitioned is the number of tasks moved running -> preempting.
	// Zero if Entered is false.
	Transitioned int
}

// generateID returns a unique job ID using a UUID v4.
func generateID() string {
	return fmt.Sprintf("job-%s", uuid.New().String())
}

// generateGangID returns a unique gang ID using a UUID v4.
func generateGangID() string {
	return fmt.Sprintf("gang-%s", uuid.New().String())
}
