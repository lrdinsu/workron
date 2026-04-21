package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore implements JobStore backed by a SQLite database.
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore opens (or creates) a SQLite database at dbPath,
// runs schema migrations, and enables WAL mode for concurrent read/write access.
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// SQLite only supports one writer at a time. Go's default connection pool
	// hands separate connections to concurrent goroutines, causing SQLITE_BUSY
	// errors and per-connection pragma loss. Limiting to one connection serializes
	// all access through Go's pool, eliminating lock contention at the DB level.
	db.SetMaxOpenConns(1)

	// WAL mode provides faster writes by appending to a log instead of copying
	// pages to a rollback journal. With a single connection the concurrency
	// benefit (readers not blocking writers) is unused, but WAL remains the
	// better journal mode for write performance regardless of pool size.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable WAL mode: %w", err)
	}

	// Safety net: if SQLite encounters internal lock contention (e.g., during
	// crash recovery), retry for up to 5 seconds instead of failing immediately.
	// Under normal operation with a single connection this should never trigger.
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}

	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return &SQLiteStore{db: db}, nil
}

// Close closes the underlying database connection.
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func migrate(db *sql.DB) error {
	jobsSchema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id                TEXT PRIMARY KEY,
		command           TEXT NOT NULL,
		status            TEXT NOT NULL DEFAULT 'pending',
		created_at        DATETIME NOT NULL,
		started_at        DATETIME,
		done_at           DATETIME,
		last_heartbeat    DATETIME,
		max_retries       INTEGER DEFAULT 3,
		attempts          INTEGER DEFAULT 0,
		depends_on        TEXT    DEFAULT '[]',
		resources         TEXT,
		worker_id         TEXT    DEFAULT '',
		priority          INTEGER DEFAULT 0,
		queue_name        TEXT    DEFAULT '',
		gang_id           TEXT    DEFAULT '',
		gang_size         INTEGER DEFAULT 0,
		gang_index        INTEGER DEFAULT 0,
		checkpoint        TEXT,
		outputs           TEXT,
		reservation_epoch INTEGER DEFAULT 0,
		reserved_at       DATETIME,
		preemption_epoch  INTEGER DEFAULT 0,
		drain_started_at  DATETIME
	)`
	if _, err := db.Exec(jobsSchema); err != nil {
		return err
	}

	workersSchema := `
	CREATE TABLE IF NOT EXISTS workers (
		id             TEXT PRIMARY KEY,
		exec_addr      TEXT    NOT NULL,
		resources      TEXT    NOT NULL DEFAULT '{}',
		tags           TEXT    NOT NULL DEFAULT '[]',
		status         TEXT    NOT NULL DEFAULT 'active',
		last_heartbeat DATETIME NOT NULL,
		registered_at  DATETIME NOT NULL
	)`
	_, err := db.Exec(workersSchema)
	return err
}

// jobColumns is the canonical column list for all job SELECT queries.
// Every scanJob / queryJobs call must match this order exactly.
const jobColumns = `id, command, status, created_at, started_at, done_at,
	last_heartbeat, max_retries, attempts, depends_on,
	resources, worker_id, priority, queue_name,
	gang_id, gang_size, gang_index,
	checkpoint, outputs, reservation_epoch, reserved_at, preemption_epoch,
	drain_started_at`

// JobStore implementation

func (s *SQLiteStore) AddJob(ctx context.Context, params AddJobParams) string {
	id := generateID()
	now := time.Now()

	status := StatusPending
	if len(params.DependsOn) > 0 {
		status = StatusBlocked
	}

	depsJSON, err := json.Marshal(params.DependsOn)
	if err != nil {
		panic(fmt.Sprintf("sqlite: marshal depends_on: %v", err))
	}

	var resourcesJSON *string
	if params.Resources != nil {
		b, err := json.Marshal(params.Resources)
		if err != nil {
			panic(fmt.Sprintf("sqlite: marshal resources: %v", err))
		}
		s := string(b)
		resourcesJSON = &s
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO jobs (id, command, status, created_at, max_retries, attempts, depends_on,
		                    resources, priority, queue_name, gang_id, gang_size, gang_index)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, params.Command, string(status), now, 3, 0, string(depsJSON),
		resourcesJSON, params.Priority, params.QueueName,
		params.GangID, params.GangSize, params.GangIndex,
	)

	if err != nil {
		// Only fails on disk full or DB corruption, surface it loudly
		// rather than silently dropping a job.
		panic(fmt.Sprintf("sqlite: add job: %v", err))
	}

	return id
}

func (s *SQLiteStore) GetJob(ctx context.Context, id string) (*Job, bool) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE id = ?`, id,
	)
	return scanJob(row)
}

func (s *SQLiteStore) ClaimJob(ctx context.Context) (*Job, bool) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE jobs SET status = 'running', started_at = ?, attempts = attempts + 1, last_heartbeat = NULL
		WHERE id = (SELECT id FROM jobs WHERE status = 'pending' LIMIT 1)
		RETURNING `+jobColumns,
		time.Now(),
	)
	return scanJob(row)
}

func (s *SQLiteStore) UpdateJobStatus(ctx context.Context, id string, status JobStatus) {
	var doneAt *time.Time
	if status == StatusDone || status == StatusFailed {
		t := time.Now()
		doneAt = &t
	}

	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = ?, done_at = ? WHERE id = ?`,
		string(status), doneAt, id,
	)
	if err != nil {
		panic(fmt.Sprintf("sqlite: update job status: %v", err))
	}
}

func (s *SQLiteStore) ListJobs(ctx context.Context) []*Job {
	return s.queryJobs(ctx, `SELECT `+jobColumns+` FROM jobs`)
}

func (s *SQLiteStore) ListRunningJobs(ctx context.Context) []*Job {
	return s.queryJobs(ctx, `SELECT `+jobColumns+` FROM jobs WHERE status = 'running'`)
}

func (s *SQLiteStore) UpdateHeartbeat(ctx context.Context, id string) {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET last_heartbeat = ? WHERE id = ?`,
		time.Now(), id,
	)
	if err != nil {
		panic(fmt.Sprintf("sqlite: update heartbeat: %v", err))
	}
}

// SendHeartbeat wraps UpdateHeartbeat to satisfy the worker.JobSource interface.
// Direct-store mode does not participate in gang preemption signaling,
// so the result is always empty.
func (s *SQLiteStore) SendHeartbeat(ctx context.Context, id string) (HeartbeatResult, error) {
	s.UpdateHeartbeat(ctx, id)
	return HeartbeatResult{}, nil
}

// UnblockReady transitions blocked jobs to pending when all their
// dependencies have completed. Uses json_each to check each element
// of the depends_on JSON array against the jobs table.
func (s *SQLiteStore) UnblockReady(ctx context.Context) {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET status = 'pending'
		WHERE status = 'blocked'
		AND NOT EXISTS (
			SELECT 1 FROM json_each(jobs.depends_on) AS dep
			WHERE dep.value NOT IN (SELECT id FROM jobs WHERE status = 'done')
		)`)
	if err != nil {
		panic(fmt.Sprintf("sqlite: unblock ready: %v", err))
	}
}

// --- Scan helpers ---

// populateJobFromNullables fills a Job's nullable and JSON fields after scanning.
func populateJobFromNullables(j *Job, status string,
	startedAt, doneAt, lastHeartbeat sql.NullTime,
	depsJSON string, resourcesJSON sql.NullString,
	workerID sql.NullString, priority sql.NullInt64,
	queueName, gangID sql.NullString,
	gangSize, gangIndex sql.NullInt64,
	checkpoint, outputs sql.NullString,
	reservationEpoch sql.NullInt64, reservedAt sql.NullTime,
	preemptionEpoch sql.NullInt64,
	drainStartedAt sql.NullTime,
) {
	j.Status = JobStatus(status)
	if startedAt.Valid {
		j.StartedAt = &startedAt.Time
	}
	if doneAt.Valid {
		j.DoneAt = &doneAt.Time
	}
	if lastHeartbeat.Valid {
		j.LastHeartbeat = &lastHeartbeat.Time
	}
	if err := json.Unmarshal([]byte(depsJSON), &j.DependsOn); err != nil {
		panic(fmt.Sprintf("sqlite: unmarshal depends_on: %v", err))
	}
	if resourcesJSON.Valid && resourcesJSON.String != "" {
		var r ResourceSpec
		if err := json.Unmarshal([]byte(resourcesJSON.String), &r); err != nil {
			panic(fmt.Sprintf("sqlite: unmarshal resources: %v", err))
		}
		j.Resources = &r
	}
	if workerID.Valid {
		j.WorkerID = workerID.String
	}
	if priority.Valid {
		j.Priority = int(priority.Int64)
	}
	if queueName.Valid {
		j.QueueName = queueName.String
	}
	if gangID.Valid {
		j.GangID = gangID.String
	}
	if gangSize.Valid {
		j.GangSize = int(gangSize.Int64)
	}
	if gangIndex.Valid {
		j.GangIndex = int(gangIndex.Int64)
	}
	if checkpoint.Valid {
		j.Checkpoint = json.RawMessage(checkpoint.String)
	}
	if outputs.Valid {
		j.Outputs = json.RawMessage(outputs.String)
	}
	if reservationEpoch.Valid {
		j.ReservationEpoch = int(reservationEpoch.Int64)
	}
	if reservedAt.Valid {
		j.ReservedAt = &reservedAt.Time
	}
	if preemptionEpoch.Valid {
		j.PreemptionEpoch = int(preemptionEpoch.Int64)
	}
	if drainStartedAt.Valid {
		j.DrainStartedAt = &drainStartedAt.Time
	}
}

// scanJob scans a single database row into a Job.
// Column order must match jobColumns.
func scanJob(row *sql.Row) (*Job, bool) {
	var j Job
	var status string
	var startedAt, doneAt, lastHeartbeat sql.NullTime
	var depsJSON string
	var resourcesJSON, workerID, queueName, gangID sql.NullString
	var checkpoint, outputs sql.NullString
	var priority, gangSize, gangIndex sql.NullInt64
	var reservationEpoch, preemptionEpoch sql.NullInt64
	var reservedAt, drainStartedAt sql.NullTime

	err := row.Scan(
		&j.ID, &j.Command, &status, &j.CreatedAt,
		&startedAt, &doneAt, &lastHeartbeat,
		&j.MaxRetries, &j.Attempts, &depsJSON,
		&resourcesJSON, &workerID, &priority, &queueName,
		&gangID, &gangSize, &gangIndex,
		&checkpoint, &outputs, &reservationEpoch, &reservedAt, &preemptionEpoch,
		&drainStartedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		panic(fmt.Sprintf("sqlite: scan job: %v", err))
	}

	populateJobFromNullables(&j, status, startedAt, doneAt, lastHeartbeat,
		depsJSON, resourcesJSON, workerID, priority, queueName,
		gangID, gangSize, gangIndex, checkpoint, outputs,
		reservationEpoch, reservedAt, preemptionEpoch, drainStartedAt)

	return &j, true
}

// queryJobs runs a SELECT query and scans all result rows into Jobs.
func (s *SQLiteStore) queryJobs(ctx context.Context, query string, args ...any) []*Job {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		panic(fmt.Sprintf("sqlite: query jobs: %v", err))
	}
	defer func() { _ = rows.Close() }()

	var jobs []*Job
	for rows.Next() {
		var j Job
		var status string
		var startedAt, doneAt, lastHeartbeat sql.NullTime
		var depsJSON string
		var resourcesJSON, workerID, queueName, gangID sql.NullString
		var checkpoint, outputs sql.NullString
		var priority, gangSize, gangIndex sql.NullInt64
		var reservationEpoch, preemptionEpoch sql.NullInt64
		var reservedAt, drainStartedAt sql.NullTime

		if err := rows.Scan(
			&j.ID, &j.Command, &status, &j.CreatedAt,
			&startedAt, &doneAt, &lastHeartbeat,
			&j.MaxRetries, &j.Attempts, &depsJSON,
			&resourcesJSON, &workerID, &priority, &queueName,
			&gangID, &gangSize, &gangIndex,
			&checkpoint, &outputs, &reservationEpoch, &reservedAt, &preemptionEpoch,
			&drainStartedAt,
		); err != nil {
			panic(fmt.Sprintf("sqlite: scan job row: %v", err))
		}

		populateJobFromNullables(&j, status, startedAt, doneAt, lastHeartbeat,
			depsJSON, resourcesJSON, workerID, priority, queueName,
			gangID, gangSize, gangIndex, checkpoint, outputs,
			reservationEpoch, reservedAt, preemptionEpoch, drainStartedAt)

		jobs = append(jobs, &j)
	}

	if len(jobs) == 0 {
		return []*Job{} // return an empty slice (not nil slice)
	}
	return jobs
}

// --- WorkerStore implementation ---

func (s *SQLiteStore) RegisterWorker(ctx context.Context, w Worker) error {
	now := time.Now()

	resourcesJSON, err := json.Marshal(w.Resources)
	if err != nil {
		return fmt.Errorf("sqlite: marshal resources: %w", err)
	}
	tagsJSON, err := json.Marshal(w.Tags)
	if err != nil {
		return fmt.Errorf("sqlite: marshal tags: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO workers (id, exec_addr, resources, tags, status, last_heartbeat, registered_at)
		VALUES (?, ?, ?, ?, 'active', ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			exec_addr      = excluded.exec_addr,
			resources      = excluded.resources,
			tags           = excluded.tags,
			status         = 'active',
			last_heartbeat = excluded.last_heartbeat`,
		w.ID, w.ExecAddr, string(resourcesJSON), string(tagsJSON), now, now,
	)
	if err != nil {
		return fmt.Errorf("sqlite: register worker: %w", err)
	}
	return nil
}

func (s *SQLiteStore) WorkerHeartbeat(ctx context.Context, workerID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE workers SET last_heartbeat = ?, status = 'active' WHERE id = ?`,
		time.Now(), workerID,
	)
	if err != nil {
		return fmt.Errorf("sqlite: worker heartbeat: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("worker %q not found", workerID)
	}
	return nil
}

func (s *SQLiteStore) GetWorker(ctx context.Context, workerID string) (*Worker, bool) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, exec_addr, resources, tags, status, last_heartbeat, registered_at
		 FROM workers WHERE id = ?`, workerID,
	)
	return scanWorker(row)
}

func (s *SQLiteStore) ListWorkers(ctx context.Context) []*Worker {
	return s.queryWorkers(ctx,
		`SELECT id, exec_addr, resources, tags, status, last_heartbeat, registered_at FROM workers`)
}

func (s *SQLiteStore) ListActiveWorkers(ctx context.Context) []*Worker {
	return s.queryWorkers(ctx,
		`SELECT id, exec_addr, resources, tags, status, last_heartbeat, registered_at
		 FROM workers WHERE status = 'active'`)
}

func (s *SQLiteStore) RemoveStaleWorkers(ctx context.Context, timeout time.Duration) int {
	cutoff := time.Now().Add(-timeout)
	res, err := s.db.ExecContext(ctx,
		`UPDATE workers SET status = 'offline'
		 WHERE status = 'active' AND last_heartbeat < ?`, cutoff,
	)
	if err != nil {
		panic(fmt.Sprintf("sqlite: remove stale workers: %v", err))
	}
	n, _ := res.RowsAffected()
	return int(n)
}

// --- Worker scan helpers ---

func scanWorker(row *sql.Row) (*Worker, bool) {
	var w Worker
	var status string
	var resourcesJSON, tagsJSON string

	err := row.Scan(
		&w.ID, &w.ExecAddr, &resourcesJSON, &tagsJSON,
		&status, &w.LastHeartbeat, &w.RegisteredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		panic(fmt.Sprintf("sqlite: scan worker: %v", err))
	}

	w.Status = WorkerStatus(status)
	if err := json.Unmarshal([]byte(resourcesJSON), &w.Resources); err != nil {
		panic(fmt.Sprintf("sqlite: unmarshal worker resources: %v", err))
	}
	if err := json.Unmarshal([]byte(tagsJSON), &w.Tags); err != nil {
		panic(fmt.Sprintf("sqlite: unmarshal worker tags: %v", err))
	}

	return &w, true
}

func (s *SQLiteStore) queryWorkers(ctx context.Context, query string, args ...any) []*Worker {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		panic(fmt.Sprintf("sqlite: query workers: %v", err))
	}
	defer func() { _ = rows.Close() }()

	var workers []*Worker
	for rows.Next() {
		var w Worker
		var status string
		var resourcesJSON, tagsJSON string

		if err := rows.Scan(
			&w.ID, &w.ExecAddr, &resourcesJSON, &tagsJSON,
			&status, &w.LastHeartbeat, &w.RegisteredAt,
		); err != nil {
			panic(fmt.Sprintf("sqlite: scan worker row: %v", err))
		}

		w.Status = WorkerStatus(status)
		if err := json.Unmarshal([]byte(resourcesJSON), &w.Resources); err != nil {
			panic(fmt.Sprintf("sqlite: unmarshal worker resources: %v", err))
		}
		if err := json.Unmarshal([]byte(tagsJSON), &w.Tags); err != nil {
			panic(fmt.Sprintf("sqlite: unmarshal worker tags: %v", err))
		}

		workers = append(workers, &w)
	}

	if len(workers) == 0 {
		return []*Worker{}
	}
	return workers
}

// SetWorkerHeartbeat sets a worker's heartbeat to a specific time. Used for testing.
func (s *SQLiteStore) SetWorkerHeartbeat(id string, t time.Time) {
	_, err := s.db.Exec(`UPDATE workers SET last_heartbeat = ? WHERE id = ?`, t, id)
	if err != nil {
		panic(fmt.Sprintf("sqlite: set worker heartbeat: %v", err))
	}
}

// --- GangStore implementation ---

// AddGang creates N tasks sharing the same gang_id. All start as blocked.
func (s *SQLiteStore) AddGang(ctx context.Context, params AddJobParams) (string, []string) {
	gangID := generateGangID()
	now := time.Now()
	taskIDs := make([]string, params.GangSize)

	var resourcesJSON *string
	if params.Resources != nil {
		b, err := json.Marshal(params.Resources)
		if err != nil {
			panic(fmt.Sprintf("sqlite: marshal resources: %v", err))
		}
		s := string(b)
		resourcesJSON = &s
	}

	for i := 0; i < params.GangSize; i++ {
		id := generateID()
		_, err := s.db.ExecContext(ctx,
			`INSERT INTO jobs (id, command, status, created_at, max_retries, attempts, depends_on,
			                    resources, priority, queue_name, gang_id, gang_size, gang_index)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, params.Command, string(StatusBlocked), now, 3, 0, "[]",
			resourcesJSON, params.Priority, params.QueueName,
			gangID, params.GangSize, i,
		)
		if err != nil {
			panic(fmt.Sprintf("sqlite: add gang task: %v", err))
		}
		taskIDs[i] = id
	}

	return gangID, taskIDs
}

// ListGangTasks returns all tasks for a gang, sorted by gang_index.
func (s *SQLiteStore) ListGangTasks(ctx context.Context, gangID string) []*Job {
	return s.queryJobs(ctx, `SELECT `+jobColumns+` FROM jobs WHERE gang_id = ? ORDER BY gang_index`, gangID)
}

// ReserveGang atomically transitions all blocked gang tasks to reserved.
// SQLite's single-connection pool naturally serializes this.
func (s *SQLiteStore) ReserveGang(ctx context.Context, gangID string, assignments map[string]string, epoch int) error {
	// Check all tasks are blocked
	var nonBlocked int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE gang_id = ? AND status != 'blocked'`, gangID,
	).Scan(&nonBlocked)
	if err != nil {
		panic(fmt.Sprintf("sqlite: check gang status: %v", err))
	}
	if nonBlocked > 0 {
		return fmt.Errorf("gang %s: %d tasks are not blocked", gangID, nonBlocked)
	}

	now := time.Now()
	for jobID, workerID := range assignments {
		_, err := s.db.ExecContext(ctx,
			`UPDATE jobs SET status = 'reserved', worker_id = ?, reservation_epoch = ?, reserved_at = ?
			 WHERE id = ? AND gang_id = ?`,
			workerID, epoch, now, jobID, gangID,
		)
		if err != nil {
			panic(fmt.Sprintf("sqlite: reserve gang task: %v", err))
		}
	}

	return nil
}

// ClaimReservedJob finds one reserved job assigned to workerID and transitions it to running.
func (s *SQLiteStore) ClaimReservedJob(ctx context.Context, workerID string) (*Job, bool) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE jobs SET status = 'running', started_at = ?, attempts = attempts + 1, last_heartbeat = NULL
		WHERE id = (SELECT id FROM jobs WHERE status = 'reserved' AND worker_id = ? LIMIT 1)
		RETURNING `+jobColumns,
		time.Now(), workerID,
	)
	return scanJob(row)
}

// RollbackGang moves all reserved tasks in a gang back to blocked.
func (s *SQLiteStore) RollbackGang(ctx context.Context, gangID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = 'blocked', worker_id = '', reserved_at = NULL
		 WHERE gang_id = ? AND status = 'reserved'`,
		gangID,
	)
	if err != nil {
		panic(fmt.Sprintf("sqlite: rollback gang: %v", err))
	}
	return nil
}

// FailGang is the legacy non-preemption failure-propagation path,
// reached by handleJobFail and the reaper when PreemptGang returned
// Entered=false (no sibling was running, so there was no live execution
// attempt to drain). See GangStore.FailGang for the full contract.
func (s *SQLiteStore) FailGang(ctx context.Context, gangID string, retry bool) error {
	targetStatus := string(StatusFailed)
	if retry {
		targetStatus = string(StatusBlocked)
	}

	var doneAt *time.Time
	if !retry {
		t := time.Now()
		doneAt = &t
	}

	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = ?, worker_id = CASE WHEN ? = 'blocked' THEN '' ELSE worker_id END,
		              reserved_at = CASE WHEN ? = 'blocked' THEN NULL ELSE reserved_at END,
		              done_at = CASE WHEN ? = 'failed' THEN ? ELSE done_at END
		 WHERE gang_id = ? AND status IN ('blocked', 'reserved', 'pending')`,
		targetStatus, targetStatus, targetStatus, targetStatus, doneAt, gangID,
	)
	if err != nil {
		panic(fmt.Sprintf("sqlite: fail gang: %v", err))
	}
	return nil
}

// SetReservedAt sets a job's reserved_at to a specific time. Used for testing.
func (s *SQLiteStore) SetReservedAt(id string, t time.Time) {
	_, err := s.db.Exec(`UPDATE jobs SET reserved_at = ? WHERE id = ?`, t, id)
	if err != nil {
		panic(fmt.Sprintf("sqlite: set reserved_at: %v", err))
	}
}

// SetDrainStartedAt sets a job's drain_started_at to a specific time.
// Used for testing force-drain timeout behavior.
func (s *SQLiteStore) SetDrainStartedAt(id string, t time.Time) {
	_, err := s.db.Exec(`UPDATE jobs SET drain_started_at = ? WHERE id = ?`, t, id)
	if err != nil {
		panic(fmt.Sprintf("sqlite: set drain_started_at: %v", err))
	}
}

// --- Preemption ---

// PreemptGang enters the gang into coordinated drain if any task is running.
// See GangStore.PreemptGang for the full contract.
func (s *SQLiteStore) PreemptGang(ctx context.Context, gangID string, triggerJobID string) (PreemptGangResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PreemptGangResult{}, fmt.Errorf("sqlite: begin preempt tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Is any task in this gang currently running? Also find max PreemptionEpoch.
	var running int
	var maxEpoch int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE gang_id = ? AND status = 'running'`, gangID,
	).Scan(&running); err != nil {
		return PreemptGangResult{}, fmt.Errorf("sqlite: count running: %w", err)
	}
	if running == 0 {
		return PreemptGangResult{Entered: false}, nil
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(preemption_epoch), 0) FROM jobs WHERE gang_id = ?`, gangID,
	).Scan(&maxEpoch); err != nil {
		return PreemptGangResult{}, fmt.Errorf("sqlite: max epoch: %w", err)
	}

	newEpoch := maxEpoch + 1
	now := time.Now()

	// running -> preempting for non-trigger, with Attempts refund (max guard)
	// Using CASE to keep it one statement.
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'preempting',
		     preemption_epoch = ?,
		     drain_started_at = ?,
		     attempts = CASE
		         WHEN id = ? THEN attempts
		         WHEN attempts > 0 THEN attempts - 1
		         ELSE 0
		     END
		 WHERE gang_id = ? AND status = 'running'`,
		newEpoch, now, triggerJobID, gangID,
	)
	if err != nil {
		return PreemptGangResult{}, fmt.Errorf("sqlite: preempt running: %w", err)
	}
	affected, _ := res.RowsAffected()

	// reserved -> blocked (clear worker_id, reserved_at)
	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = 'blocked', worker_id = '', reserved_at = NULL
		 WHERE gang_id = ? AND status = 'reserved'`, gangID,
	); err != nil {
		return PreemptGangResult{}, fmt.Errorf("sqlite: preempt reserved: %w", err)
	}

	// pending-> blocked
	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = 'blocked' WHERE gang_id = ? AND status = 'pending'`, gangID,
	); err != nil {
		return PreemptGangResult{}, fmt.Errorf("sqlite: preempt pending: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return PreemptGangResult{}, fmt.Errorf("sqlite: commit preempt: %w", err)
	}

	return PreemptGangResult{
		Entered:      true,
		Epoch:        newEpoch,
		Transitioned: int(affected),
	}, nil
}

// MarkPreempted is worker acknowledgement: preempting -> preempted.
// See GangStore.MarkPreempted for the full contract.
func (s *SQLiteStore) MarkPreempted(ctx context.Context, jobID string, epoch int) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = 'preempted'
		 WHERE id = ? AND status = 'preempting' AND preemption_epoch = ?`,
		jobID, epoch,
	)
	if err != nil {
		return fmt.Errorf("sqlite: mark preempted: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("job %q: not preempting or epoch %d mismatch", jobID, epoch)
	}
	return nil
}

// ForceDrainPreempting is reaper timeout: preempting → preempted, no epoch check.
// See GangStore.ForceDrainPreempting for the full contract.
func (s *SQLiteStore) ForceDrainPreempting(ctx context.Context, jobID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = 'preempted' WHERE id = ? AND status = 'preempting'`, jobID,
	)
	if err != nil {
		return fmt.Errorf("sqlite: force drain: %w", err)
	}
	// Idempotent: zero rows affected is fine (already preempted or elsewhere).
	return nil
}

// CompletePreemption normalizes a drained gang to blocked or failed.
// See GangStore.CompletePreemption for the full contract.
func (s *SQLiteStore) CompletePreemption(ctx context.Context, gangID string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("sqlite: begin complete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Any task still running or preempting? If so, not drained yet.
	var inFlight int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE gang_id = ? AND status IN ('running', 'preempting')`, gangID,
	).Scan(&inFlight); err != nil {
		return false, fmt.Errorf("sqlite: count in-flight: %w", err)
	}
	if inFlight > 0 {
		return false, nil
	}

	// Exhausted retries OR already-terminal sibling -> gang fails.
	var exhausted, terminal int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE gang_id = ? AND attempts >= max_retries`, gangID,
	).Scan(&exhausted); err != nil {
		return false, fmt.Errorf("sqlite: count exhausted: %w", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE gang_id = ? AND status IN ('done', 'failed')`, gangID,
	).Scan(&terminal); err != nil {
		return false, fmt.Errorf("sqlite: count terminal: %w", err)
	}

	// Check gang exists.
	var total int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE gang_id = ?`, gangID,
	).Scan(&total); err != nil {
		return false, fmt.Errorf("sqlite: count gang: %w", err)
	}
	if total == 0 {
		return false, fmt.Errorf("gang %q: no tasks found", gangID)
	}

	target := string(StatusBlocked)
	var doneAt *time.Time
	if exhausted > 0 || terminal > 0 {
		target = string(StatusFailed)
		t := time.Now()
		doneAt = &t
	}

	// Transition preempted/blocked tasks to target; leave done/failed untouched.
	if _, err := tx.ExecContext(ctx,
		`UPDATE jobs
		 SET status = ?,
		     worker_id = '',
		     reserved_at = NULL,
		     drain_started_at = NULL,
		     done_at = CASE WHEN ? = 'failed' THEN ? ELSE done_at END
		 WHERE gang_id = ? AND status IN ('preempted', 'blocked')`,
		target, target, doneAt, gangID,
	); err != nil {
		return false, fmt.Errorf("sqlite: complete preemption: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("sqlite: commit complete: %w", err)
	}
	return true, nil
}

// SaveCheckpoint stores raw bytes on the job, epoch-guarded.
// See GangStore.SaveCheckpoint for the full contract.
func (s *SQLiteStore) SaveCheckpoint(ctx context.Context, jobID string, epoch int, data json.RawMessage) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET checkpoint = ?
		 WHERE id = ? AND status = 'preempting' AND preemption_epoch = ?`,
		string(data), jobID, epoch,
	)
	if err != nil {
		return fmt.Errorf("sqlite: save checkpoint: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("job %q: not preempting or epoch %d mismatch", jobID, epoch)
	}
	return nil
}

// GetCheckpoint returns the stored checkpoint bytes for a job, if any.
func (s *SQLiteStore) GetCheckpoint(ctx context.Context, jobID string) (json.RawMessage, bool) {
	var checkpoint sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT checkpoint FROM jobs WHERE id = ?`, jobID).Scan(&checkpoint)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		panic(fmt.Sprintf("sqlite: get checkpoint: %v", err))
	}
	if !checkpoint.Valid || checkpoint.String == "" {
		return nil, false
	}
	return json.RawMessage(checkpoint.String), true
}
