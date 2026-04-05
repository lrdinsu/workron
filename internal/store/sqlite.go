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
		preemption_epoch  INTEGER DEFAULT 0
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
	checkpoint, outputs, reservation_epoch, preemption_epoch`

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
func (s *SQLiteStore) SendHeartbeat(ctx context.Context, id string) (string, error) {
	s.UpdateHeartbeat(ctx, id)
	return "", nil
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
	reservationEpoch, preemptionEpoch sql.NullInt64,
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
	if preemptionEpoch.Valid {
		j.PreemptionEpoch = int(preemptionEpoch.Int64)
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

	err := row.Scan(
		&j.ID, &j.Command, &status, &j.CreatedAt,
		&startedAt, &doneAt, &lastHeartbeat,
		&j.MaxRetries, &j.Attempts, &depsJSON,
		&resourcesJSON, &workerID, &priority, &queueName,
		&gangID, &gangSize, &gangIndex,
		&checkpoint, &outputs, &reservationEpoch, &preemptionEpoch,
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
		reservationEpoch, preemptionEpoch)

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

		if err := rows.Scan(
			&j.ID, &j.Command, &status, &j.CreatedAt,
			&startedAt, &doneAt, &lastHeartbeat,
			&j.MaxRetries, &j.Attempts, &depsJSON,
			&resourcesJSON, &workerID, &priority, &queueName,
			&gangID, &gangSize, &gangIndex,
			&checkpoint, &outputs, &reservationEpoch, &preemptionEpoch,
		); err != nil {
			panic(fmt.Sprintf("sqlite: scan job row: %v", err))
		}

		populateJobFromNullables(&j, status, startedAt, doneAt, lastHeartbeat,
			depsJSON, resourcesJSON, workerID, priority, queueName,
			gangID, gangSize, gangIndex, checkpoint, outputs,
			reservationEpoch, preemptionEpoch)

		jobs = append(jobs, &j)
	}

	if len(jobs) == 0 {
		return []*Job{} // return an empty slice (not nil slice)
	}
	return jobs
}
