package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore implements JobStore backed by a PostgreSQL database.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore connects to a PostgreSQL database, runs schema migrations,
// and returns a ready-to-use store with a connection pool of the given size.
func NewPostgresStore(ctx context.Context, connString string, poolSize int) (*PostgresStore, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}
	if poolSize > 0 {
		config.MaxConns = int32(poolSize)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	if err := pgMigrate(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return &PostgresStore{pool: pool}, nil
}

// Close shuts down the connection pool.
func (s *PostgresStore) Close() {
	s.pool.Close()
}

// WithReaperLock uses a PostgreSQL transaction-scoped advisory lock to ensure
// only one scheduler instance runs the reaper at a time. The lock is held for
// the duration of fn and automatically released when the transaction commits.
// Returns (true, nil) if the lock was acquired and fn executed,
// (false, nil) if another instance holds the lock.
func (s *PostgresStore) WithReaperLock(ctx context.Context, fn func(ctx context.Context)) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin reaper lock tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var acquired bool
	err = tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1)", int64(1)).Scan(&acquired)
	if err != nil {
		return false, fmt.Errorf("try advisory lock: %w", err)
	}

	if !acquired {
		return false, nil
	}

	fn(ctx)

	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("commit reaper lock tx: %w", err)
	}
	return true, nil
}

func pgMigrate(ctx context.Context, pool *pgxpool.Pool) error {
	jobsSchema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id                TEXT PRIMARY KEY,
		command           TEXT NOT NULL,
		status            TEXT NOT NULL DEFAULT 'pending',
		created_at        TIMESTAMPTZ NOT NULL,
		started_at        TIMESTAMPTZ,
		done_at           TIMESTAMPTZ,
		last_heartbeat    TIMESTAMPTZ,
		max_retries       INTEGER DEFAULT 3,
		attempts          INTEGER DEFAULT 0,
		depends_on        JSONB   DEFAULT '[]'::jsonb,
		resources         JSONB,
		worker_id         TEXT    DEFAULT '',
		priority          INTEGER DEFAULT 0,
		queue_name        TEXT    DEFAULT '',
		gang_id           TEXT    DEFAULT '',
		gang_size         INTEGER DEFAULT 0,
		gang_index        INTEGER DEFAULT 0,
		checkpoint        JSONB,
		outputs           JSONB,
		reservation_epoch INTEGER DEFAULT 0,
		preemption_epoch  INTEGER DEFAULT 0
	)`
	if _, err := pool.Exec(ctx, jobsSchema); err != nil {
		return err
	}

	workersSchema := `
	CREATE TABLE IF NOT EXISTS workers (
		id             TEXT PRIMARY KEY,
		exec_addr      TEXT        NOT NULL,
		resources      JSONB       NOT NULL DEFAULT '{}'::jsonb,
		tags           JSONB       NOT NULL DEFAULT '[]'::jsonb,
		status         TEXT        NOT NULL DEFAULT 'active',
		last_heartbeat TIMESTAMPTZ NOT NULL,
		registered_at  TIMESTAMPTZ NOT NULL
	)`
	_, err := pool.Exec(ctx, workersSchema)
	return err
}

// --- JobStore implementation ---

// pgJobColumns is the canonical column list for all job SELECT queries.
const pgJobColumns = `id, command, status, created_at, started_at, done_at,
	last_heartbeat, max_retries, attempts, depends_on,
	resources, worker_id, priority, queue_name,
	gang_id, gang_size, gang_index,
	checkpoint, outputs, reservation_epoch, preemption_epoch`

func (s *PostgresStore) AddJob(ctx context.Context, params AddJobParams) string {
	id := generateID()
	now := time.Now()

	status := StatusPending
	if len(params.DependsOn) > 0 {
		status = StatusBlocked
	}

	dependsOn := params.DependsOn
	if dependsOn == nil {
		dependsOn = []string{}
	}
	depsJSON, err := json.Marshal(dependsOn)
	if err != nil {
		panic(fmt.Sprintf("postgres: marshal depends_on: %v", err))
	}

	var resourcesJSON []byte
	if params.Resources != nil {
		resourcesJSON, err = json.Marshal(params.Resources)
		if err != nil {
			panic(fmt.Sprintf("postgres: marshal resources: %v", err))
		}
	}

	_, err = s.pool.Exec(ctx,
		`INSERT INTO jobs (id, command, status, created_at, max_retries, attempts, depends_on,
		                    resources, priority, queue_name, gang_id, gang_size, gang_index)
		 VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13)`,
		id, params.Command, string(status), now, 3, 0, string(depsJSON),
		resourcesJSON, params.Priority, params.QueueName,
		params.GangID, params.GangSize, params.GangIndex,
	)
	if err != nil {
		panic(fmt.Sprintf("postgres: add job: %v", err))
	}

	return id
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (*Job, bool) {
	row := s.pool.QueryRow(ctx,
		`SELECT `+pgJobColumns+` FROM jobs WHERE id = $1`, id,
	)
	return pgScanJob(row)
}

func (s *PostgresStore) ClaimJob(ctx context.Context) (*Job, bool) {
	row := s.pool.QueryRow(ctx, `
		WITH claimed AS (
			SELECT id FROM jobs
			WHERE status = 'pending'
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs SET status = 'running', started_at = $1, attempts = attempts + 1, last_heartbeat = NULL
		FROM claimed
		WHERE jobs.id = claimed.id
		RETURNING jobs.id, jobs.command, jobs.status, jobs.created_at, jobs.started_at,
		          jobs.done_at, jobs.last_heartbeat, jobs.max_retries, jobs.attempts, jobs.depends_on,
		          jobs.resources, jobs.worker_id, jobs.priority, jobs.queue_name,
		          jobs.gang_id, jobs.gang_size, jobs.gang_index,
		          jobs.checkpoint, jobs.outputs, jobs.reservation_epoch, jobs.preemption_epoch`,
		time.Now(),
	)
	return pgScanJob(row)
}

func (s *PostgresStore) UpdateJobStatus(ctx context.Context, id string, status JobStatus) {
	var doneAt *time.Time
	if status == StatusDone || status == StatusFailed {
		t := time.Now()
		doneAt = &t
	}

	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = $1, done_at = $2 WHERE id = $3`,
		string(status), doneAt, id,
	)
	if err != nil {
		panic(fmt.Sprintf("postgres: update job status: %v", err))
	}
}

func (s *PostgresStore) ListJobs(ctx context.Context) []*Job {
	return s.pgQueryJobs(ctx, `SELECT `+pgJobColumns+` FROM jobs`)
}

func (s *PostgresStore) ListRunningJobs(ctx context.Context) []*Job {
	return s.pgQueryJobs(ctx, `SELECT `+pgJobColumns+` FROM jobs WHERE status = 'running'`)
}

func (s *PostgresStore) UpdateHeartbeat(ctx context.Context, id string) {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET last_heartbeat = $1 WHERE id = $2`,
		time.Now(), id,
	)
	if err != nil {
		panic(fmt.Sprintf("postgres: update heartbeat: %v", err))
	}
}

// SendHeartbeat wraps UpdateHeartbeat to satisfy the worker.JobSource interface.
func (s *PostgresStore) SendHeartbeat(ctx context.Context, id string) (string, error) {
	s.UpdateHeartbeat(ctx, id)
	return "", nil
}

// UnblockReady transitions blocked jobs to pending when all their
// dependencies have completed. Uses jsonb_array_elements_text to check
// each element of the depends_on JSONB array against the jobs table.
func (s *PostgresStore) UnblockReady(ctx context.Context) {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'pending'
		WHERE status = 'blocked'
		AND NOT EXISTS (
			SELECT 1 FROM jsonb_array_elements_text(jobs.depends_on) AS dep
			WHERE dep NOT IN (SELECT id FROM jobs WHERE status = 'done')
		)`)
	if err != nil {
		panic(fmt.Sprintf("postgres: unblock ready: %v", err))
	}
}

// --- Scan helpers ---

// pgPopulateJob fills a Job's JSON and nullable fields after scanning raw values.
func pgPopulateJob(j *Job, status string, depsJSON, resourcesJSON, checkpointJSON, outputsJSON []byte) {
	j.Status = JobStatus(status)
	if err := json.Unmarshal(depsJSON, &j.DependsOn); err != nil {
		panic(fmt.Sprintf("postgres: unmarshal depends_on: %v", err))
	}
	if len(resourcesJSON) > 0 {
		var r ResourceSpec
		if err := json.Unmarshal(resourcesJSON, &r); err != nil {
			panic(fmt.Sprintf("postgres: unmarshal resources: %v", err))
		}
		j.Resources = &r
	}
	if len(checkpointJSON) > 0 {
		j.Checkpoint = json.RawMessage(checkpointJSON)
	}
	if len(outputsJSON) > 0 {
		j.Outputs = json.RawMessage(outputsJSON)
	}
}

// pgScanJob scans a single pgx row into a Job.
// Column order must match pgJobColumns.
func pgScanJob(row pgx.Row) (*Job, bool) {
	var j Job
	var status string
	var depsJSON, resourcesJSON, checkpointJSON, outputsJSON []byte

	err := row.Scan(
		&j.ID, &j.Command, &status, &j.CreatedAt,
		&j.StartedAt, &j.DoneAt, &j.LastHeartbeat,
		&j.MaxRetries, &j.Attempts, &depsJSON,
		&resourcesJSON, &j.WorkerID, &j.Priority, &j.QueueName,
		&j.GangID, &j.GangSize, &j.GangIndex,
		&checkpointJSON, &outputsJSON, &j.ReservationEpoch, &j.PreemptionEpoch,
	)
	if err == pgx.ErrNoRows {
		return nil, false
	}
	if err != nil {
		panic(fmt.Sprintf("postgres: scan job: %v", err))
	}

	pgPopulateJob(&j, status, depsJSON, resourcesJSON, checkpointJSON, outputsJSON)
	return &j, true
}

// pgQueryJobs runs a SELECT query and scans all result rows into Jobs.
func (s *PostgresStore) pgQueryJobs(ctx context.Context, query string, args ...any) []*Job {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		panic(fmt.Sprintf("postgres: query jobs: %v", err))
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		var j Job
		var status string
		var depsJSON, resourcesJSON, checkpointJSON, outputsJSON []byte

		if err := rows.Scan(
			&j.ID, &j.Command, &status, &j.CreatedAt,
			&j.StartedAt, &j.DoneAt, &j.LastHeartbeat,
			&j.MaxRetries, &j.Attempts, &depsJSON,
			&resourcesJSON, &j.WorkerID, &j.Priority, &j.QueueName,
			&j.GangID, &j.GangSize, &j.GangIndex,
			&checkpointJSON, &outputsJSON, &j.ReservationEpoch, &j.PreemptionEpoch,
		); err != nil {
			panic(fmt.Sprintf("postgres: scan job row: %v", err))
		}

		pgPopulateJob(&j, status, depsJSON, resourcesJSON, checkpointJSON, outputsJSON)
		jobs = append(jobs, &j)
	}

	if len(jobs) == 0 {
		return []*Job{}
	}
	return jobs
}
