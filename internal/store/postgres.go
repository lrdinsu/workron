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

func pgMigrate(ctx context.Context, pool *pgxpool.Pool) error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id             TEXT PRIMARY KEY,
		command        TEXT NOT NULL,
		status         TEXT NOT NULL DEFAULT 'pending',
		created_at     TIMESTAMPTZ NOT NULL,
		started_at     TIMESTAMPTZ,
		done_at        TIMESTAMPTZ,
		last_heartbeat TIMESTAMPTZ,
		max_retries    INTEGER DEFAULT 3,
		attempts       INTEGER DEFAULT 0,
		depends_on     JSONB DEFAULT '[]'::jsonb
	)`
	_, err := pool.Exec(ctx, schema)
	return err
}

// --- JobStore implementation ---

func (s *PostgresStore) AddJob(ctx context.Context, command string, dependsOn []string) string {
	id := generateID()
	now := time.Now()

	status := StatusPending
	if len(dependsOn) > 0 {
		status = StatusBlocked
	}

	if dependsOn == nil {
		dependsOn = []string{}
	}
	depsJSON, err := json.Marshal(dependsOn)
	if err != nil {
		panic(fmt.Sprintf("postgres: marshal depends_on: %v", err))
	}

	_, err = s.pool.Exec(ctx,
		`INSERT INTO jobs (id, command, status, created_at, max_retries, attempts, depends_on)
		 VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)`,
		id, command, string(status), now, 3, 0, string(depsJSON),
	)
	if err != nil {
		panic(fmt.Sprintf("postgres: add job: %v", err))
	}

	return id
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (*Job, bool) {
	row := s.pool.QueryRow(ctx,
		`SELECT id, command, status, created_at, started_at, done_at,
		        last_heartbeat, max_retries, attempts, depends_on
		 FROM jobs WHERE id = $1`, id,
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
		UPDATE jobs SET status = 'running', started_at = now(), attempts = attempts + 1, last_heartbeat = NULL
		FROM claimed
		WHERE jobs.id = claimed.id
		RETURNING jobs.id, jobs.command, jobs.status, jobs.created_at, jobs.started_at,
		          jobs.done_at, jobs.last_heartbeat, jobs.max_retries, jobs.attempts, jobs.depends_on`)
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
	return s.pgQueryJobs(ctx, `SELECT id, command, status, created_at, started_at, done_at,
	                              last_heartbeat, max_retries, attempts, depends_on FROM jobs`)
}

func (s *PostgresStore) ListRunningJobs(ctx context.Context) []*Job {
	return s.pgQueryJobs(ctx, `SELECT id, command, status, created_at, started_at, done_at,
	                              last_heartbeat, max_retries, attempts, depends_on
	                       FROM jobs WHERE status = 'running'`)
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
func (s *PostgresStore) SendHeartbeat(ctx context.Context, id string) error {
	s.UpdateHeartbeat(ctx, id)
	return nil
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

// pgScanJob scans a single pgx row into a Job.
// Column order must match: id, command, status, created_at, started_at,
// done_at, last_heartbeat, max_retries, attempts, depends_on.
func pgScanJob(row pgx.Row) (*Job, bool) {
	var j Job
	var status string
	var depsJSON []byte

	err := row.Scan(
		&j.ID, &j.Command, &status, &j.CreatedAt,
		&j.StartedAt, &j.DoneAt, &j.LastHeartbeat,
		&j.MaxRetries, &j.Attempts, &depsJSON,
	)
	if err == pgx.ErrNoRows {
		return nil, false
	}
	if err != nil {
		panic(fmt.Sprintf("postgres: scan job: %v", err))
	}

	j.Status = JobStatus(status)
	if err := json.Unmarshal(depsJSON, &j.DependsOn); err != nil {
		panic(fmt.Sprintf("postgres: unmarshal depends_on: %v", err))
	}

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
		var depsJSON []byte

		if err := rows.Scan(
			&j.ID, &j.Command, &status, &j.CreatedAt,
			&j.StartedAt, &j.DoneAt, &j.LastHeartbeat,
			&j.MaxRetries, &j.Attempts, &depsJSON,
		); err != nil {
			panic(fmt.Sprintf("postgres: scan job row: %v", err))
		}

		j.Status = JobStatus(status)
		if err := json.Unmarshal(depsJSON, &j.DependsOn); err != nil {
			panic(fmt.Sprintf("postgres: unmarshal depends_on: %v", err))
		}
		jobs = append(jobs, &j)
	}

	if len(jobs) == 0 {
		return []*Job{}
	}
	return jobs
}
