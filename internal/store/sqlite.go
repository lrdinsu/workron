package store

import (
	"database/sql"
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
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id             TEXT PRIMARY KEY,
		command        TEXT NOT NULL,
		status         TEXT NOT NULL DEFAULT 'pending',
		created_at     DATETIME NOT NULL,
		started_at     DATETIME,
		done_at        DATETIME,
		last_heartbeat DATETIME,
		max_retries    INTEGER DEFAULT 3,
		attempts       INTEGER DEFAULT 0
	)`
	_, err := db.Exec(schema)
	return err
}

// JobStore implementation

func (s *SQLiteStore) AddJob(command string) string {
	id := generateID()
	now := time.Now()

	_, err := s.db.Exec(
		`INSERT INTO jobs (id, command, status, created_at, max_retries, attempts)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		id, command, string(StatusPending), now, 3, 0,
	)
	if err != nil {
		// Only fails on disk full or DB corruption, surface is loudly
		// rather than silently dropping a job.
		panic(fmt.Sprintf("sqlite: add job: %v", err))
	}

	return id
}

func (s *SQLiteStore) GetJob(id string) (*Job, bool) {
	row := s.db.QueryRow(
		`SELECT id, command, status, created_at, started_at, done_at,
		        last_heartbeat, max_retries, attempts
		 FROM jobs WHERE id = ?`, id,
	)
	return scanJob(row)
}

// scanJob scans a single database row into a Job.
// Column order must match: id, command, status, created_at, started_at,
// done_at, last_heartbeat, max_retries, attempts.
// This same order is used by GetJob's SELECT and ClaimJob's RETURNING *.
func scanJob(row *sql.Row) (*Job, bool) {
	var j Job
	var status string
	var startedAt, doneAt, lastHeartbeat sql.NullTime

	err := row.Scan(
		&j.ID, &j.Command, &status, &j.CreatedAt,
		&startedAt, &doneAt, &lastHeartbeat,
		&j.MaxRetries, &j.Attempts,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false
	}
	if err != nil {
		panic(fmt.Sprintf("sqlite: scan job: %v", err))
	}

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

	return &j, true
}

func (s *SQLiteStore) ClaimJob() (*Job, bool) {
	row := s.db.QueryRow(`
		UPDATE jobs SET status = 'running', started_at = ?, attempts = attempts + 1, last_heartbeat = NULL
		WHERE id = (SELECT id FROM jobs WHERE status = 'pending' LIMIT 1)
		RETURNING id, command, status, created_at, started_at, done_at,
		          last_heartbeat, max_retries, attempts`,
		time.Now(),
	)
	return scanJob(row)
}

func (s *SQLiteStore) UpdateJobStatus(id string, status JobStatus) {
	var doneAt *time.Time
	if status == StatusDone || status == StatusFailed {
		t := time.Now()
		doneAt = &t
	}

	_, err := s.db.Exec(
		`UPDATE jobs SET status = ?, done_at = ? WHERE id = ?`,
		string(status), doneAt, id,
	)
	if err != nil {
		panic(fmt.Sprintf("sqlite: update job status: %v", err))
	}
}

func (s *SQLiteStore) ListJobs() []*Job {
	return s.queryJobs(`SELECT id, command, status, created_at, started_at, done_at,
	                            last_heartbeat, max_retries, attempts FROM jobs`)
}

func (s *SQLiteStore) ListRunningJobs() []*Job {
	return s.queryJobs(`SELECT id, command, status, created_at, started_at, done_at,
	                            last_heartbeat, max_retries, attempts
	                     FROM jobs WHERE status = 'running'`)
}

func (s *SQLiteStore) UpdateHeartbeat(id string) {
	_, err := s.db.Exec(
		`UPDATE jobs SET last_heartbeat = ? WHERE id = ?`,
		time.Now(), id,
	)
	if err != nil {
		panic(fmt.Sprintf("sqlite: update heartbeat: %v", err))
	}
}

// SendHeartbeat wraps UpdateHeartbeat to satisfy the worker.JobSource interface.
func (s *SQLiteStore) SendHeartbeat(id string) error {
	s.UpdateHeartbeat(id)
	return nil
}

// queryJobs runs a SELECT query and scans all result rows into Jobs.
func (s *SQLiteStore) queryJobs(query string, args ...any) []*Job {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		panic(fmt.Sprintf("sqlite: query jobs: %v", err))
	}
	defer func() { _ = rows.Close() }()

	var jobs []*Job
	for rows.Next() {
		var j Job
		var status string
		var startedAt, doneAt, lastHeartbeat sql.NullTime

		if err := rows.Scan(
			&j.ID, &j.Command, &status, &j.CreatedAt,
			&startedAt, &doneAt, &lastHeartbeat,
			&j.MaxRetries, &j.Attempts,
		); err != nil {
			panic(fmt.Sprintf("sqlite: scan job row: %v", err))
		}

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
		jobs = append(jobs, &j)
	}

	if len(jobs) == 0 {
		return []*Job{} // return an empty slice (not nil slice)
	}
	return jobs
}
