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

	// WAL mode allows concurrent reads while a write is in progress.
	// Without it, the reaper scanning jobs would block workers from claiming them.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable WAL mode: %w", err)
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
	panic("sqlite: ClaimJob not implemented yet")
}

func (s *SQLiteStore) UpdateJobStatus(id string, status JobStatus) {
	panic("sqlite: UpdateJobStatus not implemented yet")
}

func (s *SQLiteStore) ListJobs() []*Job {
	panic("sqlite: ListJobs not implemented yet")
}

func (s *SQLiteStore) ListRunningJobs() []*Job {
	panic("sqlite: ListRunningJobs not implemented yet")
}

func (s *SQLiteStore) UpdateHeartbeat(id string) {
	panic("sqlite: UpdateHeartbeat not implemented yet")
}
