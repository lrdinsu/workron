-include .env
export

.PHONY: build test test-v test-race test-postgres clean \
       run-scheduler run-scheduler-sqlite run-scheduler-postgres \
       run-standalone run-standalone-sqlite run-standalone-postgres \
       run-worker run-postgres stop-postgres

# ---------- Build ----------
build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/worker ./cmd/worker

# ---------- Scheduler ----------

# Scheduler with in-memory store (remote workers connect via run-worker)
run-scheduler:
	go run ./cmd/scheduler --port=8080

# Scheduler with SQLite persistence (remote workers connect via run-worker)
run-scheduler-sqlite:
	go run ./cmd/scheduler --port=8080 --db-driver=sqlite --db-url=workron.db

# Guard: fail with a helpful message if PG_URL is not set.
_require-pg-url:
ifndef PG_URL
	$(error PG_URL is not set. Copy .env.example to .env and fill in your credentials)
endif

# Scheduler with PostgreSQL persistence (requires: make run-postgres)
run-scheduler-postgres: _require-pg-url
	go run ./cmd/scheduler --port=8080 --db-driver=postgres --db-url=$(PG_URL)

# ---------- All-in-one (scheduler + local workers in one process) ----------

# All-in-one with in-memory store
run-standalone:
	go run ./cmd/scheduler --mode=standalone --port=8080 --workers=3

# All-in-one with SQLite persistence
run-standalone-sqlite:
	go run ./cmd/scheduler --mode=standalone --port=8080 --workers=3 --db-driver=sqlite --db-url=workron.db

# All-in-one with PostgreSQL persistence (requires: make run-postgres)
run-standalone-postgres: _require-pg-url
	go run ./cmd/scheduler --mode=standalone --port=8080 --workers=3 --db-driver=postgres --db-url=$(PG_URL)

# ---------- Remote worker ----------

# Start a worker process that connects to a running scheduler
run-worker:
	go run ./cmd/worker --scheduler=http://localhost:8080 --workers=3

# ---------- Test ----------

# Run all tests
test:
	go test ./...

# Run tests with verbose output
test-v:
	go test -v ./...

# Run tests with race detector
test-race:
	go test -race ./...

# Run PostgreSQL compliance tests (requires: make run-postgres)
test-postgres: _require-pg-url
	WORKRON_PG_URL=$(PG_URL) go test -tags postgres -v ./internal/store/ -run TestPostgres

# ---------- PostgreSQL ----------

# Start local PostgreSQL via Docker Compose
run-postgres:
	docker compose up -d postgres

# Stop local PostgreSQL
stop-postgres:
	docker compose down

# ---------- Clean ----------

# Remove built binaries and database files
clean:
	rm -rf bin/
	rm -f workron.db workron.db-wal workron.db-shm