.PHONY: build test test-v test-race clean run-scheduler run-scheduler-sqlite run-standalone run-standalone-sqlite run-worker

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
	go run ./cmd/scheduler --port=8080 --db=workron.db

# ---------- All-in-one (scheduler + local workers in one process) ----------

# All-in-one with in-memory store
run-standalone:
	go run ./cmd/scheduler --mode=standalone --port=8080 --workers=3

# All-in-one with SQLite persistence
run-standalone-sqlite:
	go run ./cmd/scheduler --mode=standalone --port=8080 --workers=3 --db=workron.db

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

# ---------- Clean ----------

# Remove built binaries and database files
clean:
	rm -rf bin/
	rm -f workron.db workron.db-wal workron.db-shm