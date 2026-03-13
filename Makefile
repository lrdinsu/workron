.PHONY: build run test clean

# Build both binaries
build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/worker ./cmd/worker

# Run the scheduler locally (default 3 workers on port 8080)
run:
	go run ./cmd/scheduler --port=8080 --workers=3

# Run a standalone worker process pointing at the local scheduler
run-worker:
	go run ./cmd/worker --scheduler=http://localhost:8080 --workers=3

# Run all tests
test:
	go test ./...

# Run tests with verbose output
test-v:
	go test -v ./...

# Run tests with race detector (important for concurrent code like yours)
test-race:
	go test -race ./...

# Remove built binaries
clean:
	rm -rf bin/
