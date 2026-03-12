# Workron

A lightweight distributed job scheduler written in Go.

---

## Overview

Workron is a distributed job scheduler that allows you to submit jobs via a REST API and execute them across multiple concurrent workers. It is designed with simplicity and fault tolerance in mind, and built incrementally — starting from a single-process in-memory scheduler and evolving toward a fully distributed, fault-tolerant system.

If you are curious about the architectural decisions and trade-offs behind this project, I wrote about it here:
📝 [Lynn's blog](https://lrdinsu.github.io/posts/designing-distributed-job-scheduler-go/)

---

## Features

**Currently available**
- Submit jobs via REST API
- In-memory job queue with status tracking (`pending` → `running` → `done` / `failed`)
- Multiple concurrent workers with mutex-protected atomic job claiming
- Job retry on failure, re-queued up to `MaxRetries` times before marked failed
- List and query job status via API
- Graceful shutdown, workers finish current job before exiting

**Planned**
- [ ] Separate scheduler and worker processes communicating over HTTP
- [ ] Heartbeat-based worker failure detection and job re-queuing
- [ ] Job persistence with SQLite / PostgreSQL
- [ ] Cron-style scheduling
- [ ] Job priority queue
- [ ] Web dashboard

---

## Architecture

```
┌──────────────────────────────────────────────┐
│               Single Go Process              │
│                                              │
│   REST API  ──►  Job Store  ◄──  Workers     │
│   (HTTP)        (in-memory)     (goroutines) │
│                  [pending]       Worker 1    │
│                  [running]       Worker 2    │
│                  [done]          Worker 3    │
└──────────────────────────────────────────────┘
```

Everything currently runs in a single process. The scheduler exposes a REST API to accept jobs, stores them in a mutex-protected in-memory map, and dispatches them to a configurable pool of worker goroutines. The mutex ensures that two workers can never claim the same job simultaneously.

In a later phase the architecture will split into separate scheduler and worker binaries communicating over HTTP, allowing workers to run on different machines.

---

## Getting Started

### Prerequisites

- Go 1.22+

### Installation

```bash
git clone https://github.com/lrdinsu/workron.git
cd workron
go mod tidy
```

### Run

```bash
make run
# or
go run ./cmd/scheduler --workers=3 --port=8080
```

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | `3` | Number of concurrent worker goroutines |
| `--port` | `8080` | Port for the REST API |

---

## API

### Submit a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"command": "echo hello"}'
```

Response:
```json
{
  "id": "job-1718000000000",
  "status": "pending"
}
```

### Get job status

```bash
curl http://localhost:8080/jobs/{id}
```

Response:
```json
{
  "id": "job-1718000000000",
  "command": "echo hello",
  "status": "done",
  "created_at": "2024-06-10T12:00:00Z",
  "started_at": "2024-06-10T12:00:01Z",
  "done_at": "2024-06-10T12:00:01Z"
}
```

### List all jobs

```bash
curl http://localhost:8080/jobs
```

---

## Project Structure

```
workron/
├── cmd/
│   └── scheduler/
│       └── main.go              # Entry point, wires everything together
├── internal/
│   ├── store/
│   │   ├── store.go             # JobStore interface, Job struct, JobStatus
│   │   ├── memory.go            # In-memory store implementation
│   │   └── memory_test.go
│   ├── scheduler/
│   │   ├── server.go            # HTTP handlers
│   │   └── server_test.go
│   └── worker/
│       ├── worker.go            # Poll and execute loop
│       ├── worker_test.go
│       ├── executor.go          # Runs shell commands via os/exec
│       └── executor_test.go
├── Makefile
├── .gitignore
├── go.mod
└── README.md
```

---

## Tech Stack

| Component | Choice |
|-----------|--------|
| Language | Go 1.22+ |
| HTTP | `net/http` (stdlib only) |
| Job execution | `os/exec` (stdlib) |
| Storage | In-memory (SQLite planned for Phase 5) |
| Zero external dependencies | ✅ |


---

## Contributing

This is a personal learning project and not yet ready for production use. Feedback and suggestions are welcome, feel free to open an issue.

---

## License

MIT
