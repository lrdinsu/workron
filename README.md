# Workron

A lightweight distributed job scheduler written in Go.

---

## Overview

Workron is a distributed job scheduler that accepts jobs via a REST API and executes them across concurrent workers. It supports two deployment modes: a single-process standalone mode where the scheduler and workers share memory, and a distributed mode where the scheduler and workers run as separate binaries communicating over HTTP.

If you are curious about the architectural decisions and trade-offs behind this project, I wrote about it here:
📝 [Lynn's blog](https://lrdinsu.github.io)

---

## Features

- Submit and monitor jobs via REST API
- In-memory job queue with status tracking (`pending` → `running` → `done` / `failed`)
- Multiple concurrent workers with mutex-protected atomic job claiming
- Automatic retry on failure, re-queued up to `MaxRetries` times before marked permanently failed
- Two deployment modes: standalone (single process) or distributed (separate scheduler + worker binaries over HTTP)
- Graceful shutdown — workers finish their current job before exiting

**Planned**
- [ ] Heartbeat-based worker failure detection and job re-queuing
- [ ] Job persistence with SQLite
- [ ] Cron-style scheduling
- [ ] Job priority queue
- [ ] Web dashboard

---

## Architecture

### Standalone Mode

Everything runs in a single process. Workers access the job store directly through shared memory, protected by a mutex.

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

### Distributed Mode

The scheduler and workers run as separate binaries. Workers poll the scheduler over HTTP to claim jobs and report results. Workers can run on different machines.

```
┌─────────────────────┐         ┌──────────────────────┐
│      Scheduler      │         │       Workers        │
│                     │  HTTP   │                      │
│  REST API           │◄───────►│  Worker Process A    │
│  Job Store          │         │  Worker Process B    │
│                     │         │  Worker Process C    │
└─────────────────────┘         └──────────────────────┘
    Source of truth                   Any machine
```

Both modes use the same `JobSource` interface, so the worker code is identical regardless of whether it talks to a local store or a remote scheduler.

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

### Standalone Mode

Run the scheduler and workers in a single process:

```bash
make run-standalone
# or
go run ./cmd/scheduler --mode=standalone --port=8080 --workers=3
```

### Distributed Mode

Start the scheduler and workers separately:

```bash
# Terminal 1: start the scheduler
make run
# or
go run ./cmd/scheduler --port=8080

# Terminal 2: start remote workers
make run-worker
# or
go run ./cmd/worker --scheduler=http://localhost:8080 --workers=3
```

### CLI Flags

**Scheduler** (`cmd/scheduler`)

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `scheduler` | `scheduler` (HTTP API only) or `standalone` (API + local workers) |
| `--port` | `8080` | Port for the REST API |
| `--workers` | `3` | Number of local workers (standalone mode only) |

**Worker** (`cmd/worker`)

| Flag | Default | Description |
|------|---------|-------------|
| `--scheduler` | `http://localhost:8080` | Scheduler base URL |
| `--workers` | `3` | Number of concurrent worker goroutines |

---

## API

### Submit a job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"command": "echo hello"}'
```

Response (`201 Created`):
```json
{
  "id": "job-1",
  "command": "echo hello",
  "status": "pending",
  "created_at": "2026-03-13T12:00:00Z",
  "max_retries": 3,
  "attempts": 0
}
```

### Get job status

```bash
curl http://localhost:8080/jobs/{id}
```

### List all jobs

```bash
curl http://localhost:8080/jobs
```

### Claim next job (used by workers)

```bash
curl http://localhost:8080/jobs/next
```

Returns `200` with the claimed job, or `204 No Content` if no jobs are available.

### Report job done

```bash
curl -X POST http://localhost:8080/jobs/{id}/done
```

### Report job failed

```bash
curl -X POST http://localhost:8080/jobs/{id}/fail
```

The scheduler decides whether to retry (re-queue as `pending`) or mark as permanently `failed` based on the attempt count.

---

## Project Structure

```
workron/
├── cmd/
│   ├── scheduler/
│   │   └── main.go              # Scheduler entry point (standalone or distributed)
│   └── worker/
│       └── main.go              # Standalone worker entry point
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
│       ├── executor_test.go
│       ├── client.go            # HTTP client for talking to scheduler
│       └── client_test.go
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
| Storage | In-memory (SQLite planned) |
| External dependencies | None |

---

## Contributing

This is a personal learning project and not yet ready for production use. Feedback and suggestions are welcome — feel free to open an issue.

---

## License

MIT
