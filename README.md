# Workron

A lightweight distributed job scheduler written in Go.

---

## Overview

Workron is a distributed job scheduler that accepts jobs via a REST API and executes them across concurrent workers. It supports two deployment modes: a single-process standalone mode where the scheduler and workers share memory, and a distributed mode where the scheduler and workers run as separate binaries communicating over HTTP.

Jobs are persisted to SQLite, so in-flight and completed work survives a full scheduler restart. An in-memory store is also available for development and testing.

Workers send periodic heartbeats while processing jobs. A background reaper on the scheduler detects stale heartbeats and re-queues orphaned jobs, ensuring no work is silently lost when a worker crashes.

If you are curious about the design decisions and trade-offs behind this project, I wrote about the journey here:

- 📝 [Before the Code: Designing a Distributed Job Scheduler in Go](https://lrdinsu.github.io/posts/designing-distributed-job-scheduler-go/)
- 📝 [Building the Concurrent Monolith: Atomic Job Claiming in Go](https://lrdinsu.github.io/posts/building-concurrent-monolith-atomic-job-claiming-go/)
- 📝 [Splitting and Surviving Failures: HTTP Workers and Heartbeat Detection in Go](https://lrdinsu.github.io/posts/splitting-and-surviving-failures-workron/)

---

## Features

- Submit and monitor jobs via REST API
- Pluggable storage: in-memory store for development, SQLite for persistence across restarts
- Multiple concurrent workers with atomic job claiming (mutex in memory, `UPDATE ... RETURNING` in SQLite)
- Automatic retry on failure, re-queued up to `MaxRetries` times before marked permanently failed
- Two deployment modes: standalone (single process) or distributed (separate scheduler + worker binaries over HTTP)
- Heartbeat-based failure detection — workers send heartbeats every 5 seconds, scheduler re-queues jobs with stale or missing heartbeats after 30 seconds
- Graceful shutdown — workers finish their current job before exiting

**Planned**
- [ ] DAG-based job dependencies
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
│   (HTTP)       (memory/SQLite)  (goroutines) │
│                  [pending]       Worker 1    │
│                  [running]       Worker 2    │
│                  [done]          Worker 3    │
│                                              │
│   Reaper (background goroutine)              │
│   └─ scans running jobs every 10s            │
│   └─ re-queues jobs with stale heartbeats    │
└──────────────────────────────────────────────┘
```

### Distributed Mode

The scheduler and workers run as separate binaries. Workers poll the scheduler over HTTP to claim jobs, send heartbeats, and report results. Workers can run on different machines.

```
┌─────────────────────┐         ┌──────────────────────┐
│      Scheduler      │         │       Workers        │
│                     │  HTTP   │                      │
│  REST API           │◄───────►│  Worker Process A    │
│  Job Store          │         │  Worker Process B    │
│  (memory/SQLite)    │         │  Worker Process C    │
│  Reaper             │         │                      │
│                     │         │  Sends heartbeats    │
│  Detects dead       │         │  every 5s while      │
│  workers via stale  │         │  processing a job    │
│  heartbeats         │         │                      │
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
# In-memory store
make run-standalone

# With SQLite persistence
make run-standalone-sqlite
```

### Distributed Mode

Start the scheduler and workers separately:

```bash
# Terminal 1: start the scheduler (with SQLite)
make run-scheduler-sqlite

# Terminal 2: start remote workers
make run-worker

# Terminal 3: submit jobs
curl -X POST http://localhost:8080/jobs -d '{"command":"echo hello"}'
```

### CLI Flags

**Scheduler** (`cmd/scheduler`)

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `scheduler` | `scheduler` (HTTP API only) or `standalone` (API + local workers) |
| `--port` | `8080` | Port for the REST API |
| `--workers` | `3` | Number of local workers (standalone mode only) |
| `--db` | `""` | Path to SQLite database file (empty = in-memory store) |

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

### Send heartbeat

```bash
curl -X POST http://localhost:8080/jobs/{id}/heartbeat
```

Workers call this automatically every 5 seconds while processing a job. Returns `200` on success, `404` if job not found, `409` if job is not running.

---

## Failure Detection

When a worker crashes mid-job, the scheduler detects it through missing heartbeats. A background reaper goroutine runs every 10 seconds and checks all running jobs:

- If a job's last heartbeat is older than 30 seconds (or was never set), the worker is assumed dead
- If the job has retries remaining, it is re-queued as `pending` for another worker to pick up
- If retries are exhausted, the job is marked as permanently `failed`

This ensures no job gets stuck in `running` forever, even if a worker process is killed without warning.

---

## Persistence

Workron supports two storage backends, selectable at startup via the `--db` flag:

**In-memory store** (default) — jobs live only as long as the process runs. Fast, zero dependencies, ideal for development and testing.

**SQLite store** (`--db=workron.db`) — jobs persist to a single file on disk. The scheduler can crash and restart without losing any job state. Uses WAL mode for write performance and a single-connection pool to avoid SQLite's write lock contention. The first external dependency (`modernc.org/sqlite`, a pure Go port) — no C compiler or Docker required.

Both backends implement the same `JobStore` interface. The server, workers, and reaper are unaware of which store they're using.

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
│   │   ├── memory_test.go
│   │   ├── sqlite.go            # SQLite store implementation
│   │   ├── sqlite_test.go
│   │   └── store_test.go        # Shared compliance tests for all store backends
│   ├── scheduler/
│   │   ├── server.go            # HTTP handlers
│   │   ├── server_test.go
│   │   ├── reaper.go            # Background heartbeat timeout checker
│   │   └── reaper_test.go
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
├── go.sum
└── README.md
```

---

## Tech Stack

| Component | Choice |
|-----------|--------|
| Language | Go 1.22+ |
| HTTP | `net/http` (stdlib only) |
| Job execution | `os/exec` (stdlib) |
| Storage | In-memory or SQLite (`modernc.org/sqlite`) |
| External dependencies | `modernc.org/sqlite` (pure Go, no CGo) |

---

## Contributing

This is a personal learning project and not yet ready for production use. Feedback and suggestions are welcome — feel free to open an issue.

---

## License

MIT
