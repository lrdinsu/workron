# Workron

A lightweight distributed job scheduler written in Go.

---

## Overview

Workron is a distributed job scheduler that accepts jobs via a REST API and executes them across concurrent workers. It supports two deployment modes: a single-process standalone mode where the scheduler and workers share memory, and a distributed mode where the scheduler and workers run as separate binaries communicating over HTTP.

Jobs can declare dependencies on other jobs, forming a DAG (directed acyclic graph). The scheduler validates the dependency graph at submission time, rejecting cycles, and only makes downstream jobs available for execution once all their upstream dependencies have completed.

Jobs are persisted to SQLite or PostgreSQL, so in-flight and completed work survives a full scheduler restart. PostgreSQL uses `FOR UPDATE SKIP LOCKED` for safe concurrent job claiming across multiple connections, preparing for multi-scheduler deployments. An in-memory store is also available for development and testing.

Workers send periodic heartbeats while processing jobs. A background reaper on the scheduler detects stale heartbeats and re-queues orphaned jobs, ensuring no work is silently lost when a worker crashes.

If you are curious about the design decisions and trade-offs behind this project, I wrote about the journey here:

- 📝 [Before the Code: Designing a Distributed Job Scheduler in Go](https://lrdinsu.github.io/posts/designing-distributed-job-scheduler-go/)
- 📝 [Building the Concurrent Monolith: Atomic Job Claiming in Go](https://lrdinsu.github.io/posts/building-concurrent-monolith-atomic-job-claiming-go/)
- 📝 [Splitting and Surviving Failures: HTTP Workers and Heartbeat Detection in Go](https://lrdinsu.github.io/posts/splitting-and-surviving-failures-workron/)
- 📝 [Surviving the Crash: Adding SQLite Persistence Without Touching Business Logic](https://lrdinsu.github.io/posts/persisting-jobs-with-sqlite-workron/)

---

## Features

- Submit and monitor jobs via REST API
- DAG-based job dependencies: jobs declare upstream dependencies, validated at submission time with cycle detection; downstream jobs execute only after all dependencies are complete
- Pluggable storage: in-memory for development, SQLite for single-node persistence, PostgreSQL for concurrent multi-connection access
- Multiple concurrent workers with atomic job claiming (mutex in memory, `UPDATE ... RETURNING` in SQLite, `FOR UPDATE SKIP LOCKED` in PostgreSQL)
- Automatic retry on failure, re-queued up to `MaxRetries` times before marked permanently failed
- Two deployment modes: standalone (single process) or distributed (separate scheduler + worker binaries over HTTP)
- Heartbeat-based failure detection: workers send heartbeats every 5 seconds, scheduler re-queues jobs with stale or missing heartbeats after 30 seconds
- Graceful shutdown: workers finish their current job before exiting
- Structured JSON logging with `log/slog`: typed key-value fields, log levels, logger injection with `With` for component identity
- Prometheus metrics: counters for job lifecycle events, histograms for execution duration and queue wait, gauges for current queue state via custom collector
- Request ID tracing: UUID per HTTP request, `X-Request-ID` response header, request-scoped logger via context
- Multi-scheduler coordination: multiple scheduler instances can share one PostgreSQL database safely. Job claiming uses `FOR UPDATE SKIP LOCKED`, reaper uses a transaction-scoped advisory lock (`pg_try_advisory_xact_lock`) so only one instance runs heartbeat timeout detection at a time. If the leader crashes, the lock auto-releases and another instance picks up.
- Health endpoint: `GET /health` returns instance ID, uptime, and status for load balancer checks and debugging multi-scheduler deployments

**Planned — ML Infrastructure**
- [ ] Job resource requirements (GPU, VRAM, CPU, memory) and job output tracking
- [ ] Worker registration with resource capacity reporting
- [ ] Resource-aware scheduling with bin-packing placement
- [ ] ML pipeline demo (DAG with mixed CPU/GPU steps and artifact references)

**Planned — Execution Semantics**
- [ ] Job cancellation (with cascading cancel for DAGs)
- [ ] Configurable retry backoff and per-job timeouts
- [ ] Backpressure and concurrency control

---

## Architecture

### Job Lifecycle

```
Submit ──► pending ──► running ──► done
               ▲          │
               └── retry ─┘  (if attempts < max_retries)
                          │
                          ▼
                       failed   (if retries exhausted)

With dependencies:

Submit ──► blocked ──► pending ──► running ──► done
            (waits for all         (normal lifecycle)
             deps to be done)
```

### Standalone Mode

Everything runs in a single process. Workers access the job store directly through shared memory, protected by a mutex.

```
┌──────────────────────────────────────────────┐
│               Single Go Process              │
│                                              │
│   REST API  ──►  Job Store  ◄──  Workers     │
│   (HTTP)      (mem/SQLite/PG)  (goroutines)  │
│                  [blocked]       Worker 1    │
│                  [pending]       Worker 2    │
│                  [running]       Worker 3    │
│                  [done]                      │
│                                              │
│   Reaper (background goroutine)              │
│   └─ scans running jobs every 10s            │
│   └─ re-queues jobs with stale heartbeats    │
│   └─ unblocks ready downstream jobs          │
└──────────────────────────────────────────────┘
```

### Distributed Mode

The scheduler and workers run as separate binaries. Workers poll the scheduler over HTTP to claim jobs, send heartbeats, and report results. Workers can run on different machines. Multiple scheduler instances can share one PostgreSQL database for high availability.

```
┌─────────────────────┐         ┌──────────────────────┐
│      Scheduler      │         │       Workers        │
│                     │  HTTP   │                      │
│  REST API           │◄───────►│  Worker Process A    │
│  Job Store          │         │  Worker Process B    │
│  (mem/SQLite/PG)    │         │  Worker Process C    │
│  Reaper             │         │                      │
│                     │         │  Sends heartbeats    │
│  Validates DAGs     │         │  every 5s while      │
│  Unblocks ready     │         │  processing a job    │
│  jobs on completion │         │                      │
└─────────────────────┘         └──────────────────────┘
    Source of truth                   Any machine
```

Both modes use the same `JobSource` interface, so the worker code is identical regardless of whether it talks to a local store or a remote scheduler.

---

## Getting Started

### Prerequisites

- Go 1.22+
- Docker (for PostgreSQL only)

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

# With PostgreSQL persistence (requires: make run-postgres)
make run-standalone-postgres
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

### PostgreSQL Setup

```bash
# Copy the example env file and adjust credentials if needed
cp .env.example .env

# Start PostgreSQL via Docker Compose
make run-postgres

# Run PostgreSQL compliance tests
make test-postgres

# Stop PostgreSQL
make stop-postgres
```

### CLI Flags

**Scheduler** (`cmd/scheduler`)

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `scheduler` | `scheduler` (HTTP API only) or `standalone` (API + local workers) |
| `--port` | `8080` | Port for the REST API |
| `--workers` | `3` | Number of local workers (standalone mode only) |
| `--db-driver` | `memory` | Storage backend: `memory`, `sqlite`, `postgres` |
| `--db-url` | `""` | Database connection string (SQLite file path or PostgreSQL URL) |

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

### Submit a job with dependencies

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"command": "echo step2", "depends_on": ["job-1"]}'
```

Response (`201 Created`):
```json
{
  "id": "job-2",
  "command": "echo step2",
  "status": "blocked",
  "created_at": "2026-03-13T12:00:01Z",
  "max_retries": 3,
  "attempts": 0,
  "depends_on": ["job-1"]
}
```

The job starts as `blocked` and transitions to `pending` automatically once all dependencies reach `done`. Returns `400` if any dependency ID does not exist or if the dependency graph would contain a cycle.

### Submit a pipeline

```bash
JOB1=$(curl -s -X POST http://localhost:8080/jobs \
  -d '{"command":"echo step1"}' | jq -r .id)

JOB2=$(curl -s -X POST http://localhost:8080/jobs \
  -d "{\"command\":\"echo step2\", \"depends_on\":[\"$JOB1\"]}" | jq -r .id)

JOB3=$(curl -s -X POST http://localhost:8080/jobs \
  -d "{\"command\":\"echo step3\", \"depends_on\":[\"$JOB2\"]}" | jq -r .id)

# step1 runs immediately, step2 waits for step1, step3 waits for step2
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

### Health check

```bash
curl http://localhost:8080/health
```

Response (`200 OK`):
```json
{
  "instance_id": "a1b2c3d4",
  "uptime": "2h15m30s",
  "status": "ok"
}
```

Each scheduler instance generates a unique short ID at startup. Useful for load balancer health checks and identifying which instance you're talking to in multi-scheduler deployments.

### Prometheus metrics

```bash
curl http://localhost:8080/metrics
```

Returns Prometheus-compatible metrics including `workron_jobs_submitted_total`, `workron_jobs_completed_total`, `workron_job_execution_duration_seconds`, `workron_jobs_pending`, `workron_reaper_leader`, and more.

---

## Job Dependencies (DAG)

Jobs can declare dependencies on other jobs using the `depends_on` field. This creates a directed acyclic graph (DAG) where downstream jobs only execute after all their upstream dependencies complete.

**How it works:**

- A job with `depends_on` starts in `blocked` status instead of `pending`
- When a job completes (`done`), the scheduler checks all `blocked` jobs and transitions any whose dependencies are fully satisfied to `pending`
- At submission time, the scheduler validates that all referenced job IDs exist and that the new dependency would not create a cycle (using DFS-based cycle detection)
- The reaper also checks for unblockable jobs on each tick as a safety net

**What happens when a dependency fails?**

Currently, if a dependency fails permanently, downstream jobs remain `blocked` indefinitely. This is a known limitation, and a future improvement would cascade the failure or provide a way to manually unblock or cancel downstream jobs.

---

## Failure Detection

When a worker crashes mid-job, the scheduler detects it through missing heartbeats. A background reaper goroutine runs every 10 seconds and checks all running jobs:

- If a job's last heartbeat is older than 30 seconds (or was never set), the worker is assumed dead
- If the job has retries remaining, it is re-queued as `pending` for another worker to pick up
- If retries are exhausted, the job is marked as permanently `failed`

This ensures no job gets stuck in `running` forever, even if a worker process is killed without warning.

---

## Observability

Workron provides three layers of observability:

**Structured logging** (`log/slog`): all log output is JSON with typed fields (`job_id`, `worker_id`, `request_id`, `attempt`, `error`). Log levels distinguish normal events (Info), unusual events like reaper actions (Warn), and failures (Error). Each component receives its logger via dependency injection. Workers use `logger.With("worker_id", id)` so every log line automatically identifies the worker.

**Prometheus metrics** (`GET /metrics`): counters track job lifecycle events (`workron_jobs_submitted_total`, `workron_jobs_claimed_total`, `workron_jobs_completed_total`, `workron_jobs_failed_total`, `workron_jobs_retried_total`, `workron_jobs_reaped_total`). Histograms track execution duration and queue wait time. Gauges report current queue state (`workron_jobs_pending`, `workron_jobs_running`, `workron_jobs_blocked`) via a custom `prometheus.Collector` that queries the store on each scrape.  The `workron_reaper_leader` gauge indicates whether this instance is the active reaper leader (1) or a follower (0), useful for monitoring multi-scheduler deployments.

**Request ID tracing**: every HTTP request receives a UUID, set as the `X-Request-ID` response header and included in all log lines for that request. A request-scoped child logger is created in `ServeHTTP` and propagated to handlers via context, so `request_id` appears automatically without manual threading.

---

## Persistence

Workron supports three storage backends, selectable at startup via `--db-driver`:

**In-memory store** (default): jobs live only as long as the process runs. Fast, zero dependencies, ideal for development and testing.

**SQLite store** (`--db-driver=sqlite --db-url=workron.db`): jobs persist to a single file on disk. The scheduler can crash and restart without losing any job state. Uses WAL mode for write performance and a single-connection pool to avoid SQLite's write lock contention. Uses `modernc.org/sqlite` (pure Go, no CGo).

**PostgreSQL store** (`--db-driver=postgres --db-url=postgres://...`): jobs persist to PostgreSQL with a configurable connection pool (default 10 connections). Uses `FOR UPDATE SKIP LOCKED` for atomic job claiming, allowing multiple scheduler instances to claim different jobs concurrently without blocking each other. The reaper uses `pg_try_advisory_xact_lock` so only one instance runs heartbeat timeout detection at a time. Dependencies stored as JSONB, queried with `jsonb_array_elements_text()`. Uses `pgx/v5` for native PostgreSQL support.

All three backends implement the same `JobStore` interface. The server, workers, and reaper are unaware of which store they're using.

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
│   ├── metrics/
│   │   ├── metrics.go           # Prometheus counters, histograms, registration
│   │   ├── collector.go         # Custom gauge collector (queries store on scrape)
│   │   └── metrics_test.go
│   ├── store/
│   │   ├── store.go             # JobStore interface, Job struct, JobStatus
│   │   ├── memory.go            # In-memory store implementation
│   │   ├── memory_test.go
│   │   ├── sqlite.go            # SQLite store implementation
│   │   ├── sqlite_test.go
│   │   ├── postgres.go          # PostgreSQL store implementation (pgx/v5)
│   │   ├── postgres_test.go     # PG tests (build tag: postgres)
│   │   ├── store_test.go        # Shared compliance tests for all backends
│   │   ├── dag.go               # Cycle detection + dependency validation
│   │   └── dag_test.go
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
├── docker-compose.yml           # Local PostgreSQL for development
├── .env.example                 # Environment variable template
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
| Logging | `log/slog` (stdlib, JSON output) |
| Metrics | `prometheus/client_golang` |
| Storage | In-memory, SQLite, or PostgreSQL |
| SQLite driver | `modernc.org/sqlite` (pure Go, no CGo) |
| PostgreSQL driver | `jackc/pgx/v5` (native pgxpool, no database/sql) |
| ID generation | `google/uuid` (UUID v4) |
| Local dev | Docker Compose (PostgreSQL 16) |

---

## Key Technical Decisions

**PostgreSQL advisory locks over Raft for reaper coordination.** When multiple scheduler instances share a database, only one should run the reaper (heartbeat timeout scan) at a time. Instead of adding a full consensus protocol like Raft, the reaper acquires a transaction-scoped advisory lock (`pg_try_advisory_xact_lock`) on each tick. If another instance holds it, the tick is skipped. If the lock holder crashes, PostgreSQL automatically releases the lock when the connection drops. This gives leader election for free, no external coordination service, no Raft state machine, no split-brain risk. PostgreSQL is already the shared coordination layer, so using it for this is simpler and sufficient.

**`FOR UPDATE SKIP LOCKED` over application-level locking.** Multiple workers (or multiple schedulers) claiming jobs concurrently need atomicity. The naive approach is reading a pending job, then updating it, which has a race window where two workers read the same job. Application-level distributed locks (Redis, Zookeeper) add operational complexity. PostgreSQL's `FOR UPDATE SKIP LOCKED` solves this at the database level: when one transaction locks a row, other transactions skip it and move to the next row. Zero contention, zero double-claims, no external dependencies. This is the same pattern used by production job queues like Graphile Worker.

**GPU-aware bin-packing over simple tag filtering.** For ML workload scheduling, simple tag matching ("this worker has a GPU") doesn't prevent over-commitment. If a worker has 24GB VRAM and one job is using 16GB, a naive tag filter would still assign a second 16GB job to the same worker. Resource accounting tracks allocated vs. available capacity per worker, and first-fit-decreasing bin-packing places the largest pending job on the smallest worker that still fits. This is a real scheduling problem in ML infrastructure, the same approach used by Kubernetes resource requests.

**Pull-based scheduling with server-side selection over push-based assignment.** Workers poll `GET /jobs/next?worker_id=xxx` and the scheduler picks the best job for that worker based on its registered resources. The alternative is scheduler pushes jobs to workers, which requires the scheduler to track worker availability in real time and handle push failures. Pull-based is simpler: workers ask when they're ready, the scheduler has a consistent view of what's running where (from the database), and there's no push failure mode to handle.

**Transaction-scoped advisory locks over session-scoped.** `pg_try_advisory_xact_lock` releases automatically when the transaction commits, even if the application crashes before calling unlock. Session-scoped locks (`pg_advisory_lock`) persist until explicitly released or the connection closes, but with connection pooling (pgxpool), a returned connection might carry a stale lock into a different goroutine. Transaction-scoped locks eliminate this entire class of bugs.

---

## Contributing

This is a personal learning project and not yet ready for production use. Feedback and suggestions are welcome, feel free to open an issue.

---

## License

MIT
