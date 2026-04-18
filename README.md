# Workron

A distributed job scheduler written in Go, designed for ML and batch workloads.

---

## Overview

Workron is a distributed job scheduler that accepts jobs via a REST API and executes them across concurrent workers. It supports two deployment modes: a single-process standalone mode where the scheduler and workers share memory, and a distributed mode where the scheduler and workers run as separate binaries communicating over HTTP.

Jobs can declare dependencies on other jobs, forming a DAG (directed acyclic graph). The scheduler validates the dependency graph at submission time, rejecting cycles, and only makes downstream jobs available for execution once all their upstream dependencies have completed.

Jobs are persisted to SQLite or PostgreSQL, so in-flight and completed work survives a full scheduler restart. PostgreSQL uses `FOR UPDATE SKIP LOCKED` for safe concurrent job claiming across multiple connections, preparing for multi-scheduler deployments. An in-memory store is also available for development and testing.

Workers register with the scheduler on startup, reporting their resource capacity (VRAM, memory) and execution address. Workers send periodic heartbeats while processing jobs. A background reaper on the scheduler detects stale job heartbeats and re-queues orphaned jobs, and marks workers with stale heartbeats as offline, ensuring no work is silently lost when a worker crashes.

For distributed training and other multi-worker workloads, Workron supports gang scheduling: a single job request creates N coordinated tasks that must all be placed on suitable workers before any of them start. A background admission cycle reserves workers atomically, so tasks never start partially.

If you are curious about the design decisions and trade-offs behind this project, I wrote about the journey here:

- 📝 [Before the Code: Designing a Distributed Job Scheduler in Go](https://lrdinsu.github.io/posts/designing-distributed-job-scheduler-go/)
- 📝 [Building the Concurrent Monolith: Atomic Job Claiming in Go](https://lrdinsu.github.io/posts/building-concurrent-monolith-atomic-job-claiming-go/)
- 📝 [Splitting and Surviving Failures: HTTP Workers and Heartbeat Detection in Go](https://lrdinsu.github.io/posts/splitting-and-surviving-failures-workron/)
- 📝 [Surviving the Crash: Adding SQLite Persistence Without Touching Business Logic](https://lrdinsu.github.io/posts/persisting-jobs-with-sqlite-workron/)
- 📝 [DAG Dependencies: Teaching a Job Scheduler to Wait](https://lrdinsu.github.io/posts/dag-dependencies-workron/)
- 📝 [Making the Invisible Visible: Structured Logging, Metrics, and Request Tracing](https://lrdinsu.github.io/posts/observability-slog-prometheus-workron/)


---

## Features

- **REST API:** Submit, monitor, and manage jobs over HTTP
- **DAG pipelines:** Jobs declare upstream dependencies, validated at submission with cycle detection; downstream jobs run only after all dependencies complete
- **Pluggable storage:** In-memory for development, SQLite for single-node persistence, PostgreSQL for concurrent multi-connection access
- **Atomic job claiming:** Mutex in memory, `UPDATE ... RETURNING` in SQLite, `FOR UPDATE SKIP LOCKED` in PostgreSQL
- **Automatic retry:** Failed jobs re-queued up to `MaxRetries` times before marked permanently failed
- **Two deployment modes:** Standalone (single process) or distributed (separate scheduler + worker binaries over HTTP)
- **Heartbeat-based failure detection:** Workers send 5s heartbeats; the scheduler re-queues orphaned jobs after 30s of silence
- **Graceful shutdown:** Workers finish their current job before exiting
- **Structured logging:** JSON output via `log/slog` with typed fields, log levels, and per-component logger injection
- **Prometheus metrics:** Counters for job lifecycle events, histograms for execution duration and queue wait, gauges for queue state via custom collector
- **Request ID tracing:** UUID per HTTP request, `X-Request-ID` header, request-scoped logger via context
- **Multi-scheduler coordination:** Multiple instances share one PostgreSQL database; advisory locks ensure only one reaper runs at a time
- **Health endpoint:** `GET /health` returns instance ID, uptime, and status for load balancer checks
- **Job resource requirements:** Jobs declare VRAM and memory needs, with priority and queue assignment
- **Worker registration:** Workers register with resource capacity, execution address, and tags; stale workers marked offline automatically
- **Gang scheduling:** Submit N coordinated tasks that all reserve workers atomically before any start; background admission cycle places largest gangs first with capacity accounting across running and reserved jobs; gang env vars (`GANG_ID`, `GANG_SIZE`, `GANG_INDEX`, `GANG_PEERS`) injected at claim time
- **Checkpoint and output fields:** Jobs carry opaque JSON for checkpoint/resume and output tracking

**Planned — Scheduling Intelligence**
- [ ] Gang preemption: interrupt running siblings when a gang task fails, roll back the whole gang atomically
- [ ] Priority-based preemption with checkpoint/resume
- [ ] Queue resource quotas with cross-queue borrowing

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

Gang scheduling (gang_size > 1):

Submit ──► blocked ──► reserved ──► running ──► done
            (all N      (admission   (each worker
             tasks       cycle        claims its
             start       places all   pre-assigned
             blocked)    N at once)   task)

Future (preemption):

            running ──► preempting ──► preempted
                        (SIGTERM       (re-queued
                         sent)          or gang
                                        rolled back)
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
| `--worker-id` | auto-generated UUID | Worker identifier; passed to `GET /jobs/next?worker_id=` to claim gang-reserved tasks |

---

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/jobs` | Submit a job, or a gang when `gang_size > 1` (with optional dependencies, resources, priority) |
| `GET` | `/jobs` | List all jobs |
| `GET` | `/jobs/{id}` | Get job status |
| `GET` | `/jobs/next` | Claim next pending job; pass `?worker_id=` to also claim gang-reserved tasks for that worker |
| `POST` | `/jobs/{id}/done` | Report job completed |
| `POST` | `/jobs/{id}/fail` | Report job failed (scheduler decides retry vs permanent failure; propagates to gang siblings) |
| `POST` | `/jobs/{id}/heartbeat` | Worker heartbeat for running job |
| `GET` | `/gangs/{gang_id}` | List all tasks belonging to a gang |
| `POST` | `/workers/register` | Register a worker with resource capacity |
| `POST` | `/workers/{id}/heartbeat` | Worker liveness heartbeat |
| `GET` | `/workers` | List all registered workers |
| `GET` | `/health` | Health check (instance ID, uptime, status) |
| `GET` | `/metrics` | Prometheus metrics |

<details>
<summary><strong>Job endpoints</strong></summary>

#### Submit a job

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

#### Submit a job with dependencies

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

#### Submit a job with resource requirements

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"command": "python train.py", "resources": {"vram_mb": 16384, "memory_mb": 32768}, "priority": 5, "queue_name": "training"}'
```

Jobs can declare VRAM and memory requirements, a priority level (higher is more important), and a queue name. These fields are stored and returned on the job, ready for resource-aware scheduling in a future PR.

#### Submit a gang (multi-worker job)

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"command": "torchrun train.py", "gang_size": 4, "resources": {"vram_mb": 16384}}'
```

Response (`201 Created`):
```json
{
  "gang_id": "gang-a1b2c3",
  "tasks": ["job-1", "job-2", "job-3", "job-4"]
}
```

All four tasks start as `blocked`. A background admission cycle places them on four distinct workers with enough VRAM each, transitions them to `reserved`, and each worker claims its pre-assigned task via `GET /jobs/next?worker_id=`. At claim time, the scheduler injects `GANG_ID`, `GANG_SIZE`, `GANG_INDEX`, and `GANG_PEERS` environment variables so the worker processes can form their collective communication ring.

#### Submit a pipeline

```bash
JOB1=$(curl -s -X POST http://localhost:8080/jobs \
  -d '{"command":"echo step1"}' | jq -r .id)

JOB2=$(curl -s -X POST http://localhost:8080/jobs \
  -d "{\"command\":\"echo step2\", \"depends_on\":[\"$JOB1\"]}" | jq -r .id)

JOB3=$(curl -s -X POST http://localhost:8080/jobs \
  -d "{\"command\":\"echo step3\", \"depends_on\":[\"$JOB2\"]}" | jq -r .id)

# step1 runs immediately, step2 waits for step1, step3 waits for step2
```

#### Other job endpoints

```bash
curl http://localhost:8080/jobs/{id}          # Get job status
curl http://localhost:8080/jobs               # List all jobs
curl http://localhost:8080/jobs/next          # Claim next job (200 or 204 No Content)
curl -X POST http://localhost:8080/jobs/{id}/done       # Report done
curl -X POST http://localhost:8080/jobs/{id}/fail       # Report failed
curl -X POST http://localhost:8080/jobs/{id}/heartbeat  # Send heartbeat
```

</details>

<details>
<summary><strong>Worker endpoints</strong></summary>

#### Register a worker

```bash
curl -X POST http://localhost:8080/workers/register \
  -H "Content-Type: application/json" \
  -d '{"id": "worker-1", "exec_addr": "192.168.1.10:9000", "resources": {"vram_mb": 24576, "memory_mb": 65536}, "tags": ["gpu", "a100"]}'
```

Response (`201 Created`):
```json
{
  "id": "worker-1",
  "exec_addr": "192.168.1.10:9000",
  "resources": {"vram_mb": 24576, "memory_mb": 65536},
  "tags": ["gpu", "a100"],
  "status": "active",
  "last_heartbeat": "2026-04-05T12:00:00Z",
  "registered_at": "2026-04-05T12:00:00Z"
}
```

#### Worker heartbeat

```bash
curl -X POST http://localhost:8080/workers/worker-1/heartbeat
```

Returns `200` on success, `404` if worker not found.

#### List workers

```bash
curl http://localhost:8080/workers
```

Returns all registered workers with their current status and resource capacity.

</details>

<details>
<summary><strong>Health and metrics</strong></summary>

#### Health check

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

#### Prometheus metrics

```bash
curl http://localhost:8080/metrics
```

Returns Prometheus-compatible metrics including `workron_jobs_submitted_total`, `workron_jobs_completed_total`, `workron_job_execution_duration_seconds`, `workron_jobs_pending`, `workron_reaper_leader`, and more.

</details>

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

## Gang Scheduling

Distributed training jobs (PyTorch with NCCL, Jax on TPUs, and similar collective-communication workloads) need N workers starting simultaneously. If only some start, the rest hang waiting for peers that never join. Workron solves this with reservation-based gang scheduling.

**How it works:**

- Submitting a job with `gang_size > 1` creates N tasks sharing a `gang_id`. All N start in `blocked` status. No worker can claim them through the normal path.
- A background admission cycle runs every 5 seconds. On each tick it:
  1. Rolls back reservations older than 30 seconds (workers that died before claiming).
  2. Finds gangs where every task is `blocked`.
  3. Sorts candidates by size (largest first), then priority, then submission time.
  4. Computes per-worker available capacity, counting both `running` and `reserved` jobs as used.
  5. Atomically transitions each gang's tasks to `reserved`, one per worker, and subtracts the placed capacity before considering the next gang.
- Workers claim their assigned task by passing `?worker_id=` to `GET /jobs/next`. The scheduler returns only tasks reserved for that specific worker.
- At claim time, the scheduler injects `GANG_ID`, `GANG_SIZE`, `GANG_INDEX`, and `GANG_PEERS` environment variables into the job's env map, so worker processes can discover each other and form their communication ring.
- When a gang task fails, `FailGang` propagates the failure to blocked, reserved, and pending siblings. Running and done siblings are left untouched and are expected to fail on their own through peer detection or heartbeat timeout.

**Multi-instance safety:** the admission cycle uses a separate PostgreSQL advisory lock (different lock ID from the reaper), so only one scheduler instance runs placement at a time. The two coordination loops never block each other.

**Known limitation:** if some gang tasks start running and another task's worker dies before claiming, the stale reservation is rolled back but the already-running siblings are not preempted. The gang ends up with two running tasks and one blocked task, which does not qualify for re-placement. Active preemption of running siblings is planned for a future PR.

---

## Failure Detection

When a worker crashes mid-job, the scheduler detects it through missing heartbeats. A background reaper goroutine runs every 10 seconds and checks all running jobs:

- If a job's last heartbeat is older than 30 seconds (or was never set), the worker is assumed dead
- If the job has retries remaining, it is re-queued as `pending` for another worker to pick up
- If retries are exhausted, the job is marked as permanently `failed`

This ensures no job gets stuck in `running` forever, even if a worker process is killed without warning.

The reaper also checks worker heartbeats. Workers that haven't sent a heartbeat within 60 seconds are marked as `offline`. This is separate from job heartbeats: a worker might be healthy but a specific job process could have hung, or a worker might go down entirely. Both cases are detected and handled.

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
│   │   ├── store.go             # JobStore, WorkerStore, GangStore interfaces, Job/Worker structs
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
│   │   ├── server.go            # HTTP handlers (jobs, gangs, workers, health)
│   │   ├── server_test.go
│   │   ├── gang.go              # Gang admission cycle + placement logic
│   │   ├── gang_test.go         # Unit tests for placement and capacity
│   │   ├── gang_integration_test.go # End-to-end gang lifecycle tests
│   │   ├── reaper.go            # Background heartbeat timeout checker (gang-aware)
│   │   └── reaper_test.go
│   └── worker/
│       ├── worker.go            # Poll and execute loop
│       ├── worker_test.go
│       ├── executor.go          # Runs shell commands via os/exec (context-cancelable, env injection)
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
