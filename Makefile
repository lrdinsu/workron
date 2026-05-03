-include .env
export

.PHONY: build test test-v test-race test-postgres clean \
       run-scheduler run-scheduler-sqlite run-scheduler-postgres \
       run-standalone run-standalone-sqlite run-standalone-postgres \
       run-worker run-postgres stop-postgres \
       k8s-up k8s-down k8s-logs k8s-demo k8s-build k8s-load

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

# ---------- Kubernetes (kind) ----------

KIND_CLUSTER ?= workron
SCHEDULER_IMAGE ?= workron-scheduler:dev
WORKER_IMAGE ?= workron-worker:dev

# Build both container images from the local source tree.
k8s-build:
	docker build -f cmd/scheduler/Dockerfile -t $(SCHEDULER_IMAGE) .
	docker build -f cmd/worker/Dockerfile -t $(WORKER_IMAGE) .

# Load locally-built images into the kind node so kubelet can pull them
# without a registry (paired with imagePullPolicy: IfNotPresent).
k8s-load:
	kind load docker-image $(SCHEDULER_IMAGE) $(WORKER_IMAGE) --name $(KIND_CLUSTER)

# Bring up the full stack: cluster + images + manifests, then wait for
# every workload to become Ready before returning. Idempotent enough to
# re-run after a code change followed by `make k8s-build && make k8s-load`,
# though for a clean slate prefer `make k8s-down && make k8s-up`.
k8s-up:
	kind create cluster --name $(KIND_CLUSTER) --config deploy/kind-config.yaml
	$(MAKE) k8s-build
	$(MAKE) k8s-load
	kubectl apply -k deploy/k8s/overlays/local
	kubectl -n workron rollout status statefulset/workron-postgres --timeout=180s
	kubectl -n workron wait --for=condition=ready pod -l app=workron-postgres --timeout=180s
	kubectl -n workron rollout status deployment/workron-scheduler --timeout=180s
	kubectl -n workron wait --for=condition=ready pod -l app=workron-scheduler --timeout=180s
	kubectl -n workron rollout status deployment/workron-worker --timeout=180s
	kubectl -n workron wait --for=condition=ready pod -l app=workron-worker --timeout=180s
	@echo
	@echo "Workron is up. Try: curl http://localhost:30080/healthz"

# Tear down the kind cluster entirely (removes the local PV with it).
k8s-down:
	kind delete cluster --name $(KIND_CLUSTER)

# Tail logs from both scheduler replicas concurrently.
k8s-logs:
	kubectl -n workron logs -l app=workron-scheduler -f --max-log-requests=10

# End-to-end gang-preemption demo against the in-cluster API.
k8s-demo:
	bash scripts/k8s-demo.sh

# ---------- Clean ----------

# Remove built binaries and database files
clean:
	rm -rf bin/
	rm -f workron.db workron.db-wal workron.db-shm