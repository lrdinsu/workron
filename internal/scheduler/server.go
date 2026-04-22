package scheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lrdinsu/workron/internal/metrics"
	"github.com/lrdinsu/workron/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// contextKey is an unexported type for context keys in this package.
type contextKey string

const loggerKey contextKey = "logger"

// Server holds the HTTP mux and the job store
type Server struct {
	store       store.JobStore
	workerStore store.WorkerStore // nil if the store doesn't implement WorkerStore
	gangStore   store.GangStore   // nil if the store doesn't implement GangStore
	mux         *http.ServeMux
	logger      *slog.Logger
	metrics     *metrics.Metrics
	registry    *prometheus.Registry
	instanceID  string
	startTime   time.Time
}

// NewServer creates a new Server and registers all routes.
// instanceID uniquely identifies this scheduler instance,
// useful when multiple schedulers share one database.
func NewServer(s store.JobStore, logger *slog.Logger, m *metrics.Metrics, registry *prometheus.Registry, instanceID string) *Server {
	srv := &Server{
		store:      s,
		mux:        http.NewServeMux(),
		logger:     logger,
		metrics:    m,
		registry:   registry,
		instanceID: instanceID,
		startTime:  time.Now(),
	}

	// Discover optional store capabilities via type assertion.
	// All three backends (Memory, SQLite, Postgres) implement both.
	if ws, ok := s.(store.WorkerStore); ok {
		srv.workerStore = ws
	}
	if gs, ok := s.(store.GangStore); ok {
		srv.gangStore = gs
	}
	srv.registerRoutes()
	return srv
}

// ServeHTTP implements http.Handler. It generates a unique request ID,
// creates a request-scoped logger with that ID, and sets the X-Request-ID
// response header before delegating to the mux.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := uuid.New().String()

	// Create a child logger that includes request_id on every log line.
	logger := s.logger.With("request_id", requestID)

	// Store the logger in context so handlers can retrieve it.
	ctx := context.WithValue(r.Context(), loggerKey, logger)

	// Set response header so clients can trace their request.
	w.Header().Set("X-Request-ID", requestID)

	s.mux.ServeHTTP(w, r.WithContext(ctx))
}

// requestLogger extracts the request-scoped logger from context.
// Falls back to the server's base logger if not present (e.g., in tests
// that call handlers directly without going through ServeHTTP).
func (s *Server) requestLogger(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return logger
	}
	return s.logger
}

// registerRoutes wires up all HTTP endpoints
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/next", s.handleClaimJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /jobs", s.handleListJobs)
	s.mux.HandleFunc("POST /jobs/{id}/done", s.handleJobDone)
	s.mux.HandleFunc("POST /jobs/{id}/fail", s.handleJobFail)
	s.mux.HandleFunc("POST /jobs/{id}/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("POST /jobs/{id}/preempted", s.handleJobPreempted)
	s.mux.HandleFunc("POST /jobs/{id}/checkpoint", s.handleSaveCheckpoint)
	s.mux.HandleFunc("GET /jobs/{id}/checkpoint", s.handleGetCheckpoint)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /workers/register", s.handleRegisterWorker)
	s.mux.HandleFunc("POST /workers/{id}/heartbeat", s.handleWorkerHeartbeat)
	s.mux.HandleFunc("GET /workers", s.handleListWorkers)
	s.mux.HandleFunc("GET /gangs/{gang_id}", s.handleGetGang)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
}

// submitJobRequest is the expected JSON body for POST /jobs
type submitJobRequest struct {
	Command   string              `json:"command"`
	DependsOn []string            `json:"depends_on,omitempty"`
	GangSize  int                 `json:"gang_size,omitempty"`
	Resources *store.ResourceSpec `json:"resources,omitempty"`
	Priority  int                 `json:"priority,omitempty"`
}

// gangSubmitResponse is returned when gang_size > 1.
type gangSubmitResponse struct {
	GangID string   `json:"gang_id"`
	Tasks  []string `json:"tasks"`
}

// handleSubmitJob handles POST /jobs
// Accepts: {"command": "echo hello", "depends_on": ["job-123"]}
// With gang_size > 1: {"command": "torchrun train.py", "gang_size": 4, "resources": {"vram_mb": 8192}}
// Returns: the created job as JSON (201), or gang response for gang submissions.
// Returns 400 if command is empty, dependencies are missing, or a cycle is detected.
func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	logger := s.requestLogger(r.Context())

	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Command == "" {
		http.Error(w, "command is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Gang submission path
	if req.GangSize > 1 {
		if s.gangStore == nil {
			http.Error(w, "gang scheduling not supported", http.StatusNotImplemented)
			return
		}
		if len(req.DependsOn) > 0 {
			http.Error(w, "gang jobs cannot have dependencies", http.StatusBadRequest)
			return
		}

		gangID, taskIDs := s.gangStore.AddGang(ctx, store.AddJobParams{
			Command:   req.Command,
			GangSize:  req.GangSize,
			Resources: req.Resources,
			Priority:  req.Priority,
		})

		s.metrics.JobsSubmitted.Add(float64(req.GangSize))
		logger.Info("gang submitted", "gang_id", gangID, "gang_size", req.GangSize, "command", req.Command)
		s.writeJSON(w, http.StatusCreated, gangSubmitResponse{GangID: gangID, Tasks: taskIDs})
		return
	}

	// Regular single-job path
	if len(req.DependsOn) > 0 {
		if err := store.ValidateDependencies(ctx, s.store, req.DependsOn); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	id := s.store.AddJob(ctx, store.AddJobParams{
		Command:   req.Command,
		DependsOn: req.DependsOn,
		Resources: req.Resources,
		Priority:  req.Priority,
	})
	job, _ := s.store.GetJob(ctx, id)

	s.metrics.JobsSubmitted.Inc()
	logger.Info("job submitted", "job_id", id, "command", req.Command, "depends_on", req.DependsOn)
	s.writeJSON(w, http.StatusCreated, job)
}

// handleGetGang handles GET /gangs/{gang_id}
// Returns all tasks in a gang, sorted by gang_index.
func (s *Server) handleGetGang(w http.ResponseWriter, r *http.Request) {
	if s.gangStore == nil {
		http.Error(w, "gang scheduling not supported", http.StatusNotImplemented)
		return
	}

	gangID := r.PathValue("gang_id")
	tasks := s.gangStore.ListGangTasks(r.Context(), gangID)
	if len(tasks) == 0 {
		http.Error(w, "gang not found", http.StatusNotFound)
		return
	}

	s.writeJSON(w, http.StatusOK, tasks)
}

// handleGetJob handles GET /jobs/{id}
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, found := s.store.GetJob(r.Context(), id)
	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	s.writeJSON(w, http.StatusOK, job)
}

// handleListJobs handles GET /jobs
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs := s.store.ListJobs(r.Context())
	s.writeJSON(w, http.StatusOK, jobs)
}

// handleClaimJob handles GET /jobs/next
// Accepts optional query param: ?worker_id=xxx
// If worker_id is set and the worker has a reserved gang task, that task is
// claimed first (with GANG_* env vars populated). Otherwise falls through
// to claiming a regular pending job.
// Returns 204 No Content if no jobs are available.
func (s *Server) handleClaimJob(w http.ResponseWriter, r *http.Request) {
	logger := s.requestLogger(r.Context())
	ctx := r.Context()
	workerID := r.URL.Query().Get("worker_id")

	// Try reserved gang task first if worker_id is provided
	if workerID != "" && s.gangStore != nil {
		job, found := s.gangStore.ClaimReservedJob(ctx, workerID)
		if found {
			job.Env = s.buildJobEnv(ctx, job)
			s.metrics.JobsClaimed.Inc()
			if job.StartedAt != nil {
				s.metrics.JobQueueWait.Observe(job.StartedAt.Sub(job.CreatedAt).Seconds())
			}
			logger.Info("gang task claimed", "job_id", job.ID, "gang_id", job.GangID, "worker_id", workerID)
			s.writeJSON(w, http.StatusOK, job)
			return
		}
	}

	// Fall through to regular pending job
	job, found := s.store.ClaimJob(ctx)
	if !found {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	job.Env = s.buildJobEnv(ctx, job)

	s.metrics.JobsClaimed.Inc()
	if job.StartedAt != nil {
		s.metrics.JobQueueWait.Observe(job.StartedAt.Sub(job.CreatedAt).Seconds())
	}
	logger.Info("job claimed", "job_id", job.ID)
	s.writeJSON(w, http.StatusOK, job)
}

// buildJobEnv constructs the env map handed to a newly claimed job.
// It is called for both gang and non-gang claims so that a re-admitted
// task (gang or otherwise) sees its previously saved checkpoint in
// CHECKPOINT_DATA if any was stored. Gang-specific variables are
// delegated to addGangEnv.
//
// Returns nil when there is nothing to inject, so the response stays
// compact for the common case of a first-time claim.
func (s *Server) buildJobEnv(ctx context.Context, job *store.Job) map[string]string {
	var env map[string]string
	ensure := func() {
		if env == nil {
			env = make(map[string]string, 4)
		}
	}

	if job.GangID != "" {
		ensure()
		s.addGangEnv(ctx, env, job)
	}

	if len(job.Checkpoint) > 0 {
		ensure()
		// Base64 so arbitrary JSON bytes (including ones that wouldn't
		// be well-formed as a raw env value) survive process environment
		// transport. The job binary can decode on startup. Best-effort
		// for demo-scale checkpoints; large artifacts should go through
		// a side channel, not env vars.
		env["CHECKPOINT_DATA"] = base64.StdEncoding.EncodeToString(job.Checkpoint)
	}

	return env
}

// addGangEnv populates GANG_ID, GANG_SIZE, GANG_INDEX, and GANG_PEERS
// into env for a gang task. GANG_PEERS is a comma-separated list of
// worker ExecAddr values ordered by gang_index, used by distributed
// training frameworks for rendezvous.
func (s *Server) addGangEnv(ctx context.Context, env map[string]string, job *store.Job) {
	env["GANG_ID"] = job.GangID
	env["GANG_SIZE"] = strconv.Itoa(job.GangSize)
	env["GANG_INDEX"] = strconv.Itoa(job.GangIndex)

	if s.workerStore == nil {
		return
	}

	// Build GANG_PEERS from sibling tasks' worker ExecAddrs
	tasks := s.gangStore.ListGangTasks(ctx, job.GangID)
	peers := make([]string, len(tasks))
	for _, task := range tasks {
		if task.WorkerID == "" {
			continue
		}
		w, found := s.workerStore.GetWorker(ctx, task.WorkerID)
		if found {
			peers[task.GangIndex] = w.ExecAddr
		}
	}
	env["GANG_PEERS"] = strings.Join(peers, ",")
}

// handleJobDone handles POST /jobs/{id}/done
// Worker reports that a job completed successfully.
// After marking the job done, unblocks any downstream jobs whose
// dependencies are now fully satisfied.
func (s *Server) handleJobDone(w http.ResponseWriter, r *http.Request) {
	logger := s.requestLogger(r.Context())
	id := r.PathValue("id")
	ctx := r.Context()

	job, found := s.store.GetJob(ctx, id)
	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if job.Status != store.StatusRunning {
		http.Error(w, "job is not running", http.StatusConflict)
		return
	}

	s.store.UpdateJobStatus(ctx, id, store.StatusDone)
	s.store.UnblockReady(ctx)

	// Record execution duration from timestamps.
	updatedJob, _ := s.store.GetJob(ctx, id)
	if updatedJob.StartedAt != nil && updatedJob.DoneAt != nil {
		s.metrics.JobExecDuration.Observe(updatedJob.DoneAt.Sub(*updatedJob.StartedAt).Seconds())
	}

	s.metrics.JobsCompleted.Inc()
	logger.Info("job done", "job_id", id)
	w.WriteHeader(http.StatusOK)
}

// handleJobFail handles POST /jobs/{id}/fail
// Worker reports that a job failed. The scheduler decides whether to retry.
func (s *Server) handleJobFail(w http.ResponseWriter, r *http.Request) {
	logger := s.requestLogger(r.Context())
	id := r.PathValue("id")
	ctx := r.Context()

	job, found := s.store.GetJob(ctx, id)
	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if job.Status != store.StatusRunning {
		http.Error(w, "job is not running", http.StatusConflict)
		return
	}

	// Gang preemption path. If this job belongs to a gang, try to enter
	// coordinated drain before touching the trigger's own status.
	// The store decides atomically whether any sibling is still running;
	// if so, running siblings (trigger included) move to preempting and
	// non-running siblings go straight to blocked. We skip both the per-task
	// status update and the legacy FailGang call in that case.
	if job.GangID != "" && s.gangStore != nil {
		res, err := s.gangStore.PreemptGang(ctx, job.GangID, id)
		if err != nil {
			logger.Error("preempt gang failed", "gang_id", job.GangID, "job_id", id, "error", err)
			// Fall through to the legacy path on error so the job doesn't
			// get stuck in running forever.
		} else if res.Entered {
			logger.Info("gang drain started",
				"gang_id", job.GangID,
				"trigger_job_id", id,
				"preemption_epoch", res.Epoch,
				"transitioned", res.Transitioned,
			)
			s.metrics.JobsRetried.Inc()
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	// Retry logic: if attempts haven't been exhausted, re-queue as pending
	retry := job.Attempts < job.MaxRetries
	if retry {
		logger.Warn("job failed, re-queuing", "job_id", id, "attempt", job.Attempts, "max_retries", job.MaxRetries)
		s.store.UpdateJobStatus(ctx, id, store.StatusPending)
		s.metrics.JobsRetried.Inc()
	} else {
		logger.Error("job failed permanently", "job_id", id, "attempt", job.Attempts)
		s.store.UpdateJobStatus(ctx, id, store.StatusFailed)
		s.metrics.JobsFailed.Inc()
	}

	// Legacy gang failure path: reached only when PreemptGang returned
	// Entered=false (no sibling was running). Propagates to
	// blocked/reserved/pending siblings. Done siblings are left untouched.
	if job.GangID != "" && s.gangStore != nil {
		if retry {
			logger.Info("gang task failed, retrying gang siblings", "gang_id", job.GangID, "job_id", id)
		} else {
			logger.Info("gang task failed permanently, failing gang siblings", "gang_id", job.GangID, "job_id", id)
		}
		if err := s.gangStore.FailGang(ctx, job.GangID, retry); err != nil {
			logger.Error("failed to propagate gang failure", "gang_id", job.GangID, "error", err)
		}
	}

	w.WriteHeader(http.StatusOK)
}

// heartbeatResponse is the JSON body returned when the scheduler wants the
// worker to take an action on its next heartbeat. For ordinary running
// jobs the body is empty (back-compat with clients that do not decode it).
// For preempting jobs the body carries action="preempt" and the
// PreemptionEpoch the worker should echo back when it calls
// /jobs/{id}/preempted after its process exits.
type heartbeatResponse struct {
	Action          string `json:"action"`
	PreemptionEpoch int    `json:"preemption_epoch,omitempty"`
}

// handleHeartbeat handles POST /jobs/{id}/heartbeat
// Worker signals it is still alive and working on this job.
// Accepts heartbeats from both running and preempting jobs: a preempting
// job's worker still needs to heartbeat until its process exits, and
// the response body is how the scheduler tells it to stop.
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	job, found := s.store.GetJob(ctx, id)
	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	if job.Status != store.StatusRunning && job.Status != store.StatusPreempting {
		http.Error(w, "job is not running", http.StatusConflict)
		return
	}

	s.store.UpdateHeartbeat(ctx, id)

	if job.Status == store.StatusPreempting {
		s.writeJSON(w, http.StatusOK, heartbeatResponse{
			Action:          "preempt",
			PreemptionEpoch: job.PreemptionEpoch,
		})
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleJobPreempted handles POST /jobs/{id}/preempted?epoch=N
// Worker acknowledgement that a preempted job's process has exited.
// The epoch must match the job's current PreemptionEpoch, a stale
// epoch is rejected with 409 so a late ack from an earlier drain
// round doesn't flip a newly-reserved task back to preempted.
func (s *Server) handleJobPreempted(w http.ResponseWriter, r *http.Request) {
	logger := s.requestLogger(r.Context())
	id := r.PathValue("id")
	ctx := r.Context()

	if s.gangStore == nil {
		http.Error(w, "gang scheduling not supported", http.StatusNotImplemented)
		return
	}

	if _, found := s.store.GetJob(ctx, id); !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	epochStr := r.URL.Query().Get("epoch")
	if epochStr == "" {
		http.Error(w, "epoch query parameter is required", http.StatusBadRequest)
		return
	}
	epoch, err := strconv.Atoi(epochStr)
	if err != nil {
		http.Error(w, "epoch must be an integer", http.StatusBadRequest)
		return
	}

	if err := s.gangStore.MarkPreempted(ctx, id, epoch); err != nil {
		// Either the job is not in preempting or the epoch doesn't
		// match. Either way the worker's ack cannot be applied cleanly.
		logger.Info("preempt ack rejected", "job_id", id, "preemption_epoch", epoch, "error", err)
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	logger.Info("job preempted", "job_id", id, "preemption_epoch", epoch)
	w.WriteHeader(http.StatusOK)
}

// handleSaveCheckpoint handles POST /jobs/{id}/checkpoint?epoch=N
// Worker uploads opaque checkpoint bytes while a preemption drain is
// in flight. The body is stored verbatim on the job's Checkpoint
// field and surfaced to the next claim as CHECKPOINT_DATA (base64).
// The epoch must match PreemptionEpoch and the job must be in
// preempting, otherwise 409.
func (s *Server) handleSaveCheckpoint(w http.ResponseWriter, r *http.Request) {
	logger := s.requestLogger(r.Context())
	id := r.PathValue("id")
	ctx := r.Context()

	if s.gangStore == nil {
		http.Error(w, "gang scheduling not supported", http.StatusNotImplemented)
		return
	}
	if _, found := s.store.GetJob(ctx, id); !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	epochStr := r.URL.Query().Get("epoch")
	if epochStr == "" {
		http.Error(w, "epoch query parameter is required", http.StatusBadRequest)
		return
	}
	epoch, err := strconv.Atoi(epochStr)
	if err != nil {
		http.Error(w, "epoch must be an integer", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		http.Error(w, "checkpoint body is empty", http.StatusBadRequest)
		return
	}

	if err := s.gangStore.SaveCheckpoint(ctx, id, epoch, body); err != nil {
		logger.Info("checkpoint rejected", "job_id", id, "preemption_epoch", epoch, "error", err)
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	logger.Info("checkpoint saved", "job_id", id, "preemption_epoch", epoch, "bytes", len(body))
	w.WriteHeader(http.StatusOK)
}

// handleGetCheckpoint handles GET /jobs/{id}/checkpoint
// Returns the raw checkpoint bytes for a job (200), or 204 No Content
// if no checkpoint has been stored. Primarily a debugging aid; the
// normal injection path for workers is the CHECKPOINT_DATA env var
// populated on claim by buildJobEnv.
func (s *Server) handleGetCheckpoint(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	if s.gangStore == nil {
		http.Error(w, "gang scheduling not supported", http.StatusNotImplemented)
		return
	}
	if _, found := s.store.GetJob(ctx, id); !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	data, found := s.gangStore.GetCheckpoint(ctx, id)
	if !found {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		s.logger.Error("write checkpoint body", "error", err)
	}
}

// handleHealth handles GET /health
// Returns the scheduler instance ID, uptime, and status. Useful for load-balancer
// health checks and debugging multi-scheduler deployments.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{
		"instance_id": s.instanceID,
		"uptime":      time.Since(s.startTime).Round(time.Second).String(),
		"status":      "ok",
	})
}

// --- Worker endpoints ---

// handleRegisterWorker handles POST /workers/register
// A worker calls this on startup to announce itself to the scheduler.
func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	if s.workerStore == nil {
		http.Error(w, "worker registration not supported", http.StatusNotImplemented)
		return
	}

	var worker store.Worker
	if err := json.NewDecoder(r.Body).Decode(&worker); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if worker.ID == "" {
		http.Error(w, "worker id is required", http.StatusBadRequest)
		return
	}

	if err := s.workerStore.RegisterWorker(r.Context(), worker); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	registered, _ := s.workerStore.GetWorker(r.Context(), worker.ID)
	s.requestLogger(r.Context()).Info("worker registered", "worker_id", worker.ID)
	s.writeJSON(w, http.StatusCreated, registered)
}

// handleWorkerHeartbeat handles POST /workers/{id}/heartbeat
// A worker calls this periodically to signal it is still alive.
func (s *Server) handleWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	if s.workerStore == nil {
		http.Error(w, "worker registration not supported", http.StatusNotImplemented)
		return
	}

	id := r.PathValue("id")
	if err := s.workerStore.WorkerHeartbeat(r.Context(), id); err != nil {
		http.Error(w, "worker not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleListWorkers handles GET /workers
// Returns all registered workers.
func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	if s.workerStore == nil {
		http.Error(w, "worker registration not supported", http.StatusNotImplemented)
		return
	}

	workers := s.workerStore.ListWorkers(r.Context())
	s.writeJSON(w, http.StatusOK, workers)
}

// writeJSON encodes v as JSON and writes it to w
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error("failed to encode response", "error", err)
	}
}
