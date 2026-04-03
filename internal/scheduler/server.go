package scheduler

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
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
	store      store.JobStore
	mux        *http.ServeMux
	logger     *slog.Logger
	metrics    *metrics.Metrics
	registry   *prometheus.Registry
	instanceID string
	startTime  time.Time
}

// NewServer creates a new Server and registers all routes.
// instanceID uniquely identifies this scheduler instance,
// userful when multiple schedulers share one database.
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
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
}

// submitJobRequest is the expected JSON body for POST /jobs
type submitJobRequest struct {
	Command   string   `json:"command"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// handleSubmitJob handles POST /jobs
// Accepts: {"command": "echo hello", "depends_on": ["job-123"]}
// Returns: the created job as JSON (201)
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

	if len(req.DependsOn) > 0 {
		if err := store.ValidateDependencies(ctx, s.store, req.DependsOn); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	id := s.store.AddJob(ctx, req.Command, req.DependsOn)
	job, _ := s.store.GetJob(ctx, id)

	s.metrics.JobsSubmitted.Inc()
	logger.Info("job submitted", "job_id", id, "command", req.Command, "depends_on", req.DependsOn)
	s.writeJSON(w, http.StatusCreated, job)
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
// Atomically claims one pending job and returns it to the calling worker.
// Returns 204 No Content if no jobs are available.
func (s *Server) handleClaimJob(w http.ResponseWriter, r *http.Request) {
	logger := s.requestLogger(r.Context())

	job, found := s.store.ClaimJob(r.Context())
	if !found {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s.metrics.JobsClaimed.Inc()
	if job.StartedAt != nil {
		s.metrics.JobQueueWait.Observe(job.StartedAt.Sub(job.CreatedAt).Seconds())
	}
	logger.Info("job claimed", "job_id", job.ID)
	s.writeJSON(w, http.StatusOK, job)
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

	// Retry logic: if attempts haven't been exhausted, re-queue as pending
	if job.Attempts < job.MaxRetries {
		logger.Warn("job failed, re-queuing", "job_id", id, "attempt", job.Attempts, "max_retries", job.MaxRetries)
		s.store.UpdateJobStatus(ctx, id, store.StatusPending)
		s.metrics.JobsRetried.Inc()
	} else {
		logger.Error("job failed permanently", "job_id", id, "attempt", job.Attempts)
		s.store.UpdateJobStatus(ctx, id, store.StatusFailed)
		s.metrics.JobsFailed.Inc()
	}

	w.WriteHeader(http.StatusOK)
}

// handleHeartbeat handles POST /jobs/{id}/heartbeat
// Worker signals it is still alive and working on this job.
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
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

	s.store.UpdateHeartbeat(ctx, id)
	w.WriteHeader(http.StatusOK)
}

// handleHealth handles GET /health
// Returns the scheduler instance ID, uptime, and status. Useful for load
// balancer health checks and debugging multi-scheduler deployments.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{
		"instance_id": s.instanceID,
		"uptime":      time.Since(s.startTime).Round(time.Second).String(),
		"status":      "ok",
	})
}

// writeJSON encodes v as JSON and writes it to w
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.Error("failed to encode response", "error", err)
	}
}
