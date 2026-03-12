package scheduler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/lrdinsu/workron/internal/store"
)

// Server holds the HTTP mux and the job store
type Server struct {
	store store.JobStore
	mux   *http.ServeMux
}

// NewServer creates a new Server and registers all routes
func NewServer(s store.JobStore) *Server {
	srv := &Server{
		store: s,
		mux:   http.NewServeMux(),
	}
	srv.registerRoutes()
	return srv
}

// ServeHTTP implements http.Handler so the Server can be passed to http.ListenAndServe
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// regsterRoutes wires up all HTTP endpoints
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("POST /jobs", s.handleSubmitJob)
	s.mux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /jobs", s.handleListJobs)
}

// submitJobRequest is the expected JSON body for POST /jobs
type submitJobRequest struct {
	Command string `json:"command"`
}

// handleSubmitJob handles Post /jobs
// Accepts: {"command": "echo hello"}
// Returns: {"id": "job-123", "status": "pending"}
func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Command == "" {
		http.Error(w, "command is required", http.StatusBadRequest)
		return
	}

	id := s.store.AddJob(req.Command)
	job, _ := s.store.GetJob(id)

	log.Printf("[server] job %s submitted: %q", id, req.Command)
	writeJSON(w, http.StatusCreated, job)
}

// handleGetJob handles Get /jobs/{id}
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	job, found := s.store.GetJob(id)
	if !found {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// handleListJobs handles GET /jobs
func (s *Server) handleListJobs(w http.ResponseWriter, _ *http.Request) {
	jobs := s.store.ListJobs()
	writeJSON(w, http.StatusOK, jobs)
}

// writeJSON is a helper that encodes v as JSON and writes it to w
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[server] failed to encode response: %v", err)
	}
}
