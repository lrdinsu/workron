package scheduler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lrdinsu/workron/internal/store"
)

// newTestServer creates a server backed by a fresh MemoryStore for each test
func newTestServer() *Server {
	return NewServer(store.NewMemoryStore())
}

func TestHandleSubmitJob_Success(t *testing.T) {
	srv := newTestServer()

	body := bytes.NewBufferString(`{"command": "echo hello"}`)
	r := httptest.NewRequest(http.MethodPost, "/jobs", body)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d", w.Code)
	}

	var job store.Job
	if err := json.NewDecoder(w.Body).Decode(&job); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if job.ID == "" {
		t.Error("expected job ID in response, got empty string")
	}

	if job.Status != store.StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
}

func TestHandleSubmitJob_EmptyCommand(t *testing.T) {
	srv := newTestServer()

	body := bytes.NewBufferString(`{"command": ""}`)
	r := httptest.NewRequest(http.MethodPost, "/jobs", body)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}

}

func TestHandleSubmitJob_InvalidJSON(t *testing.T) {
	srv := newTestServer()

	body := bytes.NewBufferString(`not json`)
	r := httptest.NewRequest(http.MethodPost, "/jobs", body)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleGetJob_Found(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")
	srv := NewServer(s)

	r := httptest.NewRequest(http.MethodGet, "/jobs/"+id, nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var job store.Job
	if err := json.NewDecoder(w.Body).Decode(&job); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if job.ID != id {
		t.Errorf("expected job ID %s, got %s", id, job.ID)
	}
}

func TestHandleGetJob_NotFound(t *testing.T) {
	srv := newTestServer()

	r := httptest.NewRequest(http.MethodGet, "/jobs/doesnotexist", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestHandleListJobs_Empty(t *testing.T) {
	srv := newTestServer()

	r := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var jobs []store.Job
	if err := json.NewDecoder(w.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(jobs) != 0 {
		t.Errorf("expected empty list, got %d jobs", len(jobs))
	}
}

func TestHandleListJobs_WithJobs(t *testing.T) {
	s := store.NewMemoryStore()
	s.AddJob("echo hello")
	s.AddJob("echo world")
	srv := NewServer(s)

	r := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var jobs []store.Job
	if err := json.NewDecoder(w.Body).Decode(&jobs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(jobs))
	}
}
