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

func TestHandleClaimJob_ReturnsJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")
	srv := NewServer(s)

	r := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
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

	if job.Status != store.StatusRunning {
		t.Errorf("expected status running, got %s", job.Status)
	}
}

func TestHandleClaimJob_NoJobs(t *testing.T) {
	srv := newTestServer()

	r := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", w.Code)
	}
}

func TestHandleClaimJob_SkipsRunningJobs(t *testing.T) {
	s := store.NewMemoryStore()
	s.AddJob("echo first")
	srv := NewServer(s)

	// Claim the only job
	r1 := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	w1 := httptest.NewRecorder()
	srv.ServeHTTP(w1, r1)

	if w1.Code != http.StatusOK {
		t.Fatalf("first claim expected 200, got %d", w1.Code)
	}

	// Second claim should find nothing
	r2 := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, r2)

	if w2.Code != http.StatusNoContent {
		t.Errorf("second claim expected 204, got %d", w2.Code)
	}
}

func TestHandleJobDone_Success(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")

	// Claim the job first so it's in running state
	s.ClaimJob()

	srv := NewServer(s)
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/done", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	job, _ := s.GetJob(id)
	if job.Status != store.StatusDone {
		t.Errorf("expected status done, got %s", job.Status)
	}
}

func TestHandleJobDone_NotFound(t *testing.T) {
	srv := newTestServer()
	r := httptest.NewRequest(http.MethodPost, "/jobs/doesnotexist/done", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestHandleJobDone_NotRunning(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello") // status is pending, not running
	srv := NewServer(s)

	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/done", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", w.Code)
	}
}

func TestHandleJobFail_RetriesJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("bad command")

	// Claim it (attempt 1 of 3)
	s.ClaimJob()

	srv := NewServer(s)
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/fail", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// Job should be re-queued as pending since attempts(1) < maxRetries(3)
	job, _ := s.GetJob(id)
	if job.Status != store.StatusPending {
		t.Errorf("expected status pending (retry), got %s", job.Status)
	}
}

func TestHandleJobFail_PermanentFailure(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("bad command")

	// Exhaust all retries by claiming 3 times
	for i := 0; i < 3; i++ {
		s.ClaimJob()
		if i < 2 {
			// Re-queue for the next claim
			s.UpdateJobStatus(id, store.StatusPending)
		}
	}

	// Now attempts == 3 == maxRetries, job is running
	srv := NewServer(s)
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/fail", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// Job should be permanently failed since attempts(3) >= maxRetries(3)
	job, _ := s.GetJob(id)
	if job.Status != store.StatusFailed {
		t.Errorf("expected status failed, got %s", job.Status)
	}
}

func TestHandleHeartbeat_Success(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello")
	s.ClaimJob() // move to running

	srv := NewServer(s)
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/heartbeat", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	job, _ := s.GetJob(id)
	if job.LastHeartbeat == nil {
		t.Error("expected last_heartbeat to be set")
	}
}

func TestHandleHeartbeat_NotFound(t *testing.T) {
	srv := newTestServer()
	r := httptest.NewRequest(http.MethodPost, "/jobs/doesnotexist/heartbeat", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestHandleHeartbeat_NotRunning(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob("echo hello") // status is pending, not running
	srv := NewServer(s)

	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/heartbeat", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", w.Code)
	}
}
