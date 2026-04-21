package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lrdinsu/workron/internal/metrics"
	"github.com/lrdinsu/workron/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// newTestServer creates a server backed by a fresh MemoryStore for each test
func newTestServer() *Server {
	return NewServer(store.NewMemoryStore(), slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
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
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

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
	s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})
	s.AddJob(context.Background(), store.AddJobParams{Command: "echo world"})
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

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
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

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
	s.AddJob(context.Background(), store.AddJobParams{Command: "echo first"})
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

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
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})

	// Claim the job first so it's in running state
	s.ClaimJob(context.Background())

	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/done", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	job, _ := s.GetJob(context.Background(), id)
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
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"}) // status is pending, not running
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/done", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", w.Code)
	}
}

func TestHandleJobFail_RetriesJob(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "bad command"})

	// Claim it (attempt 1 of 3)
	s.ClaimJob(context.Background())

	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/fail", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// Job should be re-queued as pending since attempts(1) < maxRetries(3)
	job, _ := s.GetJob(context.Background(), id)
	if job.Status != store.StatusPending {
		t.Errorf("expected status pending (retry), got %s", job.Status)
	}
}

func TestHandleJobFail_PermanentFailure(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "bad command"})

	// Exhaust all retries by claiming 3 times
	for i := 0; i < 3; i++ {
		s.ClaimJob(context.Background())
		if i < 2 {
			// Re-queue for the next claim
			s.UpdateJobStatus(context.Background(), id, store.StatusPending)
		}
	}

	// Now attempts == 3 == maxRetries, job is running
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/fail", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// Job should be permanently failed since attempts(3) >= maxRetries(3)
	job, _ := s.GetJob(context.Background(), id)
	if job.Status != store.StatusFailed {
		t.Errorf("expected status failed, got %s", job.Status)
	}
}

func TestHandleHeartbeat_Success(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})
	s.ClaimJob(context.Background()) // move to running

	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/heartbeat", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	job, _ := s.GetJob(context.Background(), id)
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
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"}) // status is pending, not running
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/heartbeat", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status 409, got %d", w.Code)
	}
}

// TestHandleHeartbeat_RunningEmptyBody confirms that a running job's
// heartbeat response has no body. Back-compat: existing workers that
// don't decode the response body keep working.
func TestHandleHeartbeat_RunningEmptyBody(t *testing.T) {
	s := store.NewMemoryStore()
	id := s.AddJob(context.Background(), store.AddJobParams{Command: "echo hello"})
	s.ClaimJob(context.Background())

	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+id+"/heartbeat", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if w.Body.Len() != 0 {
		t.Errorf("expected empty body for running job, got %q", w.Body.String())
	}
}

// TestHandleHeartbeat_PreemptingReturnsAction confirms that a heartbeat
// on a preempting job returns JSON carrying action="preempt" and the
// current PreemptionEpoch, so the worker knows to stop its process and
// which epoch to echo back when it reports /preempted.
func TestHandleHeartbeat_PreemptingReturnsAction(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()

	// Drive a gang through reserve -> claim -> preempt to get one task into preempting.
	_ = s.RegisterWorker(ctx, store.Worker{
		ID: "w0", ExecAddr: "h:1", Resources: store.ResourceSpec{VRAMMB: 16000},
	})
	gangID, taskIDs := s.AddGang(ctx, store.AddJobParams{
		Command:   "train",
		GangSize:  1,
		Resources: &store.ResourceSpec{VRAMMB: 8000},
	})
	if err := s.ReserveGang(ctx, gangID, map[string]string{taskIDs[0]: "w0"}, 1); err != nil {
		t.Fatalf("ReserveGang: %v", err)
	}
	if _, ok := s.ClaimReservedJob(ctx, "w0"); !ok {
		t.Fatal("ClaimReservedJob: no job found")
	}
	res, err := s.PreemptGang(ctx, gangID, taskIDs[0])
	if err != nil {
		t.Fatalf("PreemptGang: %v", err)
	}
	if !res.Entered {
		t.Fatalf("PreemptGang Entered=false, want true")
	}

	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+taskIDs[0]+"/heartbeat", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp heartbeatResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Action != "preempt" {
		t.Errorf("action = %q, want preempt", resp.Action)
	}
	if resp.PreemptionEpoch != res.Epoch {
		t.Errorf("preemption_epoch = %d, want %d", resp.PreemptionEpoch, res.Epoch)
	}

	// Heartbeat should still refresh last_heartbeat for preempting jobs,
	// workers keep pinging while they shut down.
	job, _ := s.GetJob(ctx, taskIDs[0])
	if job.LastHeartbeat == nil {
		t.Error("expected last_heartbeat to be refreshed for preempting job")
	}
}

// --- DAG dependency tests ---

func TestHandleSubmitJob_WithDependencies(t *testing.T) {
	s := store.NewMemoryStore()
	depID := s.AddJob(context.Background(), store.AddJobParams{Command: "echo dep"})
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

	body := bytes.NewBufferString(`{"command": "echo child", "depends_on": ["` + depID + `"]}`)
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

	if job.Status != store.StatusBlocked {
		t.Errorf("expected status blocked, got %s", job.Status)
	}
	if len(job.DependsOn) != 1 || job.DependsOn[0] != depID {
		t.Errorf("DependsOn = %v, want [%s]", job.DependsOn, depID)
	}
}

func TestHandleSubmitJob_NoDependencies_BackwardCompatible(t *testing.T) {
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

	if job.Status != store.StatusPending {
		t.Errorf("expected status pending, got %s", job.Status)
	}
}

func TestHandleSubmitJob_NonexistentDependency(t *testing.T) {
	srv := newTestServer()

	body := bytes.NewBufferString(`{"command": "echo child", "depends_on": ["nonexistent"]}`)
	r := httptest.NewRequest(http.MethodPost, "/jobs", body)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleJobDone_UnblocksDownstream(t *testing.T) {
	s := store.NewMemoryStore()
	depID := s.AddJob(context.Background(), store.AddJobParams{Command: "echo dep"})
	childID := s.AddJob(context.Background(), store.AddJobParams{Command: "echo child", DependsOn: []string{depID}})
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

	// Claim and complete the dependency via HTTP.
	claimReq := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	claimW := httptest.NewRecorder()
	srv.ServeHTTP(claimW, claimReq)

	if claimW.Code != http.StatusOK {
		t.Fatalf("claim expected 200, got %d", claimW.Code)
	}

	doneReq := httptest.NewRequest(http.MethodPost, "/jobs/"+depID+"/done", nil)
	doneW := httptest.NewRecorder()
	srv.ServeHTTP(doneW, doneReq)

	if doneW.Code != http.StatusOK {
		t.Fatalf("done expected 200, got %d", doneW.Code)
	}

	// Child should now be pending.
	child, _ := s.GetJob(context.Background(), childID)
	if child.Status != store.StatusPending {
		t.Errorf("expected child status pending after dep completed, got %s", child.Status)
	}

	// Child should be claimable.
	claimReq2 := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	claimW2 := httptest.NewRecorder()
	srv.ServeHTTP(claimW2, claimReq2)

	if claimW2.Code != http.StatusOK {
		t.Errorf("expected to claim child, got %d", claimW2.Code)
	}

	var claimed store.Job
	if err := json.NewDecoder(claimW2.Body).Decode(&claimed); err != nil {
		t.Fatalf("failed to decode claimed job: %v", err)
	}
	if claimed.ID != childID {
		t.Errorf("claimed %q, want %q", claimed.ID, childID)
	}
}

func TestHandleJobDone_DoesNotUnblockPartialDeps(t *testing.T) {
	s := store.NewMemoryStore()
	dep1 := s.AddJob(context.Background(), store.AddJobParams{Command: "echo a"})
	dep2 := s.AddJob(context.Background(), store.AddJobParams{Command: "echo b"})
	childID := s.AddJob(context.Background(), store.AddJobParams{Command: "echo child", DependsOn: []string{dep1, dep2}})
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

	// Claim both so we can complete just one.
	s.ClaimJob(context.Background())
	s.ClaimJob(context.Background())

	// Complete only dep1.
	doneReq := httptest.NewRequest(http.MethodPost, "/jobs/"+dep1+"/done", nil)
	doneW := httptest.NewRecorder()
	srv.ServeHTTP(doneW, doneReq)

	if doneW.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", doneW.Code)
	}

	// Child should still be blocked, dep2 is running, not done.
	child, _ := s.GetJob(context.Background(), childID)
	if child.Status != store.StatusBlocked {
		t.Errorf("expected child status blocked (dep2 not done), got %s", child.Status)
	}
}

func TestHandlePipeline_EndToEnd(t *testing.T) {
	s := store.NewMemoryStore()
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")

	// Submit step1 via HTTP.
	body1 := bytes.NewBufferString(`{"command": "echo step1"}`)
	r1 := httptest.NewRequest(http.MethodPost, "/jobs", body1)
	w1 := httptest.NewRecorder()
	srv.ServeHTTP(w1, r1)

	var step1 store.Job
	json.NewDecoder(w1.Body).Decode(&step1)

	// Submit step2 depending on step1.
	body2 := bytes.NewBufferString(`{"command": "echo step2", "depends_on": ["` + step1.ID + `"]}`)
	r2 := httptest.NewRequest(http.MethodPost, "/jobs", body2)
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, r2)

	var step2 store.Job
	json.NewDecoder(w2.Body).Decode(&step2)

	if step2.Status != store.StatusBlocked {
		t.Fatalf("step2 expected blocked, got %s", step2.Status)
	}

	// Submit step3 depending on step2.
	body3 := bytes.NewBufferString(`{"command": "echo step3", "depends_on": ["` + step2.ID + `"]}`)
	r3 := httptest.NewRequest(http.MethodPost, "/jobs", body3)
	w3 := httptest.NewRecorder()
	srv.ServeHTTP(w3, r3)

	var step3 store.Job
	json.NewDecoder(w3.Body).Decode(&step3)

	if step3.Status != store.StatusBlocked {
		t.Fatalf("step3 expected blocked, got %s", step3.Status)
	}

	// Only step1 should be claimable.
	claimReq := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	claimW := httptest.NewRecorder()
	srv.ServeHTTP(claimW, claimReq)

	var claimed store.Job
	json.NewDecoder(claimW.Body).Decode(&claimed)
	if claimed.ID != step1.ID {
		t.Fatalf("expected to claim step1, got %s", claimed.ID)
	}

	// Complete step1 -> step2 should unblock.
	doneReq := httptest.NewRequest(http.MethodPost, "/jobs/"+step1.ID+"/done", nil)
	doneW := httptest.NewRecorder()
	srv.ServeHTTP(doneW, doneReq)

	s2, _ := s.GetJob(context.Background(), step2.ID)
	s3, _ := s.GetJob(context.Background(), step3.ID)
	if s2.Status != store.StatusPending {
		t.Errorf("step2 expected pending, got %s", s2.Status)
	}
	if s3.Status != store.StatusBlocked {
		t.Errorf("step3 expected still blocked, got %s", s3.Status)
	}

	// Claim and complete step2 -> step3 should unblock.
	claimReq2 := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	claimW2 := httptest.NewRecorder()
	srv.ServeHTTP(claimW2, claimReq2)

	doneReq2 := httptest.NewRequest(http.MethodPost, "/jobs/"+step2.ID+"/done", nil)
	doneW2 := httptest.NewRecorder()
	srv.ServeHTTP(doneW2, doneReq2)

	s3, _ = s.GetJob(context.Background(), step3.ID)
	if s3.Status != store.StatusPending {
		t.Errorf("step3 expected pending after step2 done, got %s", s3.Status)
	}

	// Claim and complete step3.
	claimReq3 := httptest.NewRequest(http.MethodGet, "/jobs/next", nil)
	claimW3 := httptest.NewRecorder()
	srv.ServeHTTP(claimW3, claimReq3)

	doneReq3 := httptest.NewRequest(http.MethodPost, "/jobs/"+step3.ID+"/done", nil)
	doneW3 := httptest.NewRecorder()
	srv.ServeHTTP(doneW3, doneReq3)

	// All three should be done.
	for _, id := range []string{step1.ID, step2.ID, step3.ID} {
		job, _ := s.GetJob(context.Background(), id)
		if job.Status != store.StatusDone {
			t.Errorf("job %s expected done, got %s", id, job.Status)
		}
	}
}

// --- Request ID tests ---

func TestRequestID_InResponseHeader(t *testing.T) {
	srv := newTestServer()

	r := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	requestID := w.Header().Get("X-Request-ID")
	if requestID == "" {
		t.Error("expected X-Request-ID header, got empty")
	}
}

func TestRequestID_UniquePerRequest(t *testing.T) {
	srv := newTestServer()

	r1 := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w1 := httptest.NewRecorder()
	srv.ServeHTTP(w1, r1)

	r2 := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, r2)

	id1 := w1.Header().Get("X-Request-ID")
	id2 := w2.Header().Get("X-Request-ID")
	if id1 == id2 {
		t.Errorf("expected unique request IDs, got same: %s", id1)
	}
}

// --- Health endpoint tests ---

func TestHandleHealth_ReturnsInstanceInfo(t *testing.T) {
	srv := newTestServer()

	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp["instance_id"] != "test-inst" {
		t.Errorf("expected instance_id 'test-inst', got %q", resp["instance_id"])
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status 'ok', got %q", resp["status"])
	}
	if resp["uptime"] == "" {
		t.Error("expected non-empty uptime")
	}
}

// --- Worker endpoint tests ---

func TestHandleRegisterWorker_Success(t *testing.T) {
	srv := newTestServer()

	body := bytes.NewBufferString(`{"id": "w-1", "exec_addr": "localhost:9000", "resources": {"vram_mb": 8192}}`)
	r := httptest.NewRequest(http.MethodPost, "/workers/register", body)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d", w.Code)
	}

	var worker store.Worker
	if err := json.NewDecoder(w.Body).Decode(&worker); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if worker.ID != "w-1" {
		t.Errorf("expected worker ID w-1, got %s", worker.ID)
	}
	if worker.ExecAddr != "localhost:9000" {
		t.Errorf("expected exec_addr localhost:9000, got %s", worker.ExecAddr)
	}
	if worker.Status != store.WorkerActive {
		t.Errorf("expected status active, got %s", worker.Status)
	}
}

func TestHandleRegisterWorker_MissingID(t *testing.T) {
	srv := newTestServer()

	body := bytes.NewBufferString(`{"exec_addr": "localhost:9000"}`)
	r := httptest.NewRequest(http.MethodPost, "/workers/register", body)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleWorkerHeartbeat_Success(t *testing.T) {
	srv := newTestServer()

	// Register first
	regBody := bytes.NewBufferString(`{"id": "w-1", "exec_addr": "localhost:9000"}`)
	regReq := httptest.NewRequest(http.MethodPost, "/workers/register", regBody)
	regW := httptest.NewRecorder()
	srv.ServeHTTP(regW, regReq)

	// Heartbeat
	r := httptest.NewRequest(http.MethodPost, "/workers/w-1/heartbeat", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestHandleWorkerHeartbeat_NotFound(t *testing.T) {
	srv := newTestServer()

	r := httptest.NewRequest(http.MethodPost, "/workers/nonexistent/heartbeat", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestHandleListWorkers_Empty(t *testing.T) {
	srv := newTestServer()

	r := httptest.NewRequest(http.MethodGet, "/workers", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var workers []store.Worker
	if err := json.NewDecoder(w.Body).Decode(&workers); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(workers) != 0 {
		t.Errorf("expected empty list, got %d workers", len(workers))
	}
}

func TestHandleListWorkers_WithWorkers(t *testing.T) {
	srv := newTestServer()

	// Register two workers
	for _, id := range []string{"w-1", "w-2"} {
		body := bytes.NewBufferString(`{"id": "` + id + `", "exec_addr": "host:9000"}`)
		r := httptest.NewRequest(http.MethodPost, "/workers/register", body)
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
	}

	r := httptest.NewRequest(http.MethodGet, "/workers", nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var workers []store.Worker
	if err := json.NewDecoder(w.Body).Decode(&workers); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(workers) != 2 {
		t.Errorf("expected 2 workers, got %d", len(workers))
	}
}
