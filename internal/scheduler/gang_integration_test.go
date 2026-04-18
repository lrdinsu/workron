package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lrdinsu/workron/internal/metrics"
	"github.com/lrdinsu/workron/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// gangTestEnv bundles a server, store, and helpers for gang integration tests.
type gangTestEnv struct {
	srv   *Server
	store *store.MemoryStore
	t     *testing.T
}

func newGangTestEnv(t *testing.T) *gangTestEnv {
	t.Helper()
	s := store.NewMemoryStore()
	srv := NewServer(s, slog.Default(), metrics.NewMetrics(), prometheus.NewRegistry(), "test-inst")
	return &gangTestEnv{srv: srv, store: s, t: t}
}

// registerWorker registers a worker via POST /workers/register.
func (e *gangTestEnv) registerWorker(id, execAddr string, resources store.ResourceSpec) {
	e.t.Helper()
	body, _ := json.Marshal(store.Worker{ID: id, ExecAddr: execAddr, Resources: resources})
	r := httptest.NewRequest(http.MethodPost, "/workers/register", bytes.NewReader(body))
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		e.t.Fatalf("register worker %s: expected 201, got %d: %s", id, w.Code, w.Body.String())
	}
}

// submitGang submits a gang via POST /jobs and returns the gang_id and task IDs.
func (e *gangTestEnv) submitGang(command string, gangSize int, resources *store.ResourceSpec) (string, []string) {
	e.t.Helper()
	req := submitJobRequest{Command: command, GangSize: gangSize, Resources: resources}
	body, _ := json.Marshal(req)
	r := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		e.t.Fatalf("submit gang: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var resp gangSubmitResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		e.t.Fatalf("decode gang response: %v", err)
	}
	return resp.GangID, resp.Tasks
}

// claimJob calls GET /jobs/next with optional worker_id and returns the job or nil.
func (e *gangTestEnv) claimJob(workerID string) *store.Job {
	e.t.Helper()
	url := "/jobs/next"
	if workerID != "" {
		url += "?worker_id=" + workerID
	}
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code == http.StatusNoContent {
		return nil
	}
	if w.Code != http.StatusOK {
		e.t.Fatalf("claim job: expected 200 or 204, got %d: %s", w.Code, w.Body.String())
	}
	var job store.Job
	if err := json.NewDecoder(w.Body).Decode(&job); err != nil {
		e.t.Fatalf("decode claimed job: %v", err)
	}
	return &job
}

// reportDone calls POST /jobs/{id}/done.
func (e *gangTestEnv) reportDone(jobID string) {
	e.t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+jobID+"/done", nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		e.t.Fatalf("report done %s: expected 200, got %d: %s", jobID, w.Code, w.Body.String())
	}
}

// reportFail calls POST /jobs/{id}/fail.
func (e *gangTestEnv) reportFail(jobID string) {
	e.t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+jobID+"/fail", nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		e.t.Fatalf("report fail %s: expected 200, got %d: %s", jobID, w.Code, w.Body.String())
	}
}

// getGang calls GET /gangs/{gang_id} and returns the tasks.
func (e *gangTestEnv) getGang(gangID string) []*store.Job {
	e.t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/gangs/"+gangID, nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		e.t.Fatalf("get gang %s: expected 200, got %d: %s", gangID, w.Code, w.Body.String())
	}
	var tasks []*store.Job
	if err := json.NewDecoder(w.Body).Decode(&tasks); err != nil {
		e.t.Fatalf("decode gang tasks: %v", err)
	}
	return tasks
}

// runAdmission runs one gang admission cycle.
func (e *gangTestEnv) runAdmission() {
	e.t.Helper()
	ctx := context.Background()
	RunGangAdmissionOnce(ctx, e.store, e.store, e.store, slog.Default())
}

// --- Integration Tests ---

func TestGangIntegration_FullLifecycle(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	// 1. Register 4 workers
	for i := 0; i < 4; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host"+string(rune('0'+i))+":8000", store.ResourceSpec{VRAMMB: 16000})
	}

	// 2. Submit a 4-task gang
	gangID, taskIDs := e.submitGang("torchrun train.py", 4, &store.ResourceSpec{VRAMMB: 8000})
	if len(taskIDs) != 4 {
		t.Fatalf("expected 4 tasks, got %d", len(taskIDs))
	}

	// 3. All tasks should be blocked
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		if task.Status != store.StatusBlocked {
			t.Errorf("task %s status = %q, want blocked", task.ID, task.Status)
		}
	}

	// 4. Run admission, should reserve all 4 tasks
	e.runAdmission()
	tasks = e.getGang(gangID)
	for _, task := range tasks {
		if task.Status != store.StatusReserved {
			t.Errorf("task %s status = %q, want reserved after admission", task.ID, task.Status)
		}
		if task.WorkerID == "" {
			t.Errorf("task %s has no worker_id after reservation", task.ID)
		}
	}

	// 5. Each worker claims its reserved task
	claimedJobs := make(map[string]*store.Job)
	for _, task := range tasks {
		job := e.claimJob(task.WorkerID)
		if job == nil {
			t.Fatalf("worker %s could not claim reserved task", task.WorkerID)
		}
		if job.Status != store.StatusRunning {
			t.Errorf("claimed job status = %q, want running", job.Status)
		}
		// Verify gang env vars
		if job.Env["GANG_ID"] != gangID {
			t.Errorf("GANG_ID = %q, want %q", job.Env["GANG_ID"], gangID)
		}
		if job.Env["GANG_SIZE"] != "4" {
			t.Errorf("GANG_SIZE = %q, want 4", job.Env["GANG_SIZE"])
		}
		if job.Env["GANG_PEERS"] == "" {
			t.Error("GANG_PEERS is empty")
		}
		claimedJobs[job.ID] = job
	}

	// 6. Complete all tasks
	for id := range claimedJobs {
		e.reportDone(id)
	}

	// 7. Verify all done
	for _, id := range taskIDs {
		job, _ := e.store.GetJob(ctx, id)
		if job.Status != store.StatusDone {
			t.Errorf("task %s status = %q, want done", id, job.Status)
		}
	}
}

func TestGangIntegration_InsufficientWorkers(t *testing.T) {
	e := newGangTestEnv(t)

	// Register only 3 workers
	for i := 0; i < 3; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}

	// Submit a 4-task gang
	gangID, _ := e.submitGang("train", 4, &store.ResourceSpec{VRAMMB: 8000})

	// Run admission, should NOT reserve (only 3 workers for 4 tasks)
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		if task.Status != store.StatusBlocked {
			t.Errorf("task %s status = %q, want blocked (insufficient workers)", task.ID, task.Status)
		}
	}

	// Register 4th worker
	e.registerWorker("w3", "host:8003", store.ResourceSpec{VRAMMB: 16000})

	// Run admission again, should now reserve
	e.runAdmission()
	tasks = e.getGang(gangID)
	for _, task := range tasks {
		if task.Status != store.StatusReserved {
			t.Errorf("task %s status = %q, want reserved after 4th worker registered", task.ID, task.Status)
		}
	}
}

func TestGangIntegration_ReservationTimeout(t *testing.T) {
	e := newGangTestEnv(t)

	// Register 4 workers, submit 4-task gang
	for i := 0; i < 4; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, taskIDs := e.submitGang("train", 4, &store.ResourceSpec{VRAMMB: 8000})

	// Reserve via admission
	e.runAdmission()

	// Backdate reserved_at to simulate timeout
	staleTime := time.Now().Add(-2 * time.Minute)
	for _, id := range taskIDs {
		e.store.SetReservedAt(id, staleTime)
	}

	// Run admission again, should rollback stale reservations first
	e.runAdmission()

	// Tasks should be reserved again (rolled back then re-reserved in same cycle)
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		if task.Status != store.StatusReserved {
			t.Errorf("task %s status = %q, want reserved (re-reserved after rollback)", task.ID, task.Status)
		}
	}
}

func TestGangIntegration_TaskFailure(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	// Register 3 workers, submit 3-task gang
	for i := 0; i < 3; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 3, &store.ResourceSpec{VRAMMB: 8000})

	// Reserve and claim all tasks
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		e.claimJob(task.WorkerID)
	}

	// Fail one task, siblings are running, should be left untouched.
	// handleJobFail sets the individual task to pending (retry),
	// then FailGang changes pending siblings to blocked. So the failed
	// task ends up blocked (it was pending, which is in the affected set).
	e.reportFail(tasks[0].ID)

	job0, _ := e.store.GetJob(ctx, tasks[0].ID)
	if job0.Status != store.StatusBlocked {
		t.Errorf("failed task status = %q, want blocked (retry via FailGang)", job0.Status)
	}

	// Running siblings should be untouched (no preemption)
	for _, task := range tasks[1:] {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusRunning {
			t.Errorf("running sibling %s status = %q, want running (untouched)", task.ID, job.Status)
		}
	}
}

func TestGangIntegration_TwoGangsLargestFirst(t *testing.T) {
	e := newGangTestEnv(t)

	// Register 4 workers with tight VRAM, each can only run ONE gang task
	for i := 0; i < 4; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 8000})
	}

	// Submit gang A (size 3) first, then gang B (size 4)
	// Each task needs 8000 VRAM = worker's full capacity
	gangA, _ := e.submitGang("train-small", 3, &store.ResourceSpec{VRAMMB: 8000})
	gangB, _ := e.submitGang("train-big", 4, &store.ResourceSpec{VRAMMB: 8000})

	// Run admission, gang B (larger) should be placed first, consuming all 4 workers.
	// Gang A (size 3) can't fit because no workers have remaining capacity.
	e.runAdmission()

	tasksB := e.getGang(gangB)
	for _, task := range tasksB {
		if task.Status != store.StatusReserved {
			t.Errorf("gang B task %s status = %q, want reserved (largest first)", task.ID, task.Status)
		}
	}

	tasksA := e.getGang(gangA)
	for _, task := range tasksA {
		if task.Status != store.StatusBlocked {
			t.Errorf("gang A task %s status = %q, want blocked (no capacity left)", task.ID, task.Status)
		}
	}
}

func TestGangIntegration_NonGangJobsUnaffected(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	// Register 2 workers
	e.registerWorker("w0", "host:8000", store.ResourceSpec{VRAMMB: 16000})
	e.registerWorker("w1", "host:8001", store.ResourceSpec{VRAMMB: 16000})

	// Submit a regular job and a gang
	regularID := e.store.AddJob(ctx, store.AddJobParams{Command: "echo hello"})
	e.submitGang("train", 2, &store.ResourceSpec{VRAMMB: 8000})

	// Regular job should be claimable without worker_id (not blocked by gang)
	job := e.claimJob("")
	if job == nil {
		t.Fatal("expected to claim regular job")
	}
	if job.ID != regularID {
		t.Errorf("claimed job ID = %q, want %q (the regular job)", job.ID, regularID)
	}

	// Gang tasks should still be blocked (not claimable via regular ClaimJob)
	job2 := e.claimJob("")
	if job2 != nil {
		t.Errorf("should not claim gang task via regular ClaimJob, got %s with status %s", job2.ID, job2.Status)
	}
}
