package scheduler

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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

// reportPreempted calls POST /jobs/{id}/preempted?epoch=N, the worker ack
// path a real deployment exercises via SchedulerClient.ReportPreempted.
func (e *gangTestEnv) reportPreempted(jobID string, epoch int) {
	e.t.Helper()
	url := fmt.Sprintf("/jobs/%s/preempted?epoch=%d", jobID, epoch)
	r := httptest.NewRequest(http.MethodPost, url, nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		e.t.Fatalf("report preempted %s epoch=%d: expected 200, got %d: %s", jobID, epoch, w.Code, w.Body.String())
	}
}

// saveCheckpoint posts to POST /jobs/{id}/checkpoint?epoch=N with raw bytes
// and returns the HTTP status code so callers can assert accept/reject cases.
func (e *gangTestEnv) saveCheckpoint(jobID string, epoch int, data []byte) int {
	e.t.Helper()
	url := fmt.Sprintf("/jobs/%s/checkpoint?epoch=%d", jobID, epoch)
	r := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	return w.Code
}

// getCheckpoint fetches GET /jobs/{id}/checkpoint and returns the body and status.
func (e *gangTestEnv) getCheckpoint(jobID string) (int, []byte) {
	e.t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID+"/checkpoint", nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	return w.Code, w.Body.Bytes()
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

// runCompleteDrainedPreemptions runs one pass of the reaper's drain-completion
// logic: force-timeout + gang completion.
func (e *gangTestEnv) runCompleteDrainedPreemptions() {
	e.t.Helper()
	completeDrainedPreemptions(context.Background(), e.store, slog.Default())
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

	// Fail on a gang task whose siblings are still running triggers coordinated
	// drain via PreemptGang. All three tasks move to preempting with the same
	// PreemptionEpoch and a DrainStartedAt stamp: the drain round for this gang.
	e.reportFail(tasks[0].ID)

	var sharedEpoch int
	for i, task := range tasks {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusPreempting {
			t.Errorf("task %s status = %q, want preempting (coordinated drain)", task.ID, job.Status)
		}
		if job.PreemptionEpoch == 0 {
			t.Errorf("task %s preemption_epoch = 0, want nonzero", task.ID)
		}
		if job.DrainStartedAt == nil {
			t.Errorf("task %s drain_started_at = nil, want set", task.ID)
		}
		if i == 0 {
			sharedEpoch = job.PreemptionEpoch
		} else if job.PreemptionEpoch != sharedEpoch {
			t.Errorf("task %s preemption_epoch = %d, want shared %d across gang", task.ID, job.PreemptionEpoch, sharedEpoch)
		}
	}

	// Retry-budget refund: the triggering task keeps its claim-time
	// attempts=1 (real failure); innocent siblings are refunded to 0.
	trigger, _ := e.store.GetJob(ctx, tasks[0].ID)
	if trigger.Attempts != 1 {
		t.Errorf("trigger attempts = %d, want 1 (real failure retained)", trigger.Attempts)
	}
	for _, task := range tasks[1:] {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Attempts != 0 {
			t.Errorf("sibling %s attempts = %d, want 0 (refunded)", task.ID, job.Attempts)
		}
	}
}

// TestGangIntegration_TaskFailureMixedStates covers the case where PreemptGang
// fires while some gang tasks are still reserved (never claimed). Running tasks
// enter coordinated drain; a reserved task, which has no running process to signal,
// bypasses preempting and goes straight to blocked.
func TestGangIntegration_TaskFailureMixedStates(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	// Register 3 workers, submit 3-task gang.
	for i := 0; i < 3; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 3, &store.ResourceSpec{VRAMMB: 8000})

	// Reserve all three, but claim only the first two. Third stays reserved.
	e.runAdmission()
	tasks := e.getGang(gangID)
	e.claimJob(tasks[0].WorkerID)
	e.claimJob(tasks[1].WorkerID)

	// Fail the first running task.
	e.reportFail(tasks[0].ID)

	// The two running tasks -> preempting (drain started).
	for _, task := range tasks[:2] {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusPreempting {
			t.Errorf("running task %s status = %q, want preempting", task.ID, job.Status)
		}
	}
	// The reserved task -> blocked, with worker assignment cleared.
	reserved, _ := e.store.GetJob(ctx, tasks[2].ID)
	if reserved.Status != store.StatusBlocked {
		t.Errorf("reserved task status = %q, want blocked (no process to drain)", reserved.Status)
	}
	if reserved.WorkerID != "" {
		t.Errorf("reserved task worker_id = %q, want cleared", reserved.WorkerID)
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

// TestGangIntegration_DrainCompletesToBlocked exercises the full preemption round-trip:
// trigger drain -> workers ack -> reaper completes -> gang is re-admissible.
func TestGangIntegration_DrainCompletesToBlocked(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	// Register 3 workers, submit 3-task gang, reserve + claim all.
	for i := 0; i < 3; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 3, &store.ResourceSpec{VRAMMB: 8000})
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		e.claimJob(task.WorkerID)
	}

	// Trigger drain on task 0, all three go to preempting.
	e.reportFail(tasks[0].ID)
	preempting := e.getGang(gangID)
	epoch := preempting[0].PreemptionEpoch

	// Simulate all workers acking their preempted exit.
	for _, task := range preempting {
		if err := e.store.MarkPreempted(ctx, task.ID, epoch); err != nil {
			t.Fatalf("MarkPreempted(%s): %v", task.ID, err)
		}
	}

	// A first reaper pass while drain isn't complete should be a no-op;
	// but after acks, the first pass should complete the gang to blocked.
	e.runCompleteDrainedPreemptions()

	for _, task := range tasks {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusBlocked {
			t.Errorf("task %s status = %q, want blocked after drain completion", task.ID, job.Status)
		}
		if job.DrainStartedAt != nil {
			t.Errorf("task %s drain_started_at should be cleared", task.ID)
		}
		if job.WorkerID != "" {
			t.Errorf("task %s worker_id = %q, want cleared", task.ID, job.WorkerID)
		}
	}

	// Admission should re-reserve the gang on the next tick.
	e.runAdmission()
	for _, task := range e.getGang(gangID) {
		if task.Status != store.StatusReserved {
			t.Errorf("task %s status = %q, want reserved after re-admission", task.ID, task.Status)
		}
	}
}

// TestGangIntegration_StuckGangRecovery drives the full preemption
// round-trip the way a real deployment does: workers observe the preempt
// action on their heartbeat response, call /preempted with the scheduler's
// epoch, the reaper completes the gang, and admission places it again on the
// next tick. This is the scenario the stuck-gang fix is meant to handle end to end.
func TestGangIntegration_StuckGangRecovery(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 3, &store.ResourceSpec{VRAMMB: 8000})

	// Admission reserves; each worker claims its task.
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		e.claimJob(task.WorkerID)
	}

	// Task 0 fails -> gang enters drain, all three become preempting.
	e.reportFail(tasks[0].ID)
	drained := e.getGang(gangID)
	epoch := drained[0].PreemptionEpoch
	for _, task := range drained {
		if task.Status != store.StatusPreempting {
			t.Fatalf("task %s status = %q, want preempting before acks", task.ID, task.Status)
		}
	}

	// Each worker reports /preempted with the scheduler-supplied epoch.
	for _, task := range drained {
		e.reportPreempted(task.ID, epoch)
	}

	// Post-ack, every task should be in preempted; nothing has been
	// completed yet because the reaper hasn't run.
	for _, task := range tasks {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusPreempted {
			t.Errorf("task %s status = %q, want preempted after ack", task.ID, job.Status)
		}
	}

	// Reaper tick: no task is running/preempting anymore, so the gang
	// completes to blocked (retries remain).
	e.runCompleteDrainedPreemptions()

	for _, task := range tasks {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusBlocked {
			t.Errorf("task %s status = %q, want blocked after completion", task.ID, job.Status)
		}
	}

	// Admission should re-reserve the gang on the next tick,
	// proving the gang is no longer stuck.
	e.runAdmission()
	for _, task := range e.getGang(gangID) {
		if task.Status != store.StatusReserved {
			t.Errorf("task %s status = %q, want reserved after re-admission", task.ID, task.Status)
		}
	}
}

// TestGangIntegration_CheckpointSaveAndReplay covers the checkpoint
// round-trip: a job saves bytes during drain, the gang completes and
// re-admits, and the re-claimed task sees the saved bytes surfaced as
// CHECKPOINT_DATA (base64) in its env.
func TestGangIntegration_CheckpointSaveAndReplay(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 3, &store.ResourceSpec{VRAMMB: 8000})
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		e.claimJob(task.WorkerID)
	}

	// Drain starts. Before workers ack, task 0 uploads a checkpoint.
	e.reportFail(tasks[0].ID)
	drained := e.getGang(gangID)
	epoch := drained[0].PreemptionEpoch

	checkpoint := []byte(`{"step":42,"loss":0.123}`)
	if code := e.saveCheckpoint(tasks[0].ID, epoch, checkpoint); code != http.StatusOK {
		t.Fatalf("SaveCheckpoint: got %d, want 200", code)
	}

	// GET should echo the bytes back. Compare semantically because the
	// Postgres JSONB path canonicalizes whitespace (tested elsewhere);
	// the memory backend preserves bytes exactly, which is what this
	// integration test uses.
	code, got := e.getCheckpoint(tasks[0].ID)
	if code != http.StatusOK {
		t.Fatalf("GetCheckpoint: got %d, want 200", code)
	}
	if string(got) != string(checkpoint) {
		t.Errorf("GetCheckpoint body = %q, want %q", got, checkpoint)
	}

	// Finish drain and re-admit.
	for _, task := range drained {
		e.reportPreempted(task.ID, epoch)
	}
	e.runCompleteDrainedPreemptions()
	e.runAdmission()

	// Re-claim task 0 via its newly assigned worker. The env should
	// carry CHECKPOINT_DATA containing the base64 encoding of the
	// originally uploaded bytes. GANG_* values must still be present.
	reReserved, _ := e.store.GetJob(ctx, tasks[0].ID)
	if reReserved.Status != store.StatusReserved {
		t.Fatalf("task 0 status = %q, want reserved after re-admission", reReserved.Status)
	}
	claimed := e.claimJob(reReserved.WorkerID)
	if claimed == nil {
		t.Fatal("re-claim: no job returned")
	}
	if claimed.ID != tasks[0].ID {
		t.Fatalf("claimed ID = %q, want original task 0 %q", claimed.ID, tasks[0].ID)
	}
	raw, ok := claimed.Env["CHECKPOINT_DATA"]
	if !ok {
		t.Fatal("claimed job env missing CHECKPOINT_DATA")
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("decode CHECKPOINT_DATA: %v", err)
	}
	if string(decoded) != string(checkpoint) {
		t.Errorf("decoded checkpoint = %q, want %q", decoded, checkpoint)
	}
	// Gang env is still populated for the re-claim.
	if claimed.Env["GANG_ID"] != gangID {
		t.Errorf("GANG_ID = %q, want %q", claimed.Env["GANG_ID"], gangID)
	}
}

// TestHandleSaveCheckpoint_RejectsNonPreempting and
// TestHandleSaveCheckpoint_RejectsWrongEpoch guard the checkpoint
// semantics: saves are only valid while the job is in preempting and
// must carry the current PreemptionEpoch.
func TestHandleSaveCheckpoint_RejectsNonPreempting(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	// A plain running job (not a gang), status is running, not preempting.
	id := e.store.AddJob(ctx, store.AddJobParams{Command: "echo hi"})
	e.claimJob("")

	// 409 because status != preempting.
	code := e.saveCheckpoint(id, 1, []byte(`{"step":1}`))
	if code != http.StatusConflict {
		t.Errorf("SaveCheckpoint on running job: got %d, want 409", code)
	}
}

func TestHandleSaveCheckpoint_RejectsWrongEpoch(t *testing.T) {
	e := newGangTestEnv(t)

	for i := 0; i < 2; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 2, &store.ResourceSpec{VRAMMB: 8000})
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		e.claimJob(task.WorkerID)
	}
	e.reportFail(tasks[0].ID)

	// Stale epoch 999 must be rejected with 409.
	code := e.saveCheckpoint(tasks[0].ID, 999, []byte(`{"step":1}`))
	if code != http.StatusConflict {
		t.Errorf("SaveCheckpoint with stale epoch: got %d, want 409", code)
	}
}

// TestHandleJobPreempted_EpochMismatchRejected ensures stale acks from
// an earlier drain round cannot flip a task back to preempted.
func TestHandleJobPreempted_EpochMismatchRejected(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 2, &store.ResourceSpec{VRAMMB: 8000})
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		e.claimJob(task.WorkerID)
	}
	e.reportFail(tasks[0].ID)

	// Stale epoch: scheduler must reject with 409 and not mutate state.
	url := fmt.Sprintf("/jobs/%s/preempted?epoch=999", tasks[0].ID)
	r := httptest.NewRequest(http.MethodPost, url, nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusConflict {
		t.Errorf("expected 409 on epoch mismatch, got %d: %s", w.Code, w.Body.String())
	}
	job, _ := e.store.GetJob(ctx, tasks[0].ID)
	if job.Status != store.StatusPreempting {
		t.Errorf("task should remain preempting after rejected ack, got %q", job.Status)
	}
}

// TestHandleJobPreempted_MissingEpoch rejects calls without the
// required query parameter so silent-zero acks can't slip through.
func TestHandleJobPreempted_MissingEpoch(t *testing.T) {
	e := newGangTestEnv(t)
	_, _ = e.submitGang("train", 2, &store.ResourceSpec{VRAMMB: 8000})

	// Use any existing job id; we expect the handler to reject on
	// missing epoch before it even touches the store.
	tasks := e.getGang(e.store.ListJobs(context.Background())[0].GangID)
	r := httptest.NewRequest(http.MethodPost, "/jobs/"+tasks[0].ID+"/preempted", nil)
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing epoch, got %d: %s", w.Code, w.Body.String())
	}
}

// TestGangIntegration_DrainForceTimeout exercises the reaper's force-timeout
// path: a gang enters drain, no worker ever acks, and after preemptTimeout
// the reaper force-drains the stuck tasks so the gang can complete.
func TestGangIntegration_DrainForceTimeout(t *testing.T) {
	e := newGangTestEnv(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		wid := "w" + string(rune('0'+i))
		e.registerWorker(wid, "host:800"+string(rune('0'+i)), store.ResourceSpec{VRAMMB: 16000})
	}
	gangID, _ := e.submitGang("train", 3, &store.ResourceSpec{VRAMMB: 8000})
	e.runAdmission()
	tasks := e.getGang(gangID)
	for _, task := range tasks {
		e.claimJob(task.WorkerID)
	}

	// Drain starts; no worker acks.
	e.reportFail(tasks[0].ID)

	// First pass (within grace): nothing should change.
	e.runCompleteDrainedPreemptions()
	for _, task := range tasks {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusPreempting {
			t.Errorf("task %s status = %q, want still preempting inside grace window", task.ID, job.Status)
		}
	}

	// Backdate DrainStartedAt past the timeout to simulate a stuck drain.
	stale := time.Now().Add(-2 * time.Minute)
	for _, task := range tasks {
		e.store.SetDrainStartedAt(task.ID, stale)
	}

	// Two reaper passes: first force-drains preempting -> preempted
	// and then completes the gang on pass 2 in the same call.
	e.runCompleteDrainedPreemptions()

	for _, task := range tasks {
		job, _ := e.store.GetJob(ctx, task.ID)
		if job.Status != store.StatusBlocked {
			t.Errorf("task %s status = %q, want blocked after force-drain timeout", task.ID, job.Status)
		}
	}
}
