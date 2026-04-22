package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/lrdinsu/workron/internal/store"
)

// SchedulerClient communicates with the scheduler over HTTP.
// It implements JobSource so the Worker can use it interchangeably with a direct store.
type SchedulerClient struct {
	baseURL    string
	workerID   string // included in ClaimJob requests for gang reservation matching
	httpClient *http.Client
	logger     *slog.Logger
}

// NewSchedulerClient creates a client pointing at the given scheduler URL.
// workerID is sent with ClaimJob requests so the scheduler can match reserved gang tasks to this worker.
func NewSchedulerClient(baseURL string, workerID string, logger *slog.Logger) *SchedulerClient {
	return &SchedulerClient{
		baseURL:    baseURL,
		workerID:   workerID,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

// ClaimJob calls GET /jobs/next on the scheduler to atomically claim a pending job.
// If workerID is set, appends ?worker_id= so the scheduler can prioritize reserved gang tasks for this worker.
// Returns (nil, false) if no jobs are available (204 No Content).
func (c *SchedulerClient) ClaimJob(ctx context.Context) (*store.Job, bool) {
	url := c.baseURL + "/jobs/next"
	if c.workerID != "" {
		url += "?worker_id=" + c.workerID
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusNoContent {
		return nil, false
	}

	if resp.StatusCode != http.StatusOK {
		return nil, false
	}

	var job store.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, false
	}

	return &job, true
}

// ReportDone tells the scheduler that a job completed successfully.
func (c *SchedulerClient) ReportDone(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jobs/"+id+"/done", nil)
	if err != nil {
		return fmt.Errorf("report done for job %s: %w", id, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("report done for job %s: %w", id, err)
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("report done for job %s: unexpected status %d", id, resp.StatusCode)
	}

	return nil
}

// ReportFail tells the scheduler that a job failed.
// The scheduler decides whether to retry or mark as permanently failed.
func (c *SchedulerClient) ReportFail(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jobs/"+id+"/fail", nil)
	if err != nil {
		return fmt.Errorf("report fail for job %s: %w", id, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("report fail for job %s: %w", id, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("report fail for job %s: unexpected status %d", id, resp.StatusCode)
	}

	return nil
}

// UpdateJobStatus maps done/failed status to the appropriate HTTP call.
// This satisfies the JobSource interface, allowing the worker to use
// the same code path for both direct store access and HTTP communication.
func (c *SchedulerClient) UpdateJobStatus(ctx context.Context, id string, status store.JobStatus) {
	var err error

	switch status {
	case store.StatusDone:
		err = c.ReportDone(ctx, id)
	case store.StatusFailed, store.StatusPending:
		// Both "permanently failed" and "retry" are handled server-side.
		// The worker just reports failure; the scheduler decides the outcome.
		err = c.ReportFail(ctx, id)
	}

	if err != nil {
		// Log but don't crash, the scheduler will eventually time out the job
		c.logger.Warn("failed to report job status", "error", err)
	}
}

// SendHeartbeat tells the scheduler the worker is still alive and working
// on this job. For ordinary running jobs the scheduler returns 200 with
// an empty body; for preempting jobs the body is JSON carrying
// {"action":"preempt","preemption_epoch":N}, which the worker must honor
// by stopping the process and reporting /jobs/{id}/preempted with the
// same epoch.
func (c *SchedulerClient) SendHeartbeat(ctx context.Context, id string) (store.HeartbeatResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jobs/"+id+"/heartbeat", nil)
	if err != nil {
		return store.HeartbeatResult{}, fmt.Errorf("heartbeat for job %s: %w", id, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return store.HeartbeatResult{}, fmt.Errorf("heartbeat for job %s: %w", id, err)
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return store.HeartbeatResult{}, fmt.Errorf("heartbeat for job %s: unexpected status %d", id, resp.StatusCode)
	}

	// Empty body is the common case (running jobs). Decoding it with
	// json.Decoder would return io.EOF, so peek at the Content-Length
	// first byte before attempting.
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return store.HeartbeatResult{}, fmt.Errorf("heartbeat for job %s: read body: %w", id, err)
	}
	if len(body) == 0 {
		return store.HeartbeatResult{}, nil
	}

	var result store.HeartbeatResult
	if err := json.Unmarshal(body, &result); err != nil {
		// Treat malformed body as no action rather than failing the heartbeat,
		// the worker should keep pinging so the scheduler can retry the signal
		// on its next response.
		c.logger.Warn("heartbeat response body not JSON, ignoring",
			"job_id", id, "error", err, "body_len", len(body))
		return store.HeartbeatResult{}, nil
	}
	return result, nil
}

// SaveCheckpoint uploads opaque bytes to the scheduler for this job,
// tagged with the current PreemptionEpoch. Only accepted while the
// job is in preempting, a running-state save or a stale epoch is
// rejected with 409. The next claim of this job surfaces these bytes
// as CHECKPOINT_DATA (base64) in the job env.
func (c *SchedulerClient) SaveCheckpoint(ctx context.Context, id string, epoch int, data []byte) error {
	url := fmt.Sprintf("%s/jobs/%s/checkpoint?epoch=%d", c.baseURL, id, epoch)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("save checkpoint for job %s: %w", id, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("save checkpoint for job %s: %w", id, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("save checkpoint for job %s: unexpected status %d", id, resp.StatusCode)
	}
	return nil
}

// ReportPreempted tells the scheduler that a preempted job's process
// has exited. The epoch must match the PreemptionEpoch the scheduler
// supplied on the preempt heartbeat response; a mismatch indicates a
// stale round and the scheduler rejects it with 409.
func (c *SchedulerClient) ReportPreempted(ctx context.Context, id string, epoch int) error {
	url := fmt.Sprintf("%s/jobs/%s/preempted?epoch=%d", c.baseURL, id, epoch)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("report preempted for job %s: %w", id, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("report preempted for job %s: %w", id, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("report preempted for job %s: unexpected status %d", id, resp.StatusCode)
	}
	return nil
}

// RegisterWorker announces this worker to the scheduler. Called once on startup
// before the worker begins polling for jobs. Without registration the worker
// is invisible to gang admission (which only considers registered, active workers),
// though it can still claim regular pending jobs.
func (c *SchedulerClient) RegisterWorker(ctx context.Context, w store.Worker) error {
	body, err := json.Marshal(w)
	if err != nil {
		return fmt.Errorf("marshal worker: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/workers/register", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("register worker %s: %w", w.ID, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("register worker %s: %w", w.ID, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("register worker %s: unexpected status %d", w.ID, resp.StatusCode)
	}
	return nil
}

// SendWorkerHeartbeat refreshes the worker's last_heartbeat on the scheduler.
// Must be called periodically (more often than the scheduler's workerStaleTimeout of 60s);
// otherwise the reaper marks the worker offline and gang admission stops considering it.
func (c *SchedulerClient) SendWorkerHeartbeat(ctx context.Context) error {
	if c.workerID == "" {
		return fmt.Errorf("worker heartbeat: no worker_id set on client")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/workers/"+c.workerID+"/heartbeat", nil)
	if err != nil {
		return fmt.Errorf("worker heartbeat %s: %w", c.workerID, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("worker heartbeat %s: %w", c.workerID, err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker heartbeat %s: unexpected status %d", c.workerID, resp.StatusCode)
	}
	return nil
}
