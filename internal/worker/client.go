package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// SendHeartbeat tells the scheduler the worker is still alive and working on this job.
// The returned string is a heartbeat action (empty for now, future will return "preempt"
// via the HTTP response body when the scheduler wants the worker to stop).
func (c *SchedulerClient) SendHeartbeat(ctx context.Context, id string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jobs/"+id+"/heartbeat", nil)
	if err != nil {
		return "", fmt.Errorf("heartbeat for job %s: %w", id, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("heartbeat for job %s: %w", id, err)
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("heartbeat for job %s: unexpected status %d", id, resp.StatusCode)
	}

	return "", nil
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
