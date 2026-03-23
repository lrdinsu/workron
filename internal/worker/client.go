package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/lrdinsu/workron/internal/store"
)

// SchedulerClient communicates with the scheduler over HTTP.
// It implements JobSource so the Worker can use it interchangeably with a direct store.
type SchedulerClient struct {
	baseURL    string
	httpClient *http.Client
}

// NewSchedulerClient creates a client pointing at the given scheduler URL.
func NewSchedulerClient(baseURL string) *SchedulerClient {
	return &SchedulerClient{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

// ClaimJob calls GET /jobs/next on the scheduler to atomically claim a pending job.
// Returns (nil, false) if no jobs are available (204 No Content).
func (c *SchedulerClient) ClaimJob(ctx context.Context) (*store.Job, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/jobs/next", nil)
	if err != nil {
		return nil, false
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, false
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("[client] failed to close response body: %v", closeErr)
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
			log.Printf("[client] failed to close response body: %v", closeErr)
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
			log.Printf("[client] failed to close response body: %v", closeErr)
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
		fmt.Printf("[client] warning: %v\n", err)
	}
}

// SendHeartbeat tells the scheduler the worker is still alive and working on this job.
func (c *SchedulerClient) SendHeartbeat(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/jobs/"+id+"/heartbeat", nil)
	if err != nil {
		return fmt.Errorf("heartbeat for job %s: %w", id, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("heartbeat for job %s: %w", id, err)
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("[client] failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("heartbeat for job %s: unexpected status %d", id, resp.StatusCode)
	}

	return nil
}
