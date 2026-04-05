package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/lrdinsu/workron/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

func TestJobGaugeCollector_CountsByStatus(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryStore()

	// Create jobs in various states.
	// 2 pending (no deps, not yet claimed)
	s.AddJob(ctx, store.AddJobParams{Command: "echo pending1"})
	s.AddJob(ctx, store.AddJobParams{Command: "echo pending2"})

	// 1 running (claimed)
	s.AddJob(ctx, store.AddJobParams{Command: "echo running"})
	s.ClaimJob(ctx)

	// 1 blocked (has dependency)
	depID := s.AddJob(ctx, store.AddJobParams{Command: "echo dep"})
	s.AddJob(ctx, store.AddJobParams{Command: "echo blocked", DependsOn: []string{depID}})

	// Register collector with a fresh registry.
	registry := prometheus.NewRegistry()
	collector := NewJobGaugeCollector(s)
	registry.MustRegister(collector)

	// Gather metrics.
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	// Parse into a map for easy lookup.
	got := make(map[string]float64)
	for _, f := range families {
		got[f.GetName()] = f.GetMetric()[0].GetGauge().GetValue()
	}

	// 2 pending + 1 dep (pending) = 3 pending, 1 running, 1 blocked
	if got["workron_jobs_pending"] != 3 {
		t.Errorf("workron_jobs_pending = %v, want 3", got["workron_jobs_pending"])
	}
	if got["workron_jobs_running"] != 1 {
		t.Errorf("workron_jobs_running = %v, want 1", got["workron_jobs_running"])
	}
	if got["workron_jobs_blocked"] != 1 {
		t.Errorf("workron_jobs_blocked = %v, want 1", got["workron_jobs_blocked"])
	}
}

func TestJobGaugeCollector_EmptyStore(t *testing.T) {
	s := store.NewMemoryStore()

	registry := prometheus.NewRegistry()
	collector := NewJobGaugeCollector(s)
	registry.MustRegister(collector)

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	for _, f := range families {
		val := f.GetMetric()[0].GetGauge().GetValue()
		if val != 0 {
			t.Errorf("%s = %v, want 0", f.GetName(), val)
		}
	}
}

func TestMetrics_RegisterAndIncrement(t *testing.T) {
	registry := prometheus.NewRegistry()
	m := NewMetrics()
	m.Register(registry)

	// Increment counters.
	m.JobsSubmitted.Inc()
	m.JobsSubmitted.Inc()
	m.JobsClaimed.Inc()
	m.JobsCompleted.Inc()
	m.JobsFailed.Inc()
	m.JobsRetried.Inc()
	m.JobsReaped.WithLabelValues("requeued").Inc()
	m.JobsReaped.WithLabelValues("failed").Inc()

	// Observe histograms.
	m.JobExecDuration.Observe(1.5)
	m.JobQueueWait.Observe(0.25)

	// Gather and verify output contains expected metric names.
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	var buf strings.Builder
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, f := range families {
		if err := enc.Encode(f); err != nil {
			t.Fatalf("failed to encode metric family: %v", err)
		}
	}
	output := buf.String()

	expected := []string{
		"workron_jobs_submitted_total 2",
		"workron_jobs_claimed_total 1",
		"workron_jobs_completed_total 1",
		"workron_jobs_failed_total 1",
		"workron_jobs_retried_total 1",
		`workron_jobs_reaped_total{outcome="requeued"} 1`,
		`workron_jobs_reaped_total{outcome="failed"} 1`,
		"workron_job_execution_duration_seconds_count 1",
		"workron_job_queue_wait_seconds_count 1",
	}

	for _, line := range expected {
		if !strings.Contains(output, line) {
			t.Errorf("metrics output missing %q", line)
		}
	}
}
