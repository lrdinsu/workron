package metrics

import (
	"context"

	"github.com/lrdinsu/workron/internal/store"
	"github.com/prometheus/client_golang/prometheus"
)

// JobGaugeCollector implements prometheus.Collector to report current
// job counts by status. It queries the store on each Prometheus scrape
// rather than tracking increments/decrements, ensuring gauges are always
// accurate regardless of which store backend is in use.
type JobGaugeCollector struct {
	store       store.JobStore
	pendingDesc *prometheus.Desc
	runningDesc *prometheus.Desc
	blockedDesc *prometheus.Desc
}

// NewJobGaugeCollector creates a collector that queries the given store
// for job status counts on each scrape.
func NewJobGaugeCollector(s store.JobStore) *JobGaugeCollector {
	return &JobGaugeCollector{
		store: s,
		pendingDesc: prometheus.NewDesc(
			"workron_jobs_pending",
			"Number of jobs currently in pending status.",
			nil, nil,
		),
		runningDesc: prometheus.NewDesc(
			"workron_jobs_running",
			"Number of jobs currently in running status.",
			nil, nil,
		),
		blockedDesc: prometheus.NewDesc(
			"workron_jobs_blocked",
			"Number of jobs currently in blocked status (waiting for dependencies).",
			nil, nil,
		),
	}
}

// Describe sends the descriptor for each gauge metric.
func (c *JobGaugeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.pendingDesc
	ch <- c.runningDesc
	ch <- c.blockedDesc
}

// Collect queries the store for all jobs, counts by status,
// and sends gauge values. Called on each Prometheus scrape.
func (c *JobGaugeCollector) Collect(ch chan<- prometheus.Metric) {
	var pending, running, blocked float64

	for _, job := range c.store.ListJobs(context.Background()) {
		switch job.Status {
		case store.StatusPending:
			pending++
		case store.StatusRunning:
			running++
		case store.StatusBlocked:
			blocked++
		}
	}

	ch <- prometheus.MustNewConstMetric(c.pendingDesc, prometheus.GaugeValue, pending)
	ch <- prometheus.MustNewConstMetric(c.runningDesc, prometheus.GaugeValue, running)
	ch <- prometheus.MustNewConstMetric(c.blockedDesc, prometheus.GaugeValue, blocked)
}
