package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds all Prometheus metric objects for the Workron scheduler.
type Metrics struct {
	// Counters: track cumulative events.
	JobsSubmitted prometheus.Counter
	JobsClaimed   prometheus.Counter
	JobsCompleted prometheus.Counter
	JobsFailed    prometheus.Counter
	JobsRetried   prometheus.Counter
	JobsReaped    *prometheus.CounterVec // label: "outcome" = "requeued" | "failed"

	// Gang preemption counters.
	GangsPreempted              prometheus.Counter     // increments on each PreemptGang that entered drain
	GangPreemptionsForceDrained prometheus.Counter     // increments each time a stuck preempting task is force-drained by the reaper
	GangPreemptionsCompleted    *prometheus.CounterVec // label: "outcome" = "blocked" | "failed"

	// Histograms: track distributions of durations.
	JobExecDuration         prometheus.Histogram // seconds between claim and done
	JobQueueWait            prometheus.Histogram // seconds between submit and claim
	GangPreemptionDrainTime prometheus.Histogram // seconds between PreemptGang (drain_started_at) and CompletePreemption

	// Gauges: track current state.
	ReaperLeader prometheus.Gauge // 1 if this instance holds the reaper advisory lock, 0 otherwise
}

// NewMetrics creates all metric objects but does not register them.
// Call Register() to add them to a prometheus.Registerer.
func NewMetrics() *Metrics {
	return &Metrics{
		JobsSubmitted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workron_jobs_submitted_total",
			Help: "Total number of jobs submitted via the API.",
		}),
		JobsClaimed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workron_jobs_claimed_total",
			Help: "Total number of jobs claimed by workers.",
		}),
		JobsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workron_jobs_completed_total",
			Help: "Total number of jobs that completed successfully.",
		}),
		JobsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workron_jobs_failed_total",
			Help: "Total number of jobs that failed permanently after exhausting retries.",
		}),
		JobsRetried: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workron_jobs_retried_total",
			Help: "Total number of job retry attempts (re-queued after failure).",
		}),
		JobsReaped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workron_jobs_reaped_total",
			Help: "Total number of jobs reaped due to stale heartbeats.",
		}, []string{"outcome"}), // "requeued" or "failed"

		GangsPreempted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workron_gangs_preempted_total",
			Help: "Total number of gang drains initiated (PreemptGang returned Entered=true).",
		}),
		GangPreemptionsForceDrained: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workron_gang_preemptions_force_drained_total",
			Help: "Total number of preempting tasks force-drained by the reaper after the grace window.",
		}),
		GangPreemptionsCompleted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workron_gang_preemptions_completed_total",
			Help: "Total number of gang drains completed, labeled by final gang verdict.",
		}, []string{"outcome"}), // "blocked" or "failed"

		JobExecDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "workron_job_execution_duration_seconds",
			Help:    "Time spent executing a job (from claim to done).",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 300},
		}),
		JobQueueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "workron_job_queue_wait_seconds",
			Help:    "Time a job spent waiting in the queue before being claimed.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
		}),
		GangPreemptionDrainTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "workron_gang_preemption_drain_seconds",
			Help:    "Time between drain start (PreemptGang) and gang completion (CompletePreemption).",
			Buckets: []float64{0.5, 1, 2, 5, 10, 15, 30, 45, 60, 120},
		}),

		ReaperLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "workron_reaper_leader",
			Help: "Whether this scheduler instance currently holds the reaper advisory lock (1 = leader, 0 = follower).",
		}),
	}
}

// Register adds all metrics to the given registerer (typically a *prometheus.Registry).
func (m *Metrics) Register(reg prometheus.Registerer) {
	reg.MustRegister(
		m.JobsSubmitted,
		m.JobsClaimed,
		m.JobsCompleted,
		m.JobsFailed,
		m.JobsRetried,
		m.JobsReaped,
		m.GangsPreempted,
		m.GangPreemptionsForceDrained,
		m.GangPreemptionsCompleted,
		m.JobExecDuration,
		m.JobQueueWait,
		m.GangPreemptionDrainTime,
		m.ReaperLeader,
	)
}
