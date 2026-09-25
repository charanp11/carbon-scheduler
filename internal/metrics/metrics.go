// Package metrics tracks scheduler counters and serves them in
// Prometheus's plain text exposition format. The format is simple
// enough to hand-roll, which keeps this project's dependency count at
// zero rather than pulling in the full client library for four numbers.
package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Registry holds atomic counters safe for concurrent use from the
// scheduling loop and the HTTP handler at the same time.
type Registry struct {
	released atomic.Uint64
	held     atomic.Uint64
	forced   atomic.Uint64
	failed   atomic.Uint64
	other    atomic.Uint64
	pending  atomic.Int64
}

// New returns an empty Registry.
func New() *Registry {
	return &Registry{}
}

// RecordDecision increments the counter for a scheduling action
// ("released", "held", "forced", or "failed"). An unrecognized action is
// counted separately rather than silently dropped, so the total is
// always exact for debugging.
func (r *Registry) RecordDecision(action string) {
	switch action {
	case "released":
		r.released.Add(1)
	case "held":
		r.held.Add(1)
	case "forced":
		r.forced.Add(1)
	case "failed":
		r.failed.Add(1)
	default:
		r.other.Add(1)
	}
}

// SetPending records how many jobs are currently queued.
func (r *Registry) SetPending(n int) {
	r.pending.Store(int64(n))
}

// Handler serves current counter values in Prometheus exposition
// format, ready for a standard Prometheus scrape config.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintln(w, "# HELP carbon_scheduler_jobs_total Jobs evaluated, by scheduling decision.")
		fmt.Fprintln(w, "# TYPE carbon_scheduler_jobs_total counter")
		fmt.Fprintf(w, "carbon_scheduler_jobs_total{action=\"released\"} %d\n", r.released.Load())
		fmt.Fprintf(w, "carbon_scheduler_jobs_total{action=\"held\"} %d\n", r.held.Load())
		fmt.Fprintf(w, "carbon_scheduler_jobs_total{action=\"forced\"} %d\n", r.forced.Load())
		fmt.Fprintf(w, "carbon_scheduler_jobs_total{action=\"failed\"} %d\n", r.failed.Load())
		fmt.Fprintf(w, "carbon_scheduler_jobs_total{action=\"other\"} %d\n", r.other.Load())
		fmt.Fprintln(w, "# HELP carbon_scheduler_jobs_pending Jobs currently queued.")
		fmt.Fprintln(w, "# TYPE carbon_scheduler_jobs_pending gauge")
		fmt.Fprintf(w, "carbon_scheduler_jobs_pending %d\n", r.pending.Load())
	})
}
