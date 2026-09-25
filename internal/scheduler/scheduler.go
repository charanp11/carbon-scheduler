// Package scheduler holds pending jobs and decides, each tick, which
// ones should run now versus wait for a cleaner grid window.
package scheduler

import (
	"container/heap"
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/charanp11/carbon-scheduler/internal/carbon"
	"github.com/charanp11/carbon-scheduler/internal/job"
	"github.com/charanp11/carbon-scheduler/internal/queue"
)

// Decision records what the scheduler chose to do with a job on a given
// tick and why. Every decision is logged, forming the audit trail for
// "why did this job run when it did."
type Decision struct {
	JobID  string
	Action string // "released", "held", or "forced"
	Reason string
	At     time.Time
}

// Scheduler holds pending jobs and evaluates them against live grid
// conditions. It's safe for concurrent use: Submit is expected to be
// called from API request handlers while Tick runs on a background
// timer.
type Scheduler struct {
	mu     sync.Mutex
	pq     *queue.PriorityQueue
	source carbon.Source

	// threshold is the cleanest-acceptable band before a flexible job
	// is released early. A job always runs once its deadline is
	// reached, clean grid or not — flexibility never means "maybe
	// never."
	threshold carbon.Index

	clock func() time.Time
	run   func(context.Context, job.Spec) job.Result

	hooksMu sync.Mutex
	hooks   []func(Decision)
}

// New returns a Scheduler that releases flexible jobs once the grid is
// at or below threshold, or once their deadline forces the issue.
func New(source carbon.Source, threshold carbon.Index) *Scheduler {
	return &Scheduler{
		pq:        queue.New(),
		source:    source,
		threshold: threshold,
		clock:     time.Now,
		run:       job.Run,
	}
}

// Submit validates and enqueues a job. Invalid jobs are rejected here,
// before they ever reach the scheduling loop.
func (s *Scheduler) Submit(spec job.Spec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	heap.Push(s.pq, &queue.Item{Spec: spec})
	return nil
}

// Pending returns how many jobs are currently queued.
func (s *Scheduler) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pq.Len()
}

// Snapshot returns a copy of every job currently pending, for
// persisting to disk across restarts. It does not remove them from the
// queue, and the order is unspecified.
func (s *Scheduler) Snapshot() []job.Spec {
	s.mu.Lock()
	defer s.mu.Unlock()
	specs := make([]job.Spec, 0, s.pq.Len())
	for _, item := range *s.pq {
		specs = append(specs, item.Spec)
	}
	return specs
}

// Subscribe registers fn to be called for every decision made on every
// future tick, in evaluation order. This is how status trackers, audit
// logs, and metrics stay in sync without the scheduler needing to know
// any of them exist.
func (s *Scheduler) Subscribe(fn func(Decision)) {
	s.hooksMu.Lock()
	defer s.hooksMu.Unlock()
	s.hooks = append(s.hooks, fn)
}

func (s *Scheduler) notify(d Decision) {
	s.hooksMu.Lock()
	hooks := s.hooks
	s.hooksMu.Unlock()
	for _, h := range hooks {
		h(d)
	}
}

// decide is the pure decision function driving every scheduling choice.
// It takes no locks and touches no network, which is what makes it
// straightforward to test exhaustively on its own — this is the entire
// thesis of the project, so it earns to be the most heavily tested part
// of it.
func decide(now time.Time, spec job.Spec, current carbon.Index, threshold carbon.Index) (release bool, forced bool, reason string) {
	if spec.Priority == job.Urgent {
		return true, false, "urgent job, released immediately"
	}
	if !spec.Deadline.IsZero() && !now.Before(spec.Deadline) {
		return true, true, "deadline reached, forced release regardless of grid"
	}
	if current.CleanEnough(threshold) {
		return true, false, "grid intensity at or below threshold"
	}
	return false, false, "grid intensity above threshold, holding"
}

// Tick evaluates every pending job once against current grid conditions,
// running the ones that should go and re-queuing the ones that should
// wait. It returns the decisions made, in the order they were evaluated.
func (s *Scheduler) Tick(ctx context.Context) ([]Decision, error) {
	index, err := s.source.Current(ctx)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	pending := make([]*queue.Item, 0, s.pq.Len())
	for s.pq.Len() > 0 {
		pending = append(pending, heap.Pop(s.pq).(*queue.Item))
	}
	s.mu.Unlock()

	decisions := make([]Decision, 0, len(pending))
	var held []*queue.Item

	for _, item := range pending {
		now := s.clock()
		release, forced, reason := decide(now, item.Spec, index, s.threshold)

		d := Decision{JobID: item.Spec.ID, Reason: reason, At: now}
		switch {
		case release && forced:
			d.Action = "forced"
		case release:
			d.Action = "released"
		default:
			d.Action = "held"
		}

		if release {
			s.run(ctx, item.Spec)
		} else {
			held = append(held, item)
		}

		decisions = append(decisions, d)
		slog.Info("scheduling decision",
			"job_id", d.JobID, "action", d.Action, "reason", d.Reason, "grid_index", string(index))
		s.notify(d)
	}

	s.mu.Lock()
	for _, item := range held {
		heap.Push(s.pq, item)
	}
	s.mu.Unlock()

	return decisions, nil
}

// Run ticks on the given interval until ctx is canceled. Callers
// typically run this in its own goroutine.
func (s *Scheduler) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.Tick(ctx); err != nil {
				slog.Error("scheduler tick failed", "error", err)
			}
		}
	}
}
