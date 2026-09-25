package scheduler

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/charanp11/carbon-scheduler/internal/carbon"
	"github.com/charanp11/carbon-scheduler/internal/job"
)

// TestDecide table-drives the scheduler's entire decision surface: this
// pure function is what the rest of the project exists to serve, so it
// gets the most exhaustive coverage.
func TestDecide(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	future := now.Add(2 * time.Hour)
	past := now.Add(-time.Minute)

	tests := []struct {
		name        string
		spec        job.Spec
		current     carbon.Index
		threshold   carbon.Index
		wantRelease bool
		wantForced  bool
	}{
		{
			name:        "urgent releases even on a dirty grid",
			spec:        job.Spec{Priority: job.Urgent, Deadline: future},
			current:     carbon.VeryHigh,
			threshold:   carbon.Low,
			wantRelease: true,
		},
		{
			name:        "flexible holds when grid is dirty and deadline is far off",
			spec:        job.Spec{Priority: job.Flexible, Deadline: future},
			current:     carbon.High,
			threshold:   carbon.Low,
			wantRelease: false,
		},
		{
			name:        "flexible releases when grid exactly meets threshold",
			spec:        job.Spec{Priority: job.Flexible, Deadline: future},
			current:     carbon.Low,
			threshold:   carbon.Low,
			wantRelease: true,
		},
		{
			name:        "flexible releases when grid is cleaner than threshold",
			spec:        job.Spec{Priority: job.Flexible, Deadline: future},
			current:     carbon.VeryLow,
			threshold:   carbon.Low,
			wantRelease: true,
		},
		{
			name:        "flexible is forced once its deadline has passed, dirty grid or not",
			spec:        job.Spec{Priority: job.Flexible, Deadline: past},
			current:     carbon.VeryHigh,
			threshold:   carbon.Low,
			wantRelease: true,
			wantForced:  true,
		},
		{
			name:        "flexible with no deadline holds indefinitely on a dirty grid",
			spec:        job.Spec{Priority: job.Flexible},
			current:     carbon.VeryHigh,
			threshold:   carbon.Low,
			wantRelease: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			release, forced, reason := decide(now, tt.spec, &tt.current, tt.threshold)
			if release != tt.wantRelease {
				t.Errorf("decide() release = %v, want %v (reason: %q)", release, tt.wantRelease, reason)
			}
			if forced != tt.wantForced {
				t.Errorf("decide() forced = %v, want %v (reason: %q)", forced, tt.wantForced, reason)
			}
			if reason == "" {
				t.Error("decide() returned an empty reason; every decision must be explainable")
			}
		})
	}
}

// TestDecideWhenCarbonDataIsUnavailable covers the nil-current path: a
// failed carbon API call must never block urgent or deadline-forced
// jobs, only ordinary flexible ones that actually need a reading.
func TestDecideWhenCarbonDataIsUnavailable(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("urgent still releases", func(t *testing.T) {
		release, _, reason := decide(now, job.Spec{Priority: job.Urgent, Deadline: now.Add(time.Hour)}, nil, carbon.Low)
		if !release {
			t.Errorf("urgent job should release even without carbon data (reason: %q)", reason)
		}
	})

	t.Run("deadline-forced job still releases", func(t *testing.T) {
		release, forced, reason := decide(now, job.Spec{Priority: job.Flexible, Deadline: now.Add(-time.Minute)}, nil, carbon.Low)
		if !release || !forced {
			t.Errorf("release = %v, forced = %v, want both true (reason: %q)", release, forced, reason)
		}
	})

	t.Run("ordinary flexible job holds rather than guesses", func(t *testing.T) {
		release, _, reason := decide(now, job.Spec{Priority: job.Flexible, Deadline: now.Add(time.Hour)}, nil, carbon.Low)
		if release {
			t.Errorf("flexible job should hold when carbon data is unavailable (reason: %q)", reason)
		}
	})
}

// fakeSource lets tests control grid conditions, or simulate the carbon
// API being unreachable, without a network call.
type fakeSource struct {
	index carbon.Index
	err   error
}

func (f fakeSource) Current(ctx context.Context) (carbon.Index, error) { return f.index, f.err }

func TestSchedulerTickRunsUrgentAndHoldsFlexible(t *testing.T) {
	s := New(fakeSource{index: carbon.VeryHigh}, carbon.Low)

	var ranIDs []string
	s.run = func(ctx context.Context, spec job.Spec) job.Result {
		ranIDs = append(ranIDs, spec.ID)
		return job.Result{}
	}

	future := time.Now().Add(time.Hour)
	urgent := job.Spec{ID: "u1", Priority: job.Urgent, Method: http.MethodPost, URL: "https://example.com", Deadline: future}
	flexible := job.Spec{ID: "f1", Priority: job.Flexible, Method: http.MethodPost, URL: "https://example.com", Deadline: future}

	if err := s.Submit(urgent); err != nil {
		t.Fatalf("Submit(urgent) error = %v", err)
	}
	if err := s.Submit(flexible); err != nil {
		t.Fatalf("Submit(flexible) error = %v", err)
	}

	if _, err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	if len(ranIDs) != 1 || ranIDs[0] != "u1" {
		t.Errorf("expected only the urgent job to run, ran = %v", ranIDs)
	}
	if got := s.Pending(); got != 1 {
		t.Errorf("expected 1 flexible job still pending, got %d", got)
	}
}

func TestSchedulerTickReleasesFlexibleOnCleanGrid(t *testing.T) {
	s := New(fakeSource{index: carbon.VeryLow}, carbon.Low)

	ran := false
	s.run = func(ctx context.Context, spec job.Spec) job.Result {
		ran = true
		return job.Result{}
	}

	flexible := job.Spec{ID: "f1", Priority: job.Flexible, Method: http.MethodPost, URL: "https://example.com", Deadline: time.Now().Add(time.Hour)}
	if err := s.Submit(flexible); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	if _, err := s.Tick(context.Background()); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}

	if !ran {
		t.Error("expected flexible job to release on a clean grid")
	}
	if got := s.Pending(); got != 0 {
		t.Errorf("expected queue to be empty, got %d pending", got)
	}
}

func TestTickStillRunsUrgentJobsWhenCarbonAPIFails(t *testing.T) {
	s := New(fakeSource{err: errors.New("network unreachable")}, carbon.Low)

	var ranIDs []string
	s.run = func(ctx context.Context, spec job.Spec) job.Result {
		ranIDs = append(ranIDs, spec.ID)
		return job.Result{}
	}

	future := time.Now().Add(time.Hour)
	urgent := job.Spec{ID: "u1", Priority: job.Urgent, Method: http.MethodPost, URL: "https://example.com", Deadline: future}
	flexible := job.Spec{ID: "f1", Priority: job.Flexible, Method: http.MethodPost, URL: "https://example.com", Deadline: future}
	if err := s.Submit(urgent); err != nil {
		t.Fatalf("Submit(urgent) error = %v", err)
	}
	if err := s.Submit(flexible); err != nil {
		t.Fatalf("Submit(flexible) error = %v", err)
	}

	decisions, err := s.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick() error = %v, want nil: a failed carbon fetch should never fail the whole tick", err)
	}
	if len(ranIDs) != 1 || ranIDs[0] != "u1" {
		t.Errorf("expected only the urgent job to run despite the carbon API being down, ran = %v", ranIDs)
	}
	if got := s.Pending(); got != 1 {
		t.Errorf("expected the flexible job to be held, not lost, got %d pending", got)
	}
	if len(decisions) != 2 {
		t.Errorf("expected both jobs to be evaluated and reported, got %d decisions", len(decisions))
	}
}

func TestSchedulerTickMarksFailedExecutionDistinctly(t *testing.T) {
	s := New(fakeSource{index: carbon.VeryLow}, carbon.Low)

	// The job releases (clean grid) but its HTTP call fails. That
	// should surface as "failed", not silently as "released": a caller
	// checking status needs to be able to tell the difference between
	// "ran successfully" and "ran, but the endpoint rejected it."
	s.run = func(ctx context.Context, spec job.Spec) job.Result {
		return job.Result{Err: errors.New("connection refused")}
	}

	flexible := job.Spec{ID: "f1", Priority: job.Flexible, Method: http.MethodPost, URL: "https://example.com", Deadline: time.Now().Add(time.Hour)}
	if err := s.Submit(flexible); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	decisions, err := s.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if len(decisions) != 1 || decisions[0].Action != "failed" {
		t.Errorf("decisions = %+v, want a single \"failed\" decision", decisions)
	}
}

func TestSchedulerTickMarksHTTPErrorStatusAsFailed(t *testing.T) {
	s := New(fakeSource{index: carbon.VeryLow}, carbon.Low)
	s.run = func(ctx context.Context, spec job.Spec) job.Result {
		return job.Result{StatusCode: 500}
	}

	flexible := job.Spec{ID: "f1", Priority: job.Flexible, Method: http.MethodPost, URL: "https://example.com", Deadline: time.Now().Add(time.Hour)}
	if err := s.Submit(flexible); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	decisions, err := s.Tick(context.Background())
	if err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if len(decisions) != 1 || decisions[0].Action != "failed" {
		t.Errorf("decisions = %+v, want a single \"failed\" decision for a 500 response", decisions)
	}
}

func TestSubmitUrgentSignalsAnImmediateTick(t *testing.T) {
	s := New(fakeSource{index: carbon.Low}, carbon.Low)
	urgent := job.Spec{ID: "u1", Priority: job.Urgent, Method: http.MethodPost, URL: "https://example.com", Deadline: time.Now().Add(time.Hour)}

	if err := s.Submit(urgent); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case <-s.tickNow:
		// signaled, as expected: an urgent job shouldn't have to wait
		// for the next scheduled tick to be evaluated.
	default:
		t.Error("expected submitting an urgent job to signal an immediate tick")
	}
}

func TestSubmitFlexibleDoesNotSignalAnImmediateTick(t *testing.T) {
	s := New(fakeSource{index: carbon.Low}, carbon.Low)
	flexible := job.Spec{ID: "f1", Priority: job.Flexible, Method: http.MethodPost, URL: "https://example.com", Deadline: time.Now().Add(time.Hour)}

	if err := s.Submit(flexible); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case <-s.tickNow:
		t.Error("a flexible job should not force an immediate tick, it can wait for the next scheduled one")
	default:
		// expected: nothing signaled
	}
}

func TestRunEvaluatesUrgentJobWithoutWaitingForTickInterval(t *testing.T) {
	s := New(fakeSource{index: carbon.VeryHigh}, carbon.Low)

	ran := make(chan string, 1)
	s.run = func(ctx context.Context, spec job.Spec) job.Result {
		ran <- spec.ID
		return job.Result{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx, time.Hour) // deliberately long: proves the wake path, not the ticker

	urgent := job.Spec{ID: "u1", Priority: job.Urgent, Method: http.MethodPost, URL: "https://example.com", Deadline: time.Now().Add(time.Hour)}
	if err := s.Submit(urgent); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	select {
	case id := <-ran:
		if id != "u1" {
			t.Errorf("ran job %q, want %q", id, "u1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("urgent job did not run promptly; Run() is waiting on the tick interval instead of the wake signal")
	}
}

func TestSubmitRejectsInvalidJob(t *testing.T) {
	s := New(fakeSource{index: carbon.Low}, carbon.Low)
	err := s.Submit(job.Spec{ID: "bad", Method: http.MethodPost, URL: "http://insecure.example.com"})
	if err != job.ErrInsecureURL {
		t.Errorf("Submit() error = %v, want %v", err, job.ErrInsecureURL)
	}
	if got := s.Pending(); got != 0 {
		t.Errorf("invalid job should never be queued, got %d pending", got)
	}
}
