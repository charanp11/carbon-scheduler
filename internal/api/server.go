// Package api exposes the scheduler over HTTP. Every route requires a
// valid API key, and every key is scoped so one caller can never see or
// affect another caller's jobs.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/charanp11/carbon-scheduler/internal/job"
	"github.com/charanp11/carbon-scheduler/internal/scheduler"
)

type contextKey string

const ownerContextKey contextKey = "owner"

// maxBodyBytes caps how large a request body may be, independent of
// job.Spec's own limit, so an oversized request never even reaches JSON
// decoding.
const maxBodyBytes = 1 << 20 // 1 MiB

// metricsRecorder is the subset of metrics.Registry the API layer
// needs. Declaring it here, instead of importing the metrics package
// directly, keeps this package's dependencies to what it actually uses
// and makes it trivial to pass a no-op in tests.
type metricsRecorder interface {
	RecordDecision(action string)
	SetPending(n int)
}

// JobStatus is what GET /jobs/{id} returns. At is a pointer so a job
// still waiting for its first scheduling decision omits the field
// entirely instead of showing a zero-value timestamp: plain time.Time
// doesn't cooperate with encoding/json's omitempty.
type JobStatus struct {
	JobID  string     `json:"job_id"`
	Status string     `json:"status"`
	Reason string     `json:"reason,omitempty"`
	At     *time.Time `json:"at,omitempty"`
}

// Server wires a scheduler to HTTP handlers.
type Server struct {
	sched   *scheduler.Scheduler
	metrics metricsRecorder
	limiter *rateLimiter

	// keyOwners maps sha256(api key) to an owner id. Raw keys are
	// hashed once at construction and never retained, so a memory
	// dump of a running server doesn't leak usable credentials.
	keyOwners map[[32]byte]string

	mu       sync.RWMutex
	statuses map[string]JobStatus // scoped job id -> latest known status
	jobOwner map[string]string    // scoped job id -> owner, for access checks
}

// NewServer builds a Server. apiKeys maps a raw key to an owner id.
func NewServer(sched *scheduler.Scheduler, m metricsRecorder, apiKeys map[string]string) *Server {
	owners := make(map[[32]byte]string, len(apiKeys))
	for key, owner := range apiKeys {
		owners[sha256.Sum256([]byte(key))] = owner
	}

	s := &Server{
		sched:     sched,
		metrics:   m,
		limiter:   newRateLimiter(5, time.Second),
		keyOwners: owners,
		statuses:  make(map[string]JobStatus),
		jobOwner:  make(map[string]string),
	}
	sched.Subscribe(s.recordDecision)
	return s
}

func (s *Server) recordDecision(d scheduler.Decision) {
	at := d.At
	s.mu.Lock()
	s.statuses[d.JobID] = JobStatus{JobID: d.JobID, Status: d.Action, Reason: d.Reason, At: &at}
	s.mu.Unlock()

	if s.metrics != nil {
		s.metrics.RecordDecision(d.Action)
		s.metrics.SetPending(s.sched.Pending())
	}
}

// Routes returns the server's handler.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.authenticate(s.rateLimit(s.handleSubmit)))
	mux.HandleFunc("GET /jobs/{id}", s.authenticate(s.rateLimit(s.handleStatus)))
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return mux
}

// authenticate requires a bearer API key and resolves it to an owner
// before any handler runs.
func (s *Server) authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || key == "" {
			http.Error(w, "missing or malformed Authorization header", http.StatusUnauthorized)
			return
		}

		owner, ok := s.lookupOwner(sha256.Sum256([]byte(key)))
		if !ok {
			http.Error(w, "invalid API key", http.StatusUnauthorized)
			return
		}

		next(w, r.WithContext(context.WithValue(r.Context(), ownerContextKey, owner)))
	}
}

// lookupOwner compares against every known key hash in constant time,
// rather than a direct map lookup on the hash bytes, so response timing
// can't be used to learn anything about which keys are valid.
func (s *Server) lookupOwner(sum [32]byte) (string, bool) {
	for known, owner := range s.keyOwners {
		if subtle.ConstantTimeCompare(known[:], sum[:]) == 1 {
			return owner, true
		}
	}
	return "", false
}

func (s *Server) rateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		owner, _ := r.Context().Value(ownerContextKey).(string)
		if !s.limiter.Allow(owner) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

type submitRequest struct {
	ID         string            `json:"id"`
	Priority   string            `json:"priority"` // "urgent" or "flexible"; default flexible
	Deadline   time.Time         `json:"deadline"`
	Method     string            `json:"method"`
	URL        string            `json:"url"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
	TimeoutSec int               `json:"timeout_seconds"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	owner, _ := r.Context().Value(ownerContextKey).(string)

	var req submitRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	priority := job.Flexible
	switch req.Priority {
	case "", "flexible":
		priority = job.Flexible
	case "urgent":
		priority = job.Urgent
	default:
		http.Error(w, `priority must be "urgent" or "flexible"`, http.StatusBadRequest)
		return
	}

	// Namespacing the job id by owner means two different callers can
	// never collide on, or see, each other's jobs, even if they submit
	// the same id.
	scopedID := owner + ":" + req.ID

	spec := job.Spec{
		ID:       scopedID,
		Priority: priority,
		Deadline: req.Deadline,
		Method:   req.Method,
		URL:      req.URL,
		Headers:  req.Headers,
		Body:     []byte(req.Body),
		Timeout:  time.Duration(req.TimeoutSec) * time.Second,
	}

	if err := s.sched.Submit(spec); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.jobOwner[scopedID] = owner
	s.mu.Unlock()
	if s.metrics != nil {
		s.metrics.SetPending(s.sched.Pending())
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(JobStatus{JobID: req.ID, Status: "queued"})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	owner, _ := r.Context().Value(ownerContextKey).(string)
	scopedID := owner + ":" + r.PathValue("id")

	s.mu.RLock()
	gotOwner, known := s.jobOwner[scopedID]
	status, hasStatus := s.statuses[scopedID]
	s.mu.RUnlock()

	// A job owned by someone else returns the same 404 as a job that
	// doesn't exist at all, never a 403: existence itself isn't
	// information another caller should get for free.
	if !known || gotOwner != owner {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if !hasStatus {
		json.NewEncoder(w).Encode(JobStatus{JobID: r.PathValue("id"), Status: "queued"})
		return
	}
	status.JobID = r.PathValue("id") // report the caller's own unscoped id back
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
