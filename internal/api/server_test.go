package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/charanp11/carbon-scheduler/internal/carbon"
	"github.com/charanp11/carbon-scheduler/internal/scheduler"
)

type fakeSource struct{ index carbon.Index }

func (f fakeSource) Current(ctx context.Context) (carbon.Index, error) { return f.index, nil }

type noopMetrics struct{}

func (noopMetrics) RecordDecision(string) {}
func (noopMetrics) SetPending(int)        {}

func newTestServer() *Server {
	sched := scheduler.New(fakeSource{index: carbon.VeryHigh}, carbon.Low)
	return NewServer(sched, noopMetrics{}, map[string]string{
		"alice-key": "alice",
		"bob-key":   "bob",
	})
}

func submit(t *testing.T, h http.Handler, apiKey string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(b))
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestSubmitRequiresAuthentication(t *testing.T) {
	h := newTestServer().Routes()
	rec := submit(t, h, "", map[string]any{"id": "job-1", "method": "GET", "url": "https://example.com"})
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestSubmitRejectsInvalidKey(t *testing.T) {
	h := newTestServer().Routes()
	rec := submit(t, h, "not-a-real-key", map[string]any{"id": "job-1", "method": "GET", "url": "https://example.com"})
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestSubmitRejectsInsecureURL(t *testing.T) {
	h := newTestServer().Routes()
	rec := submit(t, h, "alice-key", map[string]any{"id": "job-1", "method": "GET", "url": "http://example.com"})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d (body: %s)", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func TestSubmitRejectsUnknownFields(t *testing.T) {
	h := newTestServer().Routes()
	rec := submit(t, h, "alice-key", map[string]any{
		"id": "job-1", "method": "GET", "url": "https://example.com", "shell_command": "rm -rf /",
	})
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d; strict decoding should reject unknown fields", rec.Code, http.StatusBadRequest)
	}
}

func TestOwnersCannotSeeEachOthersJobs(t *testing.T) {
	h := newTestServer().Routes()

	rec := submit(t, h, "alice-key", map[string]any{"id": "shared-name", "method": "GET", "url": "https://example.com"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("alice's submit failed: status %d, body %s", rec.Code, rec.Body.String())
	}

	// Bob asks for a job with the same human-facing id alice used.
	req := httptest.NewRequest(http.MethodGet, "/jobs/shared-name", nil)
	req.Header.Set("Authorization", "Bearer bob-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("bob should not see alice's job; status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestOwnerCanReadOwnJobStatus(t *testing.T) {
	h := newTestServer().Routes()

	rec := submit(t, h, "alice-key", map[string]any{"id": "job-1", "method": "GET", "url": "https://example.com"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("submit failed: status %d, body %s", rec.Code, rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/jobs/job-1", nil)
	req.Header.Set("Authorization", "Bearer alice-key")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var status JobStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if status.JobID != "job-1" {
		t.Errorf("JobID = %q, want %q", status.JobID, "job-1")
	}
}

func TestHealthzRequiresNoAuth(t *testing.T) {
	h := newTestServer().Routes()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}
