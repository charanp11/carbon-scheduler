package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerExposesRecordedCounters(t *testing.T) {
	r := New()
	r.RecordDecision("released")
	r.RecordDecision("released")
	r.RecordDecision("held")
	r.SetPending(3)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	body := rec.Body.String()
	wantLines := []string{
		`carbon_scheduler_jobs_total{action="released"} 2`,
		`carbon_scheduler_jobs_total{action="held"} 1`,
		`carbon_scheduler_jobs_total{action="forced"} 0`,
		`carbon_scheduler_jobs_pending 3`,
	}
	for _, want := range wantLines {
		if !strings.Contains(body, want) {
			t.Errorf("response missing line %q\nfull body:\n%s", want, body)
		}
	}
}
