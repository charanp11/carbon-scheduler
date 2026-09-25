package job

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSpecValidate(t *testing.T) {
	future := time.Now().Add(time.Hour)

	tests := []struct {
		name    string
		spec    Spec
		wantErr error
	}{
		{
			name:    "valid https job",
			spec:    Spec{ID: "job-1", Method: http.MethodPost, URL: "https://example.com/hook", Deadline: future},
			wantErr: nil,
		},
		{
			name:    "missing id",
			spec:    Spec{Method: http.MethodPost, URL: "https://example.com/hook"},
			wantErr: ErrEmptyID,
		},
		{
			name:    "missing url",
			spec:    Spec{ID: "job-1", Method: http.MethodPost},
			wantErr: ErrEmptyURL,
		},
		{
			name:    "rejects plain http",
			spec:    Spec{ID: "job-1", Method: http.MethodPost, URL: "http://example.com/hook"},
			wantErr: ErrInsecureURL,
		},
		{
			name:    "missing method",
			spec:    Spec{ID: "job-1", URL: "https://example.com/hook"},
			wantErr: ErrEmptyMethod,
		},
		{
			name:    "rejects oversized body",
			spec:    Spec{ID: "job-1", Method: http.MethodPost, URL: "https://example.com/hook", Body: make([]byte, maxBodyBytes+1)},
			wantErr: ErrBodyTooLarge,
		},
		{
			name:    "rejects deadline in the past",
			spec:    Spec{ID: "job-1", Method: http.MethodPost, URL: "https://example.com/hook", Deadline: time.Now().Add(-time.Hour)},
			wantErr: ErrPastDeadline,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRun(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Test")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	// httptest gives us a plain http:// URL; Run doesn't re-validate
	// scheme, that's Spec.Validate's job at submission time.
	spec := Spec{
		ID:      "job-1",
		Method:  http.MethodPost,
		URL:     srv.URL,
		Headers: map[string]string{"X-Test": "carbon-scheduler"},
	}

	result := Run(t.Context(), spec)
	if result.Err != nil {
		t.Fatalf("Run() error = %v", result.Err)
	}
	if result.StatusCode != http.StatusAccepted {
		t.Errorf("Run() status = %d, want %d", result.StatusCode, http.StatusAccepted)
	}
	if gotHeader != "carbon-scheduler" {
		t.Errorf("server saw header %q, want %q", gotHeader, "carbon-scheduler")
	}
}
