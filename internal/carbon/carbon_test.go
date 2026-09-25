package carbon

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCleanEnough(t *testing.T) {
	tests := []struct {
		name      string
		current   Index
		threshold Index
		want      bool
	}{
		{"very low beats low threshold", VeryLow, Low, true},
		{"low meets low threshold", Low, Low, true},
		{"moderate fails low threshold", Moderate, Low, false},
		{"very high fails any but very high threshold", VeryHigh, High, false},
		{"unrecognized band fails closed", Index("unknown"), VeryHigh, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.current.CleanEnough(tt.threshold); got != tt.want {
				t.Errorf("CleanEnough() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientCurrent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":[{"intensity":{"forecast":120,"actual":118,"index":"moderate"}}]}`))
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, http: srv.Client()}
	index, err := c.Current(context.Background())
	if err != nil {
		t.Fatalf("Current() error = %v", err)
	}
	if index != Moderate {
		t.Errorf("Current() = %q, want %q", index, Moderate)
	}
}

func TestClientCurrentFailsClosedOnBadResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := &Client{baseURL: srv.URL, http: srv.Client()}
	if _, err := c.Current(context.Background()); err == nil {
		t.Error("Current() expected an error on a 500 response, got nil")
	}
}
