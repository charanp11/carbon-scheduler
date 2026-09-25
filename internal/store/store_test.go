package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/charanp11/carbon-scheduler/internal/job"
)

func TestSaveAndLoadQueueRoundTrips(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	want := []job.Spec{
		{ID: "owner-a:job-1", Priority: job.Flexible, Method: "POST", URL: "https://example.com/hook"},
		{ID: "owner-a:job-2", Priority: job.Urgent, Method: "GET", URL: "https://example.com/status"},
	}
	if err := s.SaveQueue(want); err != nil {
		t.Fatalf("SaveQueue() error = %v", err)
	}

	got, err := s.LoadQueue()
	if err != nil {
		t.Fatalf("LoadQueue() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("LoadQueue() returned %d jobs, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].URL != want[i].URL || got[i].Priority != want[i].Priority {
			t.Errorf("job %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestLoadQueueMissingFileIsNotAnError(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	jobs, err := s.LoadQueue()
	if err != nil {
		t.Fatalf("LoadQueue() error = %v, want nil", err)
	}
	if jobs != nil {
		t.Errorf("LoadQueue() = %v, want nil for a fresh store", jobs)
	}
}

func TestSaveQueueNeverLeavesATempFileBehind(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := s.SaveQueue([]job.Spec{{ID: "a:1", Method: "GET", URL: "https://example.com"}}); err != nil {
		t.Fatalf("SaveQueue() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "queue.json.tmp")); !os.IsNotExist(err) {
		t.Error("expected the temp file to be renamed away, but it still exists")
	}
}

func TestAppendDecisionIsAppendOnly(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	first := Decision{JobID: "a:1", Action: "held", Reason: "grid dirty", At: time.Now()}
	second := Decision{JobID: "a:1", Action: "released", Reason: "grid clean", At: time.Now()}
	if err := s.AppendDecision(first); err != nil {
		t.Fatalf("AppendDecision(first) error = %v", err)
	}
	if err := s.AppendDecision(second); err != nil {
		t.Fatalf("AppendDecision(second) error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, "decisions.jsonl"))
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	if lines != 2 {
		t.Errorf("expected 2 log lines, got %d", lines)
	}
}
