// Package store persists scheduler state to disk as plain files, so a
// restart doesn't lose flexible jobs that are still waiting for a
// cleaner grid window.
//
// This intentionally avoids a database dependency. For this workload —
// a handful of pending jobs and an append-only decision log — a couple
// of files written atomically covers the same durability guarantee a
// database would, with fewer moving parts to run and nothing to patch.
package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/charanp11/carbon-scheduler/internal/job"
)

// Store reads and writes scheduler state under a single directory.
type Store struct {
	mu        sync.Mutex
	queuePath string
	logPath   string
}

// Open ensures dir exists and returns a Store backed by files inside it.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{
		queuePath: filepath.Join(dir, "queue.json"),
		logPath:   filepath.Join(dir, "decisions.jsonl"),
	}, nil
}

// SaveQueue overwrites the persisted queue snapshot. It writes to a
// temp file first and renames it into place, so a crash mid-write can
// never leave a half-written, corrupt snapshot on disk: the rename is
// atomic, so readers only ever see the old file or the new one.
func (s *Store) SaveQueue(jobs []job.Spec) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(jobs, "", "  ")
	if err != nil {
		return err
	}

	tmp := s.queuePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.queuePath)
}

// LoadQueue reads the persisted queue snapshot. A missing file is not
// an error, it just means nothing was pending at last shutdown.
func (s *Store) LoadQueue() ([]job.Spec, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.queuePath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var jobs []job.Spec
	if err := json.Unmarshal(data, &jobs); err != nil {
		return nil, err
	}
	return jobs, nil
}

// Decision is the audit-log record of one scheduling choice: which job,
// what happened to it, and why.
type Decision struct {
	JobID  string    `json:"job_id"`
	Action string    `json:"action"`
	Reason string    `json:"reason"`
	At     time.Time `json:"at"`
}

// AppendDecision adds one record to the audit log. The log is
// append-only, nothing already written is ever rewritten, so it stays a
// reliable record even if the process is killed mid-write.
func (s *Store) AppendDecision(d Decision) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	line, err := json.Marshal(d)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	_, err = f.Write(line)
	return err
}
