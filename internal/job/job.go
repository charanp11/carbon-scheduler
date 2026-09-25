// Package job defines the unit of work the scheduler moves around: a
// single outbound HTTPS call, plus the metadata needed to decide when
// it's allowed to run.
package job

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Priority controls whether a job must run immediately or may be
// delayed until the electricity grid is running cleaner.
type Priority int

const (
	// Flexible jobs can be delayed to a cleaner window, up to Deadline.
	Flexible Priority = iota
	// Urgent jobs always run immediately regardless of grid conditions.
	Urgent
)

func (p Priority) String() string {
	if p == Urgent {
		return "urgent"
	}
	return "flexible"
}

// Spec describes a unit of work the scheduler can run.
//
// By design a job can only trigger an HTTPS call: a method, a URL, headers
// and a body. Arbitrary shell or code execution is intentionally not
// supported. A scheduler that can be told what to run over a network API
// is exactly the kind of component that should not also be able to
// execute arbitrary commands, so that entire class of injection risk is
// designed out rather than sanitized against.
type Spec struct {
	ID       string
	Priority Priority
	Deadline time.Time // latest acceptable run time, even if flexible

	Method  string
	URL     string
	Headers map[string]string
	Body    []byte

	Timeout    time.Duration
	MaxRetries int
}

// Sentinel validation errors, checkable with errors.Is.
var (
	ErrEmptyURL     = errors.New("job: url is required")
	ErrInsecureURL  = errors.New("job: url must use https")
	ErrEmptyMethod  = errors.New("job: method is required")
	ErrEmptyID      = errors.New("job: id is required")
	ErrPastDeadline = errors.New("job: deadline is in the past")
	ErrBodyTooLarge = errors.New("job: body exceeds maximum size")
)

// maxBodyBytes caps how large a submitted job body may be, so the
// queue itself can't be used to exhaust memory.
const maxBodyBytes = 1 << 20 // 1 MiB

// Validate checks that the job describes a safe, well-formed HTTPS call.
// It is called once at submission time so bad input never reaches the
// scheduling loop.
func (s Spec) Validate() error {
	if s.ID == "" {
		return ErrEmptyID
	}
	if s.URL == "" {
		return ErrEmptyURL
	}
	u, err := url.Parse(s.URL)
	if err != nil {
		return err
	}
	if u.Scheme != "https" {
		return ErrInsecureURL
	}
	if s.Method == "" {
		return ErrEmptyMethod
	}
	if len(s.Body) > maxBodyBytes {
		return ErrBodyTooLarge
	}
	if !s.Deadline.IsZero() && s.Deadline.Before(time.Now()) {
		return ErrPastDeadline
	}
	return nil
}

// Result records the outcome of a single execution attempt.
type Result struct {
	StatusCode int
	Err        error
	RanAt      time.Time
}

// defaultTimeout bounds any job call that didn't set its own, so a slow
// or unresponsive endpoint can never hang the scheduling loop.
const defaultTimeout = 30 * time.Second

// Run executes the job's HTTP call under the configured timeout. It is
// the only way a Spec ever turns into real work; there is no code path
// from a Spec to a shell.
func Run(ctx context.Context, s Spec) Result {
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var bodyReader io.Reader
	if len(s.Body) > 0 {
		bodyReader = bytes.NewReader(s.Body)
	}

	req, err := http.NewRequestWithContext(ctx, s.Method, s.URL, bodyReader)
	if err != nil {
		return Result{Err: err, RanAt: time.Now()}
	}
	for k, v := range s.Headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Result{Err: err, RanAt: time.Now()}
	}
	defer resp.Body.Close()

	return Result{StatusCode: resp.StatusCode, RanAt: time.Now()}
}
