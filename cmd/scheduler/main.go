// Command scheduler runs the carbon-aware job scheduler: an HTTP API
// for submitting jobs, a background loop that releases them against
// live grid conditions, and a Prometheus metrics endpoint.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charanp11/carbon-scheduler/internal/api"
	"github.com/charanp11/carbon-scheduler/internal/carbon"
	"github.com/charanp11/carbon-scheduler/internal/metrics"
	"github.com/charanp11/carbon-scheduler/internal/scheduler"
	"github.com/charanp11/carbon-scheduler/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	addr := envOr("SCHEDULER_ADDR", ":8080")
	dataDir := envOr("SCHEDULER_DATA_DIR", "./data")
	threshold := carbon.Index(envOr("SCHEDULER_THRESHOLD", string(carbon.Low)))
	tickInterval := envDuration("SCHEDULER_TICK_INTERVAL", 5*time.Minute)
	maxPending := envInt("SCHEDULER_MAX_PENDING", 10_000)

	apiKeys, err := parseAPIKeys(os.Getenv("SCHEDULER_API_KEYS"))
	if err != nil {
		return err
	}
	if len(apiKeys) == 0 {
		return errors.New("SCHEDULER_API_KEYS must set at least one \"key:owner\" pair")
	}

	st, err := store.Open(dataDir)
	if err != nil {
		return err
	}

	sched := scheduler.New(carbon.NewClient(), threshold)
	sched.SetMaxPending(maxPending)

	// Restore anything that was still waiting on a cleaner grid window
	// when the process last stopped.
	pending, err := st.LoadQueue()
	if err != nil {
		return err
	}
	for _, spec := range pending {
		if err := sched.Submit(spec); err != nil {
			slog.Warn("dropping persisted job that no longer validates", "job_id", spec.ID, "error", err)
		}
	}
	slog.Info("restored pending jobs from disk", "count", len(pending))

	reg := metrics.New()
	server := api.NewServer(sched, reg, apiKeys)

	// Persist every decision to the audit log and keep the on-disk
	// queue snapshot current, so a crash never silently loses a job
	// that was waiting for a clean window.
	sched.Subscribe(func(d scheduler.Decision) {
		if err := st.AppendDecision(store.Decision{JobID: d.JobID, Action: d.Action, Reason: d.Reason, At: d.At}); err != nil {
			slog.Error("failed to append audit log entry", "error", err)
		}
		if err := st.SaveQueue(sched.Snapshot()); err != nil {
			slog.Error("failed to persist queue snapshot", "error", err)
		}
	})

	mux := http.NewServeMux()
	mux.Handle("/", server.Routes())
	mux.Handle("/metrics", reg.Handler())

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go sched.Run(ctx, tickInterval)

	go func() {
		slog.Info("listening", "addr", addr, "threshold", string(threshold), "tick_interval", tickInterval)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server error", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid integer, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return d
}

// parseAPIKeys reads a "key1:owner1,key2:owner2" list from the
// SCHEDULER_API_KEYS environment variable, so keys never live in a
// config file that might get committed by accident.
func parseAPIKeys(raw string) (map[string]string, error) {
	keys := make(map[string]string)
	if raw == "" {
		return keys, nil
	}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, owner, ok := strings.Cut(pair, ":")
		if !ok || key == "" || owner == "" {
			return nil, errors.New("SCHEDULER_API_KEYS entries must be \"key:owner\", comma-separated")
		}
		keys[key] = owner
	}
	return keys, nil
}
