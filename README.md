# carbon-scheduler

A small Go service that sits between "a job is ready to run" and "a job
actually runs." Jobs marked **urgent** run immediately. Jobs marked
**flexible** are held until the local electricity grid is running
cleaner, then released, unless their deadline arrives first, in which
case they run regardless.

This is the same lever Google and Microsoft already pull at data-center
scale: delay work that doesn't care exactly when it runs until the grid
behind it is cleaner. This is a smaller, open, self-hostable version of
that idea, aimed at teams with existing batch jobs, backups, reports,
or training runs that have slack in their schedule.

## Why this exists

Electricity isn't uniformly clean throughout the day; how much of it
comes from coal and gas versus wind and solar shifts hour to hour. Most
batch work, nightly reports, video transcoding, model training runs,
backups, doesn't care whether it runs at 10am or 2pm, only that it
finishes by some deadline. This service is the piece that knows the
difference: given a job that's flexible, decide whether to run it now
or wait a bit, using live carbon intensity data instead of a guess.

It doesn't replace an existing scheduler (cron, Kubernetes CronJobs,
Airflow). It sits downstream of one: your existing trigger fires, and
instead of running the job directly, it submits the job to this
service, which owns the timing decision from there. See
[`examples/k8s-cronjob.yaml`](examples/k8s-cronjob.yaml) for what that
integration looks like in practice.

## How a job is defined, and why that's deliberately narrow

A job is exactly one HTTPS call: a method, a URL, headers, a body, and
a deadline. That's it. There is no way to submit a shell command or
arbitrary code to this service.

That's not an oversight, it's the load-bearing security decision in
this project. A scheduler that accepts "run this" over a network API is
exactly the kind of component that should not also be able to execute
arbitrary commands: allowing that would turn every future bug in this
service into a remote code execution vector. Restricting execution to
an HTTPS call closes off that entire class of injection risk before any
input validation even has to try. Most real integrations trigger
downstream work through an API call anyway (an Airflow DAG, a Lambda, a
CI pipeline), so nothing meaningful is lost.

## Design and security notes

These are the tradeoffs worth being able to explain, not just the
features:

- **No shell or code execution, by design.** Covered above. This is
  the one decision everything else builds on.
- **The carbon intensity API host is hardcoded**, not accepted as
  configuration. A job submitter controls the URL *their own job* calls
  (validated as HTTPS at submission time), but can never redirect the
  scheduler's own outbound call to an arbitrary host, closing off SSRF
  through that path.
- **API keys are hashed, never stored or logged in plaintext**, and
  compared in constant time (`crypto/subtle`) so response timing can't
  leak how close a guessed key was to a real one.
- **Jobs are namespaced by owner.** Two callers can never see, cancel,
  or overwrite each other's jobs, even if they submit the same job ID.
  A lookup for someone else's job returns 404, not 403, so existence
  itself isn't information a caller gets for free.
- **Per-owner rate limiting** (a small stdlib-only token bucket) means
  no single API key can flood the queue, and **a hard cap on total
  pending jobs** (`SCHEDULER_MAX_PENDING`) means the queue itself can't
  be grown into an out-of-memory condition even by a caller staying
  under the rate limit over time. A full queue returns 503, not 400:
  the caller did nothing wrong, the server is just at capacity.
- **Every outbound job call has a bounded timeout**, so a slow or
  unresponsive endpoint can never hang the scheduling loop.
- **Every scheduling decision is logged with its reason** ("released:
  grid intensity at or below threshold", "forced: deadline reached")
  and appended to an audit log, so "why did this job run when it did"
  always has an answer.
- **An unrecognized carbon-intensity reading fails closed.** If the
  API ever returns something this service doesn't recognize, jobs wait
  rather than run, on the assumption that an unknown state shouldn't
  be treated as a green light.
- **Zero third-party dependencies.** Persistence is two files (an
  atomically-written queue snapshot and an append-only decision log)
  instead of an embedded database, and metrics are exposed by hand in
  Prometheus's plain text format instead of pulling in the client
  library. For a project this size, that's less surface area to audit
  and nothing to patch when a dependency has a CVE.

A production version serving real traffic would want a few things this
doesn't have: secrets in a real secret store instead of an environment
variable, structured multi-tenant storage instead of flat files, and
probably a circuit breaker around the carbon API call itself. Those are
called out here rather than silently left out.

## Running it

```
go build ./cmd/scheduler
SCHEDULER_API_KEYS="devkey:me" ./scheduler
```

Environment variables:

| Variable                    | Default    | Meaning                                              |
|------------------------------|------------|-------------------------------------------------------|
| `SCHEDULER_ADDR`             | `:8080`    | HTTP listen address                                   |
| `SCHEDULER_DATA_DIR`         | `./data`   | where the queue snapshot and audit log are written    |
| `SCHEDULER_THRESHOLD`        | `low`      | cleanest-acceptable band before early release (`very low`, `low`, `moderate`, `high`, `very high`) |
| `SCHEDULER_TICK_INTERVAL`    | `5m`       | how often pending jobs are re-evaluated               |
| `SCHEDULER_MAX_PENDING`      | `10000`    | cap on jobs queued at once; `Submit` rejects with 503 once it's full |
| `SCHEDULER_API_KEYS`         | *(required)* | comma-separated `key:owner` pairs you choose yourself, e.g. `abc123:alice,def456:bob` — nothing to sign up for, this just authenticates callers of *your* service |

Submit a job:

```
curl -X POST localhost:8080/jobs \
  -H "Authorization: Bearer devkey" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "nightly-report",
    "priority": "flexible",
    "deadline": "2026-01-02T06:00:00Z",
    "method": "POST",
    "url": "https://example.com/run-report"
  }'
```

Check its status:

```
curl localhost:8080/jobs/nightly-report -H "Authorization: Bearer devkey"
```

Metrics, in Prometheus's text exposition format:

```
curl localhost:8080/metrics
```

## Architecture

```
      POST /jobs                       every tick interval
   (owner-scoped, rate       ┌──────────────────────────────┐
    limited, validated)      │                              │
        │                    ▼                              │
        │             ┌─────────────┐    Current()    ┌─────────────┐
        └───────────► │  Scheduler  │ ──────────────► │ Carbon API   │
                       │ (priority   │ ◄────────────── │ client       │
                       │  queue by   │   Index         └─────────────┘
                       │  deadline)  │
                       └──────┬──────┘
                              │ decide() — pure, exhaustively
                              │ tested, the whole thesis of
                              │ the project
                              ▼
                  ┌───────────────────────┐
                  │  released / held /     │──► HTTPS call (the job)
                  │  forced, with reason   │
                  └───────────┬────────────┘
                              │ Subscribe() hook
                  ┌───────────┴────────────┐
                  ▼                        ▼
          statuses + metrics       audit log + queue
          (in-memory, per API)     snapshot (on disk)
```

## Project layout

```
cmd/scheduler/       entrypoint: wires config, storage, scheduler, API, metrics
internal/job/        the unit of work: an HTTPS call, plus validation
internal/queue/      container/heap priority queue, urgent-first then earliest-deadline
internal/carbon/     grid carbon intensity client (and the Source interface for testing)
internal/scheduler/  the decision logic and the ticking loop
internal/store/      file-backed persistence: queue snapshot + append-only audit log
internal/api/        HTTP layer: auth, per-owner scoping, rate limiting
internal/metrics/    hand-rolled Prometheus exposition endpoint
examples/            a Kubernetes CronJob showing the intended integration shape
```

## Testing

```
go test -race -cover ./...
```

The scheduling decision logic (`internal/scheduler`'s `decide` function)
is table-driven and covers every branch: urgent jobs, deadline-forced
release, threshold-met release, and holding on a dirty grid. The API
layer has tests for the security properties directly: unauthenticated
requests are rejected, invalid keys are rejected, insecure URLs are
rejected, unrecognized JSON fields are rejected, and one owner's job is
never visible to another.

## What's not implemented

- Retries with backoff on a failed job call. A failed execution (a
  network error or a 4xx/5xx response) is currently detected and
  recorded as `action: "failed"` in the audit log and metrics, but the
  job isn't automatically retried, it needs to be resubmitted. Retry
  behavior needs a decision on how retries interact with jobs that are
  also waiting on a clean grid window before it's worth building.
- Historical carbon *forecasts* — the scheduler reads the current
  reading each tick rather than planning against a 24-hour forecast the
  way Google's system does. Forecast-aware scheduling would let a job
  wait *intelligently* for a known-upcoming clean window instead of
  polling, and is the natural next step.
- Multi-region support. The UK Carbon Intensity API is free and
  requires no key, which made it the right choice for a self-contained
  demo, but a real deployment would want a provider like WattTime or
  electricityMaps with regional data matching where the job actually
  runs.

## License

MIT, see [LICENSE](LICENSE).
