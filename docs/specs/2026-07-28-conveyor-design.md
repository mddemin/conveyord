# Conveyor — Design Spec

**Date:** 2026-07-28
**Status:** Approved
**Purpose:** A learning project to master production Go. The deliverable is competence; the codebase is the vehicle.

## 1. Goal & Constraints

Build **Conveyor**, a distributed job queue with a control plane and remote agents, covering the parts of Go that distinguish a competent Go engineer from someone who writes another language in Go syntax.

Constraints, decided up front:

- **Stdlib-first.** Dependencies only where the standard library genuinely falls short: `pgx` (Postgres driver), `grpc-go` + `protobuf` (wire protocol), `testcontainers-go` (integration tests), `otel` (tracing), `goleak` (test assertion). No web framework, no ORM, no config library, no logging library — `net/http`, `database/sql` semantics, `flag`/env, and `log/slog` cover those.
- **The learner writes the code.** Claude specifies tasks and reviews every step. Claude does not implement unless asked.
- **Every phase ends with something that runs.** No pure-scaffolding phases.
- Go 1.26, Postgres 16, Linux.

Starting point: empty repository, `mise.toml` pinning Go.

## 2. System Shape

Two processes:

| Process | Role |
|---|---|
| `conveyord` | Control plane. Owns Postgres, HTTP API (enqueue/query jobs), SSE stream for live job events, gRPC server that agents connect to. |
| `agent` | Data plane. Dials `conveyord` over a bidirectional gRPC stream, receives leased jobs, executes them, streams progress and results back, heartbeats. |

**Why two processes rather than one binary with a worker pool.** The agent model is how Temporal, Buildkite, GitHub Actions runners, and Nomad work. It forces the problems that produce good Go engineers: leasing, heartbeats, at-least-once delivery, idempotency, cancellation propagation across a network boundary, and graceful drain. An in-process worker pool allows cheating on all of them.

Job payloads stay deliberately boring — fetch a URL, compute a checksum, sleep-and-fail-on-purpose. The learning is in the machinery.

## 3. Package Layout

Each package has one purpose and is comprehensible without reading its neighbors.

```
cmd/conveyord/        wiring + flags only
cmd/agent/
internal/job/         domain: Job, State, transitions, retry policy. ZERO non-stdlib deps.
internal/store/       Postgres persistence (pgx). Leasing via SKIP LOCKED.
internal/dispatch/    scheduler: lease -> assign -> expire -> retry -> dead-letter
internal/httpapi/     REST + SSE handlers
internal/rpc/         gRPC server (control plane) + client (agent)
internal/executor/    agent-side job execution + concurrency limiting
proto/conveyor/v1/    .proto definitions
```

**Invariant enforced in review:** dependencies point inward. `internal/job` imports only the standard library. Persistence, transport, and observability concerns never leak into the domain. A violation is a review finding, not a style preference.

## 4. Data Flow

One job's life:

```
POST /jobs
  -> store.Enqueue                         state=pending
  -> dispatch loop: SELECT ... FOR UPDATE SKIP LOCKED
                                           state=leased, lease_expires_at set
  -> pushed down the agent's open gRPC stream
  -> agent executes, streams Progress messages back
  -> agent sends Result                    state=succeeded | failed
  -> on failure: retry policy computes next_attempt_at with jitter
                                           state=pending
  -> attempts exhausted                    state=dead
  -> every state change fans out to SSE subscribers
```

**Crash safety comes from the lease, not from trust.** If an agent dies mid-job, `lease_expires_at` passes and a reaper returns the job to `pending`. Jobs therefore run **at-least-once**, and executors must be idempotent. This is reviewed.

## 5. Concurrency Model

The core rule, everywhere: **every goroutine has a known owner, a known stop signal, and a defined destination for its error.** No fire-and-forget `go doThing()` anywhere in the codebase. This is the strictest review criterion in the project.

### Shutdown is a tree, not a broadcast

`cmd/conveyord` owns a root context cancelled by `signal.NotifyContext` on SIGINT/SIGTERM. Everything below receives that context. Components stop in dependency order, coordinated by `errgroup`:

```
signal -> cancel root ctx
  -> HTTP server: Shutdown(timeoutCtx)   stop accepting, drain in-flight
  -> gRPC server: GracefulStop()         tell agents to drain, wait
  -> dispatch loop: exits its select
  -> SSE hub: closes subscriber channels
  -> pgx pool: Close()                   last; everything above may still need it
```

Ordering this wrong (closing the pool while handlers still hold it) is the classic Go shutdown bug and panics on exit. Expect to hit it once.

### Primitives and where each is used

| Primitive | Where | What it teaches |
|---|---|---|
| `errgroup.Group` with ctx | root lifecycle, agent stream pumps | structured concurrency, first-error-wins cancellation |
| channels + `select` | SSE hub, agent job intake | the Go mental model |
| `sync.Mutex` | SSE subscriber registry | when a lock genuinely beats a channel |
| `context` cancellation | HTTP -> dispatch -> gRPC -> executor | the most important API in Go |

### The SSE hub

Deliberately the trickiest component. A hub holding `map[subID]chan Event` has three classic failure modes, all of which will occur:

1. Send on a closed channel when a client disconnects mid-broadcast.
2. Goroutine leak when a subscriber never unregisters.
3. **System-wide stall** because one slow client's buffer filled and the broadcaster blocked.

The fix for (3) — buffered channel plus a `select` with a `default` that drops the event and marks the subscriber lagging — is a reusable career-long pattern. `go test -race` is mandatory on this package.

### Errors

Sentinel errors (`job.ErrNotFound`, `job.ErrInvalidTransition`) live in the domain package, are wrapped with `%w` as they travel up, are inspected with `errors.Is` / `errors.As`, and are translated to HTTP status codes or gRPC codes **only at the edges**. `internal/job` has no concept of a 404.

A retryable-versus-terminal distinction lives on the error type and is read by the retry policy: a 400 from a job's target URL is not retried; a 503 is.

### Deliberate simplification

The dispatch loop polls Postgres on a ticker rather than using `LISTEN/NOTIFY`. Polling is roughly 40 lines and correct; `LISTEN/NOTIFY` is a latency optimization. The code carries:

```go
// ponytail: polling dispatch, switch to LISTEN/NOTIFY if p99 enqueue->start latency matters
```

Optional phase 13 revisits it with measurements.

## 6. Testing Strategy

Three tiers; the ratio matters more than the count.

1. **Domain tests** (`internal/job`) — pure, fast, no I/O. Table-driven with `t.Run` subtests. The state machine and retry policy are where table tests genuinely earn their keep.
2. **Integration tests** (`internal/store`, `internal/dispatch`) — real Postgres via testcontainers-go, one container per package via `TestMain`, per-test isolation by rolled-back transaction or truncate helper. **The database is not mocked.** Mocked SQL tests pass while the query is wrong, and `SKIP LOCKED` semantics cannot be mocked meaningfully.
3. **End-to-end** — `conveyord` plus an in-process `agent`; enqueue over real HTTP, assert the job reaches `succeeded` and that the SSE stream observed every transition.

Non-negotiables:

- `go test -race ./...` in CI from phase 1.
- `t.Cleanup` over `defer` in helpers.
- `context.WithTimeout` on every test touching the network.
- **No `time.Sleep` in tests.** Poll with a deadline, or synchronize on a channel.
- `goleak` verifies no goroutines survive shutdown.

**Interfaces only where a test needs a seam** — the executor's job runner, the clock in the retry policy. Not one interface per struct. Go convention is that the *consumer* declares a small interface at the point of use; a `StoreInterface` mirroring every method of `Store` is a review finding.

## 7. Review Loop

The learner implements; Claude reviews. Per task:

1. Learner implements, runs `go test -race ./...` and `go vet ./...`, signals ready.
2. Claude reads the actual diff and reports findings ranked by severity: correctness, then idiom, then style — with the reasoning, not just the verdict.
3. Claude does not rewrite the learner's code unless asked. Diagnosing "this goroutine can leak, here is the scenario" and fixing it oneself is the point.
4. Each phase ends with a checkpoint: it runs, tests pass, the behavior is demonstrable.

Reviews are direct. Problems are stated plainly; good work is not padded.

## 8. Phases

Twelve phases, plus one optional.

### Foundations

**Phase 1 — Module and domain skeleton.** `go mod init`; `internal/job` with `Job`, `State`, and `Transition(from, to) error`. Table-driven tests for every legal and illegal transition. No database, no HTTP.
*Teaches:* package design, sentinel errors, table tests.
*Done when:* the state machine is fully tested and `internal/job` imports only the standard library.

**Phase 2 — Retry policy.** Exponential backoff with full jitter, max attempts, retryable-versus-terminal error classification, injected `Clock` interface.
*Teaches:* consumer-defined interfaces, deterministic time in tests.
*Done when:* the backoff sequence is asserted without sleeping.

### Persistence

**Phase 3 — Postgres store and migrations.** pgx pool; schema (`jobs`, `job_events`); migrations as embedded `.sql` via `embed.FS` with a roughly 30-line runner; Enqueue/Get/List.
*Teaches:* `embed`, pgx versus `database/sql`, context on every query.
*Done when:* testcontainers integration tests pass.

**Phase 4 — Leasing.** `SELECT ... FOR UPDATE SKIP LOCKED`, `lease_expires_at`, lease renewal, expiry reaper.
*Teaches:* the actual mechanism behind every Postgres-backed queue.
*Done when:* a test with 10 concurrent leasers proves no job is handed out twice.

### Control plane

**Phase 5 — HTTP API.** `net/http` with Go 1.22 routing patterns (`POST /jobs`, `GET /jobs/{id}`, `GET /jobs`); middleware chain (request ID, `slog` logging, panic recovery, timeout); JSON helpers; error-to-status translation at the edge.
*Teaches:* stdlib HTTP is sufficient; middleware without a framework.
*Done when:* a job can be enqueued and read back over HTTP.

**Phase 6 — SSE hub and live stream.** `GET /jobs/{id}/events` and `GET /events`. The slow-subscriber problem, buffered channels, drop-and-mark-lagging, clean unregister on disconnect.
*Teaches:* the channel patterns that matter.
*Done when:* `-race` is clean and a subscriber that never reads cannot stall the hub.

**Phase 7 — Dispatch loop and lifecycle.** Ticker-driven poll; `errgroup` root; ordered graceful shutdown; `signal.NotifyContext`.
*Done when:* SIGTERM mid-job drains cleanly with zero leaked goroutines, verified by `goleak`.

### Agents over gRPC

**Phase 8 — Proto and codegen.** `proto/conveyor/v1/`; buf for generation; bidirectional `Assign` stream; `Heartbeat`, `Progress`, `Result` messages.
*Teaches:* protobuf schema design, backward compatibility, codegen in a Go build.

**Phase 9 — gRPC server side.** Agent registration; a send-pump/recv-pump goroutine pair per agent coordinated by `errgroup`; in-memory agent registry; capability-based routing.
*Teaches:* bidirectional streaming, the two-pump pattern, domain errors mapped to gRPC codes.

**Phase 10 — Agent binary.** Dial with reconnect backoff; bounded local concurrency; job execution; progress streaming; honoring server-side cancellation; drain on SIGTERM.
*Teaches:* cancellation across a network boundary — the hardest and most valuable piece in the project.

### Production shape

**Phase 11 — Observability.** OpenTelemetry traces propagated HTTP -> dispatch -> gRPC -> executor; metrics for queue depth, lease expiries, job duration; `slog` carrying trace IDs; `net/http/pprof` behind a flag.
*Done when:* one enqueued job appears as a single connected trace spanning both binaries.

**Phase 12 — Ops and hardening.** Multi-stage Dockerfiles; Compose stack (conveyord, two agents, Postgres, Jaeger); GitHub Actions running `go test -race`, `go vet`, `staticcheck`; a load test that finds the first real bottleneck, then a fix justified by pprof evidence.

### Optional

**Phase 13 — Earn the optimization.** Replace polling dispatch with `LISTEN/NOTIFY` and measure the difference. A deliberate exercise in earning an optimization rather than assuming it.

**Pacing:** phases 1–4 move fastest. Phases 6 and 10 each take longer than expected and are where the deepest learning lands.
