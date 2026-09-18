# Conveyor

A distributed job queue with a control plane (`conveyord`) and HTTP API, built as a
deliberate exercise in production Go: leasing, at-least-once delivery, graceful
shutdown, and the concurrency patterns that separate Go from Go-syntax.

The codebase is the vehicle; [the spec](docs/specs/2026-07-28-conveyor-design.md)
is the curriculum. Stdlib-first by rule: `pgx`, `grpc`/`protobuf`, `testcontainers-go`,
and `goleak` are the only dependencies.

## Status

| Phase | Scope | State |
|---|---|---|
| 1–2 | Domain model, state machine, retry policy | done |
| 3–4 | Postgres store, migrations, `SKIP LOCKED` leasing, reaper | done |
| 5 | HTTP API: enqueue/query, JSON errors, middleware | done |
| 6 | SSE hub: slow-subscriber drops, lagged signal, heartbeats | done |
| 7 | Dispatch loop, lease renewal, lifecycle & graceful shutdown | done |
| 8–10 | gRPC agents over bidirectional streams | done |
| 11–12 | OpenTelemetry, Docker/Compose, load test | planned |

## Architecture

```
POST /jobs ──▶ store.Enqueue ──▶ pending
                     ▲                │
                     │        dispatch loop (SKIP LOCKED lease)
                     │                ▼
              retry / reaper ◀── leased ──▶ rpc.Server (gRPC Assign stream)
                     │                │
                     │       Complete / Fail / Release (+ backoff, jitter, dead-letter)
                     ▼                ▼
               job_events ◀──── every state change
                     │
                     ▼
                 SSE hub ──▶ GET /events, GET /jobs/{id}/events   (lossy, live)
```

Crash safety comes from leases, not trust: a dead worker's lease expires, the
reaper returns the job to `pending`, and it runs again — **at-least-once**.
Executors must be idempotent. The event stream is advisory on purpose: Postgres
is the record; a slow SSE client gets dropped events and a `lagged` frame, never
a stalled broadcaster.

## Quickstart

```bash
# one throwaway Postgres
docker run -d --name conveyor-pg -p 5432:5432 \
  -e POSTGRES_DB=conveyor -e POSTGRES_USER=conveyor -e POSTGRES_PASSWORD=conveyor \
  postgres:17-alpine

# the daemon (migrates on boot). HTTP and gRPC default to loopback: neither
# authenticates.
go run ./cmd/conveyord \
  -dsn "postgres://conveyor:conveyor@127.0.0.1:5432/conveyor?sslmode=disable"

# an agent (separate terminal). Without one, jobs lease and return to pending.
go run ./cmd/agent -server 127.0.0.1:50051
```

Then watch work move through the system live:

```bash
curl -N 127.0.0.1:8080/events            # hold the stream open

curl -sS -X POST 127.0.0.1:8080/jobs \
  -d '{"type":"sleep","payload":{"duration":"2s"}}'
```

You'll see `pending` → `leased by <host>-<pid>` → `succeeded` scroll past.
SIGTERM mid-job drains cleanly: in-flight work finishes and records its result;
a second Ctrl-C kills immediately if you're impatient.

### API

```
POST /jobs                {"type":"...","payload":{...},"id":"optional"}   → 201
GET  /jobs?state=pending&limit=50                                          → list
GET  /jobs/{id}                                                            → one job
GET  /jobs/{id}/events    Server-Sent Events for one job
GET  /events              Server-Sent Events for all jobs
GET  /agents              connected agents (404 if the registry is unwired)
```

Failures are always `{"error":"..."}`; request IDs are echoed in
`X-Request-ID` and attached to every log line.

## Layout

```
cmd/conveyord/     wiring only: flags, pool, errgroup shutdown tree
cmd/agent/         flags, dial, executor catalogue (sleep, checksum)
internal/job/      domain: Job, states, transitions, retry policy. stdlib only.
internal/store/    pgx persistence, migrations (embedded SQL), SKIP LOCKED leasing
internal/dispatch/ ticker loop, bounded workers, lease renewal, reaper loop
internal/rpc/      gRPC registry, assignment, result waiters keyed by attempt
internal/agent/    stream pumps and executors
internal/httpapi/  REST + SSE handlers, error→status mapping at the edge
internal/hub/      fan-out broadcaster; never blocks, drops for slow subscribers
internal/pgtest/   per-process-schema throwaway Postgres for integration tests
proto/             conveyor.v1 schema; generate with `make proto`
docs/              design spec + per-phase plans
```

Dependencies point inward: `internal/job` imports nothing outside the standard
library, and storage/transport concerns never leak into it.

## Testing

```bash
go test -race ./...
```

Three tiers, no mocked databases: pure domain tables, integration against real
Postgres via testcontainers, and an end-to-end SIGTERM-drain test that reads its
verdict from Postgres after the server is gone. Goleak verifies no goroutine
outlives what started it.

Against a long-lived dev database instead (each package gets an isolated
schema, so concurrent packages don't truncate each other):

```bash
export CONVEYOR_TEST_DSN="postgres://conveyor:conveyor@127.0.0.1:5432/conveyor?sslmode=disable"
go test -race ./...
```

## Tooling

```bash
make test    # go test -race ./...
make vet     # go vet + staticcheck + gofmt check
```

## License

[MIT](LICENSE)
