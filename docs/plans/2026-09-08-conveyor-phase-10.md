# Phase 10 — The agent, and the promises phase 9 could not keep

**Goal:** a real `cmd/agent` executes jobs over the phase-9 stream, permanent failures stop retrying, and a job is never buried because nobody was listening.

*Done when:* `conveyord` plus one `cmd/agent` process runs a job end to end with no `grpcurl` in sight; a permanently-failing job is `dead` after one attempt; a job enqueued with no agent connected is still `pending` ten minutes later; an abandoned attempt is cancelled on the agent instead of running to completion unobserved.

---

## How to read this plan

Same contract as phases 7–9: **tests and configs are given verbatim; implementation is requirements and hints.** Every item below states the evidence that motivates it, so nothing here is speculative work.

---

## Where phase 9 left off

Phase 9 shipped `internal/rpc` (registry, two-pump `Assign`, blocking `Server.Run`), wired it into `conveyord`, and retired `dummyRunner`. Two bugs and one design hole surfaced when the manual demo (`2026-08-28-conveyor-phase-9.md:832-847`) was actually executed against a live agent, rather than reasoned about:

* **Fixed in phase 9.** `runOne` created `recCtx` *before* `safeRun`, so the 5-second recording budget was spent by the time there was an outcome to write. Any job longer than 5s lost its result and sat `leased` until the reaper — invisible while `dummyRunner`'s jobs took 250 ms, fatal the moment an agent held one. Live symptom: `ERROR fail failed err="context deadline exceeded"` after a 47-second job. Regression test: `TestRecordingBudgetSurvivesALongJob` (`internal/dispatch/dispatch_test.go`).
* **Fixed in phase 9.** `runOne` logged `ERROR fail failed err=null` on every *successful* `Fail`, and shadowed the job's own error with `Fail`'s. Now a three-way switch on a separate `ferr`.
* **Not fixed — this phase's task 2.** A delivery failure consumes a retry attempt. Live measurement: a job whose agent disconnected went `leased attempt 0` → `pending` → `dead attempt 5` in about one second, because each `ErrNoAgents` bounce is a `Fail`.

---

## Task 1 — `Result.permanent`, and a `Permanent` that survives the wire

**Goal:** an agent can say "do not retry this".

Phase 9 constraint 3 says a `Result` with `error == ""` is success and anything else is failure, with no way to express permanence — so `Policy.OnFailure` retries a malformed payload until `MaxAttempts` (`2026-08-28-conveyor-phase-9.md:463`). Field 4 on `Result` was deliberately left free for this.

**Steps:**

1. `proto/conveyor/v1/conveyor.proto`: add `bool permanent = 4;` to `Result`. Additive, so `buf breaking` stays green; `make proto` regenerates.
2. `internal/rpc.deliver`: when `r.Permanent`, wrap the error so `job.IsPermanent` reports true — `fmt.Errorf("%w: %s", job.Permanent, r.Error)` or whatever `internal/job` already exposes for this. Check `job.IsPermanent`'s exact contract before choosing; it is the only consumer that matters.
3. Wire fixture: extend `fixtures()` in `internal/rpc/wire_test.go` with a permanent-failure `Result`, then `OVERWRITE_GOLDEN=1`. Goldens 1-7 must stay byte-identical — a changed existing hex means a field number moved.

**Test to add:** a `deliver` case proving `Run` returns an error for which `job.IsPermanent` is true, and one proving a non-permanent result is unchanged. The dead-lettering itself is already covered by `TestFailBuriesPermanentErrors` (`internal/store/fail_test.go:118`).

---

## Task 2 — Undelivered work must not cost an attempt

**Goal:** an agentless queue waits; it does not dead-letter.

**Evidence:** with `Policy{Base: time.Second, Max: time.Minute, MaxAttempts: 5}` and a 500 ms poll, a job whose agent left reached `dead attempt 5` within a second of the disconnect. Every `ErrNoAgents`/`ErrSlowAgent` reaches `store.Fail`, which increments `attempt` (`store.go:378`) and applies the policy. So a five-second agent restart is enough to bury the queue, and phase 9's own acceptance line — "the job is `pending` with `attempt` incremented" (`2026-08-28-conveyor-phase-9.md:859`) — holds for well under a second before the job dies.

**The distinction to encode:** work that was *never delivered* costs nothing; work an agent *took* costs an attempt. `ErrAgentGone` and `Result.error` are the second kind — the job may have run — and must keep consuming attempts.

**Steps:**

1. `internal/job`: `var ErrUndelivered = errors.New("job was not delivered to any executor")`. It lives here because neither `internal/dispatch` nor `internal/rpc` may import the other (phase 9 review checklist, `:882`), and both already depend on `internal/job`.
2. `internal/rpc`: wrap the two non-delivery sentinels with it. `Run`'s existing `assign %s: %w` becomes a wrap that carries both, so `errors.Is(err, ErrNoAgents)` *and* `errors.Is(err, job.ErrUndelivered)` hold. `ErrAgentGone` is deliberately **not** wrapped.
3. `internal/store`: `Release(ctx, id, leasedBy string) error` — clears `leased_by`/`lease_expires_at`, sets `state = 'pending'`, leaves `attempt` and `next_attempt_at` alone, and keeps the `WHERE id = $1 AND leased_by = $2 AND state = 'leased'` fencing every other write uses. Record an event so the timeline shows it.
4. `internal/dispatch.runOne`: on `errors.Is(err, job.ErrUndelivered)`, call `Release` instead of `Fail`, log at debug, and return. The `Store` interface in `dispatch.go` grows one method.
5. Decide the poll behaviour: releasing a job the loop will re-lease 500 ms later is a busy loop against Postgres when no agent is connected. Cheapest honest answer is for `runTurn` to skip leasing entirely while the registry is empty — but dispatch cannot see the registry, so the alternative is `Release` writing `next_attempt_at = now() + Interval`. Choose one and say why in the code.

**Tests to add:**

* `internal/store`: `Release` clears the lease, leaves `attempt` untouched, and refuses another agent's lease (mirror `TestFailClearsTheLease` and `TestFailRejectsAnotherAgent`).
* `internal/dispatch`: a runner returning a `job.ErrUndelivered`-wrapped error produces a `Release` and **no** `Fail` — the fake store needs a `released []string` slot.
* `internal/rpc`: `Run` with an empty registry returns an error satisfying both `errors.Is(err, ErrNoAgents)` and `errors.Is(err, job.ErrUndelivered)`; `ErrAgentGone` satisfies only the former.

---

## Task 3 — `cmd/agent`

**Goal:** the binary that makes the demo a product.

**Shape:** one `Assign` stream, mirroring the server's two pumps. `-server`, `-name`, `-types` (comma-separated capabilities), `-concurrency`, `-heartbeat` flags; `signal.NotifyContext` and an `errgroup` root, same discipline as `conveyord`.

**Requirements:**

* **`Register` first**, before anything else, or the server never assigns it work (phase 9 §0.3).
* **Reconnect with backoff.** The server's `keepalive.ServerParameters{Time: 30s, Timeout: 10s}` bounds a black-holed connection; the agent must survive it and come back. Reuse `job.Policy.Backoff` rather than inventing a second backoff.
* **Heartbeat on a ticker**, at an interval that satisfies the server's `EnforcementPolicy.MinTime` (see task 5).
* **Executors are a map from job type to `func(ctx, payload) error`**, registered in `main`. Phase 10 ships one real one plus `sleep` for demos; anything more is speculation.
* **`Progress` is optional but cheap** — one frame at start is what makes the SSE stream useful.
* **A `Result` for every `Assignment`, exactly once.** The server drops results it has no waiter for, and logs them; that log line is the debugging seam.
* **Honour `CancelJob`** (task 4): cancel the job's context, and still send a `Result` for it — abandoning silently leaves the server's waiter to time out on nothing.
* **Bound execution.** The agent — not the server — is the only party that knows how long a `resize` should take (phase 9 decision, `:853`). A per-type timeout that fails the job with a permanent error (task 1) is what stops a wedged executor from holding a lease forever.

**Tests:** the executor map and the frame handling are the parts worth testing without a network — one test that an unknown job type comes back as a permanent failure rather than silence, one that a cancelled job still reports. The end-to-end path is covered by task 6's daemon test.

---

## Task 4 — Deliver `CancelJob`

**Goal:** an abandoned attempt stops running.

`ServerToAgent.cancel_job` has been on the wire since phase 8 (`proto/conveyor/v1/conveyor.proto:15`, golden `server_to_agent_cancel_job.hex`) and nothing has ever sent it. The consequence is written into phase 9's own design: `runOne` abandons attempt N on `ErrLeaseLost` while that agent keeps working, which is exactly why waiters are keyed by `(jobID, attempt)`. Attempt tokens make the *bookkeeping* correct; `CancelJob` makes the *work* stop.

**Steps:**

1. `internal/rpc`: `Server.Run`'s `ctx.Done()` branch sends a `CancelJob{JobId: j.ID}` to the agent it assigned to, on the same one-slot `send` channel, before returning `ctx.Err()`. A full buffer means the agent is already behind; log and move on — best effort, as the proto comment says.
2. Keep the waiter registered until `Run` returns, which it already is. A late `Result` for a cancelled attempt then lands on its own key and is dropped with the existing warning.

**Test:** the registry-level assertion is enough and needs no live agent — `Run` with a cancelled context puts a `CancelJob` for that job on the agent's channel. Add it beside `TestRunReturnsOnCancel`.

---

## Task 5 — Loose ends from phase 9

Small, independent, each with a named reason:

1. **`keepalive.EnforcementPolicy{MinTime: …}`** on `grpc.NewServer` in `cmd/conveyord`. Without it the server's default 5-minute minimum GOAWAYs any agent that pings more often — and task 3 gives agents a ping interval for the first time. Set it below the agent's heartbeat interval.
2. **The gRPC hard-stop deadline is hardcoded 5s** (`main.go`, `time.AfterFunc(5*time.Second, grpcSrv.Stop)`) while HTTP uses `cfg.drainTimeout`. Either derive it from `-drain` or document why streams get their own shorter ladder.
3. **`reg.List()` has no operator surface.** The demo could not observe registration without reading the daemon's logs. A `GET /agents` returning id plus capabilities is a handful of lines and makes "is my agent connected?" answerable; it also makes the phase-9 shutdown ordering (`CloseAll` before `GracefulStop`) observable end to end.
4. **No authentication on the gRPC listener.** Any process that can reach `:50051` can register, claim capabilities, and take jobs. This is a trust boundary, so it needs a decision recorded even if the answer is "bind to localhost and defer mTLS to deployment" — silence is the wrong answer.
5. **`staticcheck` does not run on this toolchain.** `go run honnef.co/go/tools/cmd/staticcheck@2025.1.1` fails with `export data version 4 is greater than maximum supported version 2` against Go 1.26, and the mise shim is unset. The phase review checklist (`:870`) asks for it; pin a version that understands 1.26 or drop the line.

---

## Task 6 — Prove it, then measure the thing that did not add up

**Steps:**

1. **End-to-end daemon test.** `cmd/conveyord`'s `TestSIGTERMDrainsAnInFlightJob` already runs a registered-but-silent agent over the real listener and asserts `pending attempt 1` after the signal. Phase 10's version runs `cmd/agent`'s executor code in-process instead, and asserts the job reaches `succeeded` — the assertion phase 9 had to give up when `dummyRunner` was deleted.
2. **No-agent patience test.** Enqueue with an empty registry, wait past `MaxAttempts × Interval`, assert the job is still `pending` with `attempt == 0`. This is task 2's acceptance criterion and the one that would have caught the burial.
3. **The open measurement.** The attempt burn was *faster than the policy allows*: attempts 1→4 inside one second, where `Backoff` draws from `[0,1s)`, `[0,2s)`, `[0,4s)`, `[0,8s)` — a sub-second total has probability under a tenth of a percent, and it happened twice. Backoff is written correctly (`store.go:379`) and covered (`TestFailSchedulesARetry`, `TestFailHidesTheJobUntilItsNextAttempt`), and `pgx` encodes `time.Duration` to `interval` exactly (`pgtype_default.go:200`), so the discrepancy is unexplained rather than diagnosed. Settle it before task 2 makes it invisible:

   ```sql
   SELECT state, attempt, next_attempt_at, updated_at FROM jobs WHERE id = $1;
   SELECT state, message, created_at FROM job_events WHERE job_id = $1 ORDER BY created_at;
   ```

   The event timeline gives the spacing of every transition. If the gaps match the policy, the earlier reading was mine to explain; if they do not, something re-leases ahead of `next_attempt_at` and that is a queue-correctness bug that outranks everything else in this phase.

---

## Global constraints

Carried from phases 7–9, plus:

1. **`internal/dispatch` imports no `internal/rpc`, and `internal/rpc` imports no `internal/dispatch`.** Shared vocabulary goes in `internal/job`. Task 2's sentinel is the test of this rule.
2. **`cmd/agent` owns execution policy; the server owns delivery.** No job timeouts, no retry decisions, and no capability inference in `internal/rpc`.
3. **Every deferral in this document names its evidence.** Phase 9's demo found two bugs that no test could have found, because both needed a job that outlived a fake. Prefer running the thing.

---

## Phase review checklist

```bash
gofmt -l . | grep -v '^gen/'
go vet ./...
go test -race -count=2 ./...
go tool buf lint
go tool buf breaking proto --against '.git#branch=main,subdir=proto'
rm -rf gen && make proto && git diff --stat   # goldens 1-7 byte-identical
# + manual: conveyord + cmd/agent, one job to succeeded, then kill -9 the agent
#   mid-job and watch the job return to pending without losing an attempt
```

By eye:

* No `Fail` on a path where the job was never delivered.
* `Result.permanent` set by the agent for a malformed payload, and the job `dead` after one attempt.
* A `CancelJob` on the wire when the dispatcher abandons an attempt.
* `cmd/agent` reconnects after `kill -9` of `conveyord` without operator help.
