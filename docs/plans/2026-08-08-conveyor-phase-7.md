# Phase 7 — Dispatch loop and lifecycle

**Goal:** jobs start running by themselves, and the process stops cleanly when told to.

*Done when:* SIGTERM in the middle of a job drains without losing it, `goleak` reports zero
leaked goroutines, and `-race -count=2` is clean.

---

## How to read this plan

Test code is given verbatim — the tests are the specification, and you should be able to
paste them and watch them fail. Implementation is given as requirements, signatures and
hints; the bodies are yours. Where a decision has more than one defensible answer, the plan
argues the options and recommends one rather than asserting.

Part 0 is a primer with programs that were run before this was written. The output shown is
real. Read it before Task 1 — every trap in it is one this phase walks into.

---

## Global constraints

1. **No fire-and-forget.** Every `go` statement has a written owner, a written stop signal, and a written destination for its error. This is the strictest rule in the project and this is the phase it exists for.
2. **No `time.Sleep` in tests**, with one exception: proving a negative ("nothing happened within 200ms"). Same rule as phase 6.
3. `-race -count=2` on every commit. `goleak` from Task 6 onward.
4. **Every context has a parent.** `context.Background()` appears exactly once in non-test code, in `main`. If a second one appears, a cancellation is not reaching something.
5. **The dispatcher owns no HTTP and no SQL.** It talks to interfaces it declares. It must be testable with no database.
6. At-least-once stays true. Nothing in this phase may create a path where a job silently disappears; when in doubt, let the lease expire and the reaper handle it.

---

## Part 0 — The primer

### 0.1 A context is a tree, and cancellation flows down it

`context.WithCancel(parent)` returns a child. Cancelling the parent cancels every
descendant; cancelling the child affects nothing above it. That is the whole model, and the
shutdown design is just an application of it:

```
root ctx (cancelled by SIGTERM)
├── http.Server BaseContext        → every request ctx
│   └── SSE handler's r.Context()
├── dispatch loop ctx
│   └── per-job ctx (also cancelled when the job's lease is lost)
└── reaper loop ctx
```

Three things worth knowing precisely:

**`ctx.Err()` tells you *that* it ended; `context.Cause(ctx)` tells you *why*.** With
`signal.NotifyContext` the cause is the signal, which is exactly what you want in a log line:

```
ctx.Err() = context canceled
context.Cause = terminated signal received
is Canceled  = true
```

**Cancelling is idempotent and must always happen.** `defer cancel()` on every
`WithCancel`/`WithTimeout`, including ones you "know" will be cancelled elsewhere. `go vet`
catches the missing ones; the ones it misses are the leaks.

**A cancelled context does not stop anything by itself.** It closes a channel. Code that
never selects on `ctx.Done()` and never passes the context to something that does will run
happily to completion after shutdown. Every loop in this phase must select on it.

### 0.2 `errgroup`, and the one thing everyone gets wrong

`golang.org/x/sync/errgroup` is a `WaitGroup` that collects an error and, in its
`WithContext` form, cancels a context when a goroutine fails. The spec names it, so this is
the phase that adds the dependency — the module's first non-pgx one.

Run before writing this:

```
clean return: sibling still running after a clean sibling exit
error return: Wait=boom, sibling saw context canceled
two errors:  Wait = first
SetLimit(2):  peak concurrent = 2
```

Read those four lines carefully, because the first is the trap:

**A goroutine returning `nil` does not cancel the group.** `errgroup.WithContext` cancels
only on the *first non-nil error*. So if your HTTP server returns cleanly (say
`ErrServerClosed` handled and swallowed) while the dispatch loop is still running, nothing
tells the dispatcher to stop and `Wait` blocks forever. This is the single most common
`errgroup` bug and it looks exactly like a hang on shutdown.

The fix is not to return a fake error. It is to give every long-running component the *same*
cancellation source — the root context — so they stop for the same reason at the same time.
`errgroup`'s context is for propagating *failures*; the root context is for propagating
*intent*.

**First error wins**, later ones are discarded (`Wait` returned `first`, not `second`). If
you need all of them, log inside each goroutine; `Wait`'s return is for deciding the exit
code, not for reporting.

**`SetLimit(n)`** bounds concurrency: `g.Go` blocks when n are running. That is the whole
worker pool, and it is why this phase does not need one.

### 0.3 `signal.NotifyContext` and the second Ctrl-C

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()
```

The first signal cancels `ctx`. What about an impatient operator pressing Ctrl-C again
because the drain is taking too long? Observed:

```
stop() called: a second SIGTERM now terminates immediately
signal: terminated
```

`stop()` removes the handler and restores the default disposition, so the next signal kills
the process outright. **Call `stop()` as soon as the first signal is received**, not only in
a `defer` at the end:

```go
<-ctx.Done()
stop() // a second signal now kills us, which is the correct response to "hurry up"
```

Without that, a hung drain is unkillable except with SIGKILL, and you will meet this on the
day the drain is hung.

### 0.4 A ticker is not a sleep

```go
tk := time.NewTicker(d)
defer tk.Stop()
for {
	select {
	case <-tk.C:
		// work
	case <-ctx.Done():
		return ctx.Err()
	}
}
```

Four properties that matter here:

- **The first tick comes after `d`, not immediately.** A daemon that starts and does nothing for 5 seconds looks broken. Do one pass before entering the loop.
- **Ticks are dropped, not queued** (capacity 1). If a pass takes longer than the interval you get one pending tick, not a backlog. That is the behaviour you want; the alternative would be a queue that can only grow.
- **`Stop` does not close `C`.** A `for range tk.C` after `Stop` blocks forever. Always `select` with `ctx.Done()`.
- **Sleeping instead drifts.** `time.Sleep(d)` after work that took `w` gives a period of `d+w`. A ticker's period stays `d`.

### 0.5 `goleak` finds the goroutine you forgot

```go
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
```

A ticker whose consumer never returns, with no `Stop`:

```
goleak: Errors on successful test run: found unexpected goroutines:
[Goroutine 7 in state chan receive, with x.TestForgottenTicker.func1 on top of the stack:
x.TestForgottenTicker.func1()
	.../leak_test.go:16 +0x49
created by x.TestForgottenTicker in goroutine 6
```

Note **"Errors on successful test run"** — every test passed. That is the point: a leaked
goroutine breaks nothing until the process has been up for a week. `goleak` is the only
thing in the toolchain that notices.

It works by sampling the goroutine profile after tests finish and diffing against what was
running at the start, so it needs a moment for goroutines to actually exit — which is why
its complaints are sometimes fixed by *waiting* for what you launched rather than by
changing what you launched.

Two things will make it complain about code you did not write: pgx's pool keeps background
goroutines, and testcontainers keeps a reaper. Use `goleak.IgnoreTopFunction(...)` for those
specifically. Never a blanket ignore — the point is the ones you did write.

### 0.6 The trap phase 6 built for you

`http.Server.Shutdown` stops accepting connections and **waits for active handlers to
return**. Your SSE handler returns when its request context is cancelled. Shutdown does not
cancel request contexts. So:

```
Shutdown, SSE client connected:    took 3s     err=context deadline exceeded
...with BaseContext cancelled:     took 0s     err=<nil>
```

Three seconds is the timeout I gave it; with a real 30-second drain window every deploy
would take 30 seconds and then kill live streams abruptly anyway.

The fix is `BaseContext`, which supplies the parent context for every request the server
handles:

```go
srv := &http.Server{
	Handler:     api,
	BaseContext: func(net.Listener) context.Context { return baseCtx },
}
```

Cancel `baseCtx` first, then call `Shutdown`. Every streaming handler sees `r.Context().Done()`,
returns, and the drain completes instantly. Ordinary request handlers are unaffected — they
were about to finish anyway.

This is worth internalising beyond this project: **`Shutdown` drains handlers, it does not
interrupt them.** Anything long-lived needs its own cancellation, wired to the same signal.

### 0.7 Bounded concurrency, and why the bound is not optional

`Lease(ctx, agent, n, d)` already limits how many jobs you take per pass. The bound this
phase adds is on how many run *at once*. Without it, a queue with 10,000 pending jobs
becomes 10,000 goroutines, 10,000 open sockets, and an OOM.

`errgroup.SetLimit(n)` is the mechanism. The subtlety is what happens at the boundary:
`g.Go` **blocks** when the limit is reached. So the dispatch loop must not call `g.Go` from
the same goroutine that needs to stay responsive to `ctx.Done()`, or shutdown waits for a
slot. Two ways out — lease no more than the number of free slots, or select on the context
before submitting. The first is better, and Task 2 uses it.

---

## What this phase actually builds

Everything is in place except the thing that makes a queue a queue: **nothing runs jobs**.
A job enqueued today sits at `pending` forever. Phase 7 is the first time the system moves
on its own.

```
ticker ──▶ store.Lease(n) ──▶ per-job goroutine ──▶ Runner.Run(ctx, job)
                                   │                      │
                                   │                      ├─ nil   ──▶ store.Complete(succeeded)
                                   │                      └─ error ──▶ store.Fail(cause, policy)
                                   │                                        │
                                   └─ renewal ticker ──▶ store.Renew        └─▶ pending (retry) | dead
ticker ──▶ store.ReapExpired()  ──▶ pending
```

Every one of those store calls already publishes to the hub, so the whole thing shows up on
`GET /events` the moment it works. That is your debugging tool for this phase: open a stream
and watch.

### The honest gap

There are no agents until phase 10. So what runs the job?

A `Runner` interface declared by `dispatch`, with a placeholder implementation in
`cmd/conveyord` that does nothing but succeed after a short delay. Phase 9 replaces it with
"push down the agent's gRPC stream" and the dispatcher does not change.

This is not a fake for testing's sake — it is the seam the real thing plugs into, and having
it now means the lifecycle work is done before the network work makes everything harder to
debug. Mark the placeholder clearly:

```go
// ponytail: placeholder runner, replaced by gRPC assignment in phase 9.
```

### The shutdown order, and why

```
SIGTERM
 └─▶ cancel root ctx, and call stop() so a second signal kills us
      ├─▶ cancel baseCtx        SSE handlers return (0.6)
      ├─▶ srv.Shutdown(drainCtx) stop accepting, finish in-flight requests
      ├─▶ dispatch loop returns  stops leasing new work
      │    └─▶ in-flight jobs    finish, or are abandoned at the drain deadline
      ├─▶ reaper loop returns
      └─▶ pool.Close()           last: everything above may still be writing
```

Getting the last line wrong is the classic Go shutdown bug — closing the pool while a
handler or a job completion is still using it, and panicking on the way out. The rule that
prevents it: **the pool is created first and closed last**, and `defer pool.Close()` in
`run` gives you that for free as long as nothing outlives `run`.

**What happens to a job still running when the deadline passes?** Nothing special, and that
is the design: its lease expires, the reaper returns it to `pending`, and it runs again.
At-least-once is doing the work here — which is precisely why the executor must be
idempotent, and why abandoning is a legitimate answer rather than a data-loss bug.

---

## Task 1 — The dispatch loop

**Goal:** a loop that leases work on a ticker and stops when told, with no database and no
HTTP in the tests.

### Steps

1. Create `internal/dispatch/dispatch.go`.
2. Declare the interfaces it needs; do not import `internal/store`.
3. Tests with a fake store.
4. Commit.

### The shape

```go
// Store is the slice of storage the dispatcher needs.
type Store interface {
	Lease(ctx context.Context, leasedBy string, n int, d time.Duration) ([]job.Job, error)
	Renew(ctx context.Context, id, leasedBy string, d time.Duration) error
	Complete(ctx context.Context, id, leasedBy string, state job.State, message string) error
	Fail(ctx context.Context, id, leasedBy string, cause error, p job.Policy) (job.Decision, error)
}

// Runner executes one job. Phase 9 replaces the local implementation with
// assignment over an agent's gRPC stream; this interface does not change.
type Runner interface {
	Run(ctx context.Context, j job.Job) error
}

type Config struct {
	Name        string        // this dispatcher's identity, stored as leased_by
	Interval    time.Duration // how often to poll when there is no work
	Lease       time.Duration // how long a lease is held
	MaxInFlight int           // jobs running at once
	Policy      job.Policy
}

func New(store Store, runner Runner, log *slog.Logger, cfg Config) *Dispatcher

// Run polls until ctx is cancelled, then returns after in-flight jobs finish.
func (d *Dispatcher) Run(ctx context.Context) error
```

Four decisions worth understanding:

**`Run` blocks and returns an error.** It is not `Start()`/`Stop()`. A blocking `Run(ctx)`
composes with `errgroup` in one line and has exactly one way to stop, so there is no state
machine and no "stopped twice" case. Same shape as `http.Server.Serve`.

**`Name` is configuration, not a global.** Two dispatchers in one process would otherwise
share a `leased_by` and each could complete the other's jobs. Default it to
`hostname-pid` in `main`.

**The dispatcher declares `Store`, not `store.Store`.** Same reason `httpapi` does: the
tests must run without Postgres, and the loop's logic is worth testing at microsecond speed.
There is a real cost — the interface duplicates five signatures — and it is worth it here
because this is the package with the most branching logic in the project.

**Return `ctx.Err()` on cancellation, not `nil`.** The caller can decide whether a
context-cancelled return is an error; a `nil` return loses the information. `errgroup` at
the top will see `context.Canceled` and `main` will recognise it as a clean stop.

### Requirements

- One pass before the first tick, so a fresh process picks up a backlog immediately.
- After a pass that leased a **full** batch, loop again without waiting: a backlog should drain at the speed of the work, not the ticker.
- After a pass that leased nothing or a partial batch, wait for the next tick.
- A `Lease` error is logged and the loop continues. The database being briefly unavailable is not a reason for the daemon to exit; it is a reason to try again in `Interval`.
- Every `select` includes `ctx.Done()`.
- Jitter the interval by ±10%. With one process it changes nothing; with three replicas it stops them from polling in lockstep. Two lines now, versus a puzzling load pattern later.

### Tests

These live in `internal/dispatch/dispatch_test.go` as `package dispatch` — the tests need
the unexported fake and read internal fields.

```go
// fakeStore is a Store that records calls and hands out scripted jobs. Locked
// because the dispatcher calls it from several goroutines.
type fakeStore struct {
	mu sync.Mutex

	lease    func(n int) ([]job.Job, error) // nil means "no work"
	leases   int
	renewed  []string
	complete []string
	failed   []string
}

func (f *fakeStore) Lease(_ context.Context, _ string, n int, _ time.Duration) ([]job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases++
	if f.lease == nil {
		return nil, nil
	}
	return f.lease(n)
}

func (f *fakeStore) Renew(_ context.Context, id, _ string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewed = append(f.renewed, id)
	return nil
}

func (f *fakeStore) Complete(_ context.Context, id, _ string, _ job.State, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.complete = append(f.complete, id)
	return nil
}

func (f *fakeStore) Fail(_ context.Context, id, _ string, _ error, _ job.Policy) (job.Decision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, id)
	return job.Decision{State: job.Pending}, nil
}

// runnerFunc adapts a function to Runner.
type runnerFunc func(context.Context, job.Job) error

func (f runnerFunc) Run(ctx context.Context, j job.Job) error { return f(ctx, j) }

func newTestDispatcher(t *testing.T, s Store, r Runner) *Dispatcher {
	t.Helper()
	return New(s, r, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Name:        "test",
		Interval:    time.Millisecond,
		Lease:       time.Minute,
		MaxInFlight: 4,
		Policy:      job.Policy{Base: time.Millisecond, Max: time.Second, MaxAttempts: 3},
	})
}

// run starts the dispatcher and returns a stop function that cancels it and
// waits for Run to return. Every test needs this shape.
func run(t *testing.T, d *Dispatcher) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	// Exits when ctx is cancelled, which the returned stop function does and
	// then waits for.
	go func() { errc <- d.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v, want nil or context.Canceled", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return within 5s of cancellation")
		}
	}
}

// A fresh process must not wait a full interval before looking for work.
func TestFirstPassHappensBeforeTheFirstTick(t *testing.T) {
	s := &fakeStore{}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))
	d.cfg.Interval = time.Hour // no tick will arrive during this test

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return s.leaseCount() > 0 })
}

func TestCancellationReturnsPromptly(t *testing.T) {
	s := &fakeStore{}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))

	stop := run(t, d) // stop asserts Run returns within 5s
	stop()
}

// The database going away must not take the daemon with it.
func TestALeaseErrorDoesNotStopTheLoop(t *testing.T) {
	s := &fakeStore{lease: func(int) ([]job.Job, error) { return nil, errors.New("connection refused") }}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return s.leaseCount() >= 3 })
}

// A backlog should drain at the speed of the work, not at the speed of the
// ticker. A full batch means there is probably more.
func TestAFullBatchIsFollowedImmediately(t *testing.T) {
	var handed atomic.Int64
	s := &fakeStore{lease: func(n int) ([]job.Job, error) {
		if handed.Load() >= 8 {
			return nil, nil
		}
		out := make([]job.Job, n)
		for i := range out {
			out[i] = job.Job{ID: fmt.Sprintf("j%d", handed.Add(1)), State: job.Leased}
		}
		return out, nil
	}}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))
	d.cfg.Interval = time.Hour // only the drain path can produce a second pass

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return s.completedCount() >= 8 })
}

func TestNothingLeasedWaitsForTheTicker(t *testing.T) {
	s := &fakeStore{}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))
	d.cfg.Interval = 500 * time.Millisecond

	stop := run(t, d)
	defer stop()

	// One immediate pass, then nothing for a while: proving a negative, so the
	// wait is the assertion.
	time.Sleep(200 * time.Millisecond)
	if n := s.leaseCount(); n != 1 {
		t.Errorf("made %d lease attempts in 200ms with a 500ms interval, want 1", n)
	}
}
```

Add the small locked accessors (`leaseCount`, `completedCount`, `failedIDs`, `renewedIDs`)
next to `fakeStore`, and copy `waitFor` from `internal/httpapi/event_server_test.go` — it is
the same Gosched-based helper, and duplicating eight lines beats exporting a test helper
from another package.

---

## Task 2 — Running jobs, bounded

**Goal:** leased jobs actually run, at most `MaxInFlight` at once, and their outcomes reach
the store.

### Steps

1. Add `golang.org/x/sync` to the module.
2. Run each leased job under an `errgroup` with `SetLimit`.
3. Record success with `Complete`, failure with `Fail`.
4. Commit.

### The mechanism

```go
g, gctx := errgroup.WithContext(ctx)
g.SetLimit(d.cfg.MaxInFlight)
...
for _, j := range leased {
	g.Go(func() error {
		d.runOne(gctx, j)
		return nil // a failed job is not a failed dispatcher
	})
}
...
// on the way out
return g.Wait()
```

**`runOne` returns nothing and `g.Go` always returns nil.** This looks like throwing away
errors and is deliberate: a job failing is *data*, recorded with `Fail` and visible on the
event stream. If it were an `errgroup` error it would cancel `gctx`, which would kill every
other running job because one of them failed. Write that reasoning down; it is the kind of
thing a reviewer "fixes".

**Lease no more than the free capacity.** `g.Go` blocks when the limit is reached, and
blocking there means the loop is not watching `ctx.Done()`:

```go
free := d.cfg.MaxInFlight - int(d.inFlight.Load())
if free <= 0 {
	// wait for a slot or for shutdown, whichever comes first
}
leased, err := d.store.Lease(ctx, d.cfg.Name, free, d.cfg.Lease)
```

The alternative — lease a fixed batch and let `g.Go` block — leaves jobs leased but not
started, holding a lease they are not using, which the reaper will eventually take back
while they sit in a queue. Worse than it sounds.

### Recording the outcome

```go
func (d *Dispatcher) runOne(ctx context.Context, j job.Job) {
	err := d.run(ctx, j)          // wraps Runner.Run, recovers panics
	if err != nil {
		dec, ferr := d.store.Fail(ctx, j.ID, d.cfg.Name, err, d.cfg.Policy)
		...
		return
	}
	if err := d.store.Complete(ctx, j.ID, d.cfg.Name, job.Succeeded, ""); err != nil {
		...
	}
}
```

Three requirements that are easy to miss:

**A panic in a runner must become a failure, not a dead process.** Phase 5 taught this for
HTTP; the same argument applies harder here, because a panicking job would otherwise take
down every other running job with the process. Recover, convert to an error, log the stack.

**Completion must not use a cancelled context.** During shutdown, `ctx` is already cancelled
by the time a job finishes — so `Complete` would fail with `context.Canceled` and the job
would look abandoned despite having succeeded. Use a short detached context for the
recording call:

```go
recCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
defer cancel()
```

`context.WithoutCancel` keeps values (request IDs, traces) and drops cancellation. This is
the single most valuable line in the phase and it is invisible until you test shutdown.

**A `Fail` or `Complete` error is logged, not returned.** There is nobody to return it to.
The job keeps its lease, the lease expires, the reaper returns it to `pending`, and it runs
again — at-least-once holding the line.

### Tests

```go
func TestASuccessfulJobIsCompleted(t *testing.T) {
	s := &fakeStore{lease: oneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return len(s.completedIDs()) > 0 })
	if got := s.completedIDs()[0]; got != "j1" {
		t.Errorf("completed %q, want j1", got)
	}
	if ids := s.failedIDs(); len(ids) != 0 {
		t.Errorf("a successful job was also failed: %v", ids)
	}
}

func TestAFailedJobIsReportedWithItsCause(t *testing.T) {
	want := errors.New("executor exploded")
	s := &fakeStore{lease: oneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return want }))

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return len(s.failedIDs()) > 0 })
	if got := s.failCause(); !errors.Is(got, want) {
		t.Errorf("Fail got cause %v, want %v", got, want)
	}
}

// One job panicking must not take the process — or any other job — down.
func TestAPanickingJobBecomesAFailure(t *testing.T) {
	s := &fakeStore{lease: oneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error {
		panic("boom")
	}))

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return len(s.failedIDs()) > 0 })
	if got := s.failCause(); got == nil || !strings.Contains(got.Error(), "boom") {
		t.Errorf("Fail got cause %v, want one mentioning the panic", got)
	}
}

// The bound is the difference between a backlog and an OOM.
func TestNeverMoreThanMaxInFlight(t *testing.T) {
	var running, peak atomic.Int64
	release := make(chan struct{})

	s := &fakeStore{lease: manyJobs(50)}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		running.Add(-1)
		return nil
	}))
	d.cfg.MaxInFlight = 3

	stop := run(t, d)
	waitFor(t, func() bool { return running.Load() == 3 })
	close(release)
	stop()

	if peak.Load() > 3 {
		t.Errorf("%d jobs ran at once, want at most 3", peak.Load())
	}
}

// Shutdown must reach the running job, or a deploy waits for the longest job
// in the queue instead of asking it to stop.
func TestShutdownCancelsARunningJob(t *testing.T) {
	started := make(chan struct{})
	var sawCancel atomic.Bool

	s := &fakeStore{lease: oneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(ctx context.Context, _ job.Job) error {
		close(started)
		<-ctx.Done()
		sawCancel.Store(true)
		return ctx.Err()
	}))

	stop := run(t, d)
	<-started
	stop() // cancels, and asserts Run returns

	if !sawCancel.Load() {
		t.Error("the running job's context was never cancelled")
	}
}

// A job that finishes as the process is shutting down has still finished. Its
// result must be recorded, not lost to the cancelled context that ended it.
func TestAJobFinishingDuringShutdownIsStillRecorded(t *testing.T) {
	started := make(chan struct{})
	s := &fakeStore{lease: oneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(ctx context.Context, _ job.Job) error {
		close(started)
		<-ctx.Done()
		return nil // finished successfully, just as the world ended
	}))

	stop := run(t, d)
	<-started
	stop()

	if ids := s.completedIDs(); len(ids) != 1 {
		t.Errorf("completed %v, want j1 recorded despite the cancellation", ids)
	}
}
```

`oneJob(id)` and `manyJobs(n)` are two-line helpers returning a `lease` function; write them
next to `fakeStore`.

The last test is the one that fails without `context.WithoutCancel`, and it is worth running
it against a deliberately broken version once so you see the failure before you fix it.

---

## Task 3 — Renewing the lease while a job runs

**Goal:** a job that takes longer than its lease does not get stolen; a dispatcher that has
lost its lease stops working.

### Steps

1. Start a renewal goroutine per running job.
2. Stop it when the job finishes.
3. Cancel the job when renewal fails.
4. Commit.

### Why this exists

`store.Renew` was built in phase 4 and has had no caller since. Without one, the lease
duration is a hard cap on job length: exceed it and the reaper hands your job to someone
else while you are still running it. Two dispatchers then run the same job at the same time,
and both report a result.

At-least-once means duplicate *execution* is acceptable. Duplicate execution
*while the first is still running* is worse — it is the case idempotency is hardest to get
right for.

### The mechanism

```go
// Exits when the job finishes: runOne cancels renewCtx in a defer.
go func() {
	tk := time.NewTicker(d.cfg.Lease / 3)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			if err := d.store.Renew(renewCtx, j.ID, d.cfg.Name, d.cfg.Lease); err != nil {
				cancelJob(err) // we no longer hold this job
				return
			}
		case <-renewCtx.Done():
			return
		}
	}
}()
```

**Renew at a third of the lease, not at the lease.** Two renewals must be able to fail
before the lease expires — one for the transient error, one for the retry. A half-interval
leaves no margin; a third is the usual choice and is worth a comment saying why.

**Failing to renew must cancel the job.** This is the important half. `Renew` returns
`job.ErrNotFound` when the row is no longer leased by you, which means someone else owns it
now. Continuing to run is doing work whose result you are not allowed to record — and if you
record it anyway you overwrite the new owner's outcome. Stop immediately.

That gives the per-job context two cancellation sources, which is exactly what
`context.WithCancel` composes:

```
job ctx
├── parent: dispatcher ctx  (shutdown)
└── explicit cancel         (lost the lease)
```

Use `context.WithCancelCause(parent)` so the runner can tell why it was stopped —
`context.Cause(ctx)` then returns your `ErrLeaseLost` rather than a bare `context.Canceled`.

### Tests

```go
func TestALongJobHasItsLeaseRenewed(t *testing.T) {
	release := make(chan struct{})
	s := &fakeStore{lease: oneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error {
		<-release
		return nil
	}))
	d.cfg.Lease = 30 * time.Millisecond // renews every 10ms

	stop := run(t, d)
	waitFor(t, func() bool { return len(s.renewedIDs()) >= 2 })
	close(release)
	stop()
}

// The renewal goroutine is per job. If it outlives the job, a dispatcher that
// has run a million jobs has a million tickers.
func TestRenewalStopsWhenTheJobFinishes(t *testing.T) {
	s := &fakeStore{lease: onceOneJob("j1")} // hands out j1 exactly once
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))
	d.cfg.Lease = 30 * time.Millisecond

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return len(s.completedIDs()) == 1 })
	after := len(s.renewedIDs())

	// Proving a negative: no renewals for a job that is over.
	time.Sleep(100 * time.Millisecond)
	if got := len(s.renewedIDs()); got != after {
		t.Errorf("renewals continued after the job finished: %d -> %d", after, got)
	}
}

// Losing the lease means someone else owns this job now. Continuing to run it
// produces a result we are not allowed to record.
func TestLosingTheLeaseCancelsTheJob(t *testing.T) {
	var cause atomic.Value
	s := &fakeStore{
		lease: oneJob("j1"),
		renew: func(string) error { return fmt.Errorf("%w: j1", job.ErrNotFound) },
	}
	d := newTestDispatcher(t, s, runnerFunc(func(ctx context.Context, _ job.Job) error {
		<-ctx.Done()
		cause.Store(context.Cause(ctx))
		return ctx.Err()
	}))
	d.cfg.Lease = 30 * time.Millisecond

	stop := run(t, d)
	waitFor(t, func() bool { return cause.Load() != nil })
	stop()

	if got, _ := cause.Load().(error); !errors.Is(got, ErrLeaseLost) {
		t.Errorf("job was cancelled with %v, want ErrLeaseLost", got)
	}
}
```

`fakeStore` gains a `renew func(id) error` hook. `ErrLeaseLost` is a sentinel exported from
`dispatch` — the runner may reasonably want to distinguish "we are shutting down" from "this
job is not ours any more", and phase 10's agent definitely will.

---

## Task 4 — The reaper loop

**Goal:** expired leases return to `pending` on their own.

### Steps

1. A second loop calling `store.ReapExpired` on a slower ticker.
2. Test it against a fake, and once against a real database.
3. Commit.

### The decision: one loop or two

`ReapExpired` could be called at the top of each dispatch pass — no second goroutine, no
second ticker. It is tempting and it is wrong for one reason: **the reaper must keep running
when leasing is saturated.** With `MaxInFlight` jobs running and no free slots, the dispatch
pass is either blocked or skipping, and that is exactly when leases are most likely to
expire. Coupling them means the recovery mechanism stops precisely when it is needed.

So: a separate `func (d *Dispatcher) Reap(ctx context.Context) error`, started as its own
`errgroup` member, on a slower ticker (30s to a minute — reaping is recovery, not a hot
path).

That is the second exception to "one loop is simpler", and both exceptions in this project
have the same shape: don't couple a recovery path to the path it recovers.

### Requirements

- Same loop discipline as Task 1: immediate first pass, `ctx.Done()` in the select, errors logged and the loop continues.
- Log at `Info` when it reaps something, `Debug`/nothing when it reaps zero. A reaper that logs every 30 seconds forever teaches operators to ignore its output.
- The count comes from `ReapExpired`, which already publishes one event per reaped job — no extra work here.

### Tests

```go
func TestReapRunsOnItsTicker(t *testing.T) {
	s := &fakeStore{}
	d := newTestDispatcher(t, s, noopRunner)
	d.cfg.ReapInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	// Exits on cancel below.
	go func() { errc <- d.Reap(ctx) }()

	waitFor(t, func() bool { return s.reapCount() >= 3 })
	cancel()
	if err := <-errc; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Reap returned %v", err)
	}
}

func TestReapSurvivesAnError(t *testing.T) {
	s := &fakeStore{reap: func() (int, error) { return 0, errors.New("connection refused") }}
	d := newTestDispatcher(t, s, noopRunner)
	d.cfg.ReapInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Exits on the deferred cancel.
	go func() { _ = d.Reap(ctx) }()

	waitFor(t, func() bool { return s.reapCount() >= 3 })
}
```

And one against a real database, in `internal/store` where the fixtures already exist —
phase 4 may already cover the SQL, in which case assert the *event*, which is new:

```go
func TestReapedJobPublishesPending(t *testing.T) {
	s := freshDB(t, &capturePublisher{})
	// enqueue, lease with a negative duration so the lease is already expired,
	// then ReapExpired, then assert a pending event was published for the job.
}
```

---

## Task 5 — The root: signals, errgroup, ordered shutdown

**Goal:** `main` starts everything, stops everything in the right order, and exits with a
useful status.

### Steps

1. Refactor `run` to take a context and return an error (deferred from phase 5).
2. `signal.NotifyContext` at the top.
3. `errgroup` for the three long-running components.
4. Ordered shutdown with a drain deadline.
5. Commit.

### The shape

```go
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], log, stop); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}
```

`run` takes `args` rather than reading `os.Args`, and takes `stop` so it can disarm the
signal handler the moment the first signal lands (0.3). Passing the logger keeps `run`
testable without touching the default.

Inside, in order:

```go
// 1. pool first, closed last
pool, err := pgxpool.New(context.WithoutCancel(ctx), *dsn)   // pool outlives ctx
defer pool.Close()

// 2. one hub, shared (phase 6)
events := hub.New()
st := store.New(pool, events)

// 3. base context for HTTP, so streaming handlers can be told to stop
baseCtx, cancelBase := context.WithCancel(context.Background())
defer cancelBase()

srv := &http.Server{
	Handler:     httpapi.New(st, events, log),
	BaseContext: func(net.Listener) context.Context { return baseCtx },
	...
}

d := dispatch.New(st, runner, log, cfg)

g, gctx := errgroup.WithContext(ctx)
g.Go(func() error { return serve(srv, ln) })   // returns nil on ErrServerClosed
g.Go(func() error { return d.Run(gctx) })
g.Go(func() error { return d.Reap(gctx) })
g.Go(func() error {                            // the shutdown sequencer
	<-gctx.Done()
	stop()                                     // a second signal now kills us
	cancelBase()                               // SSE handlers return
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), drainTimeout)
	defer cancel()
	return srv.Shutdown(shutCtx)
})
err = g.Wait()
```

**The pool's context must not be the root context.** `pgxpool.New` retains the context it is
given for background connection management, so passing a context that is cancelled on
SIGTERM makes the pool useless exactly when the drain needs it. `context.WithoutCancel(ctx)`
— the same tool as Task 2, for the same reason.

**The shutdown sequencer is a goroutine in the group, not code after `Wait`.** Code after
`Wait` runs only once every component has returned, and `Shutdown` is what makes the server
return, so putting it there deadlocks. A member that waits on `gctx.Done()` and then does
the ordered teardown is the standard shape.

**`serve` must swallow `http.ErrServerClosed`.** A clean shutdown returns it, and treating it
as an error means every normal exit reports failure.

**Nothing here waits for in-flight jobs explicitly** — `d.Run` does that internally via
`g.Wait()` on its own group, so `errgroup.Wait` at the root returns only once jobs are done
or their contexts have been cancelled and they have returned.

### Requirements

- Flags: keep `-addr` and `-dsn`, add `-max-in-flight`, `-poll`, `-lease`, `-drain`. Defaults must make `go run ./cmd/conveyord` work against a local Postgres with no flags.
- `Name` defaults to `hostname-pid`.
- Log one line at startup with the effective configuration. The first question in any incident is "what was it actually running with".
- Log one line when shutdown begins, and one when it completes, with the elapsed time.
- Exit code 0 on a clean signal-triggered shutdown, 1 on anything else. `errors.Is(err, context.Canceled)` is clean.

### Tests

`run` being a function taking a context makes the whole lifecycle testable in-process:

```go
// The end of the phase: a job running when the signal arrives is not lost.
func TestSIGTERMDrainsAnInFlightJob(t *testing.T) {
	dsn := pgtest.DSN(t) // needs the small addition to pgtest described below

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	// Exits when ctx is cancelled below, which is what a signal does to the
	// context main builds.
	go func() {
		errc <- run(ctx, []string{"-addr", "127.0.0.1:0", "-dsn", dsn}, testLogger(t), func() {})
	}()

	// ...enqueue a job over HTTP, wait for it to reach leased on the event
	// stream, then cancel and assert: run returns within the drain window,
	// and the job is not left in a leased state with nobody holding it.
}
```

Two things this needs and one caveat:

`run` must report the address it bound to, since `-addr :0` is the only way to test without
picking a port. Add an optional `func(net.Addr)` callback parameter, or have `run` write the
address to a channel — the callback is less machinery.

`pgtest` needs to expose the DSN, not just a pool, because `run` takes a DSN string. Three
lines next to `Pool`.

**The caveat:** this test is the most valuable one in the phase and also the most likely to
be flaky, because it exercises real timing. If it flakes, do not widen a timeout — find
which wait is not tied to an observable condition and fix that. `waitFor` on the event
stream is the tool.

---

## Task 6 — `goleak`

**Goal:** proof that no goroutine outlives the thing that started it.

### Steps

1. Add `go.uber.org/goleak`.
2. `TestMain` in `internal/dispatch`, `internal/hub`, `internal/httpapi`.
3. Add it to the packages with a database last, with targeted ignores.
4. Commit.

### Requirements

```go
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
```

For `internal/store` and `cmd/conveyord`, which have pgx and testcontainers underneath, wrap
the existing `pgtest.Run`:

```go
func TestMain(m *testing.M) {
	code := pgtest.Run(m)
	if err := goleak.Find(
		goleak.IgnoreTopFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		// ...one line per real, named exception, each with a comment
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}
```

**Every ignore is a named function with a comment saying why.** A blanket ignore turns the
tool off while leaving the impression it is on. If you cannot name the goroutine, you have
found something worth understanding rather than something worth ignoring.

Expect this task to fail the first time you run it. That is its entire value — the failures
it finds are real and they are in code you wrote in Tasks 1–5.

---

## Phase review checklist

```bash
gofmt -l .
go vet ./...
staticcheck -checks=all ./...
go test -race -count=2 ./...
go test -race -count=10 ./internal/dispatch/
go list -deps ./internal/job | grep -v '^github.com/mddemin/conveyor' | grep '\.'
```

`internal/job` must still print nothing. `internal/dispatch` may depend on `internal/job`
and `golang.org/x/sync` — and nothing else. If `internal/store` appears in its imports, the
interface it declares is not being used.

By eye:

- Every `go` statement has a comment naming its stop signal and where its error goes.
- `context.Background()` appears exactly once outside tests.
- Every long-running loop selects on `ctx.Done()`.
- Every `NewTicker` has a `defer Stop()`.
- Result recording uses a context that survives shutdown (`WithoutCancel`), and the reason is written down.
- A panic in a runner cannot reach `main`.
- The pool is created before everything and closed after everything.
- `http.ErrServerClosed` is not reported as a failure.
- `stop()` is called when the first signal arrives, so the second one kills the process.

By hand:

```bash
go run ./cmd/conveyord &
curl -N localhost:8080/events &
curl -sS -X POST localhost:8080/jobs -d '{"type":"resize","payload":{"w":1}}'
```

You should see `pending` then `leased` then `succeeded` on the stream without touching
anything else. Then, with a job in flight, `kill -TERM` the process and watch: the stream
should end promptly, the shutdown log line should show a short elapsed time, and the exit
code should be 0.

---

## What phase 8 inherits

A running system: jobs enqueue, dispatch, execute, retry, and stream — all in one process,
with a placeholder runner. Phase 8 defines the protobuf schema and phase 9 replaces
`Runner.Run` with "assign over the agent's stream", which is a change to *one interface
implementation* if this phase's boundaries hold.

The two things phase 7 leaves deliberately undone: the dispatcher is one process (two
replicas both poll, which `SKIP LOCKED` makes safe but which nothing coordinates), and the
placeholder runner does no real work. Neither is a gap to fix now; both are context for why
phase 9 looks the way it does.
