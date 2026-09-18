package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
	"go.uber.org/goleak"
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		runtime.Gosched()
	}
}

// fakeStore is a Store that records calls and hands out scripted jobs. Locked
// because the dispatcher calls it from several goroutines — and the test
// goroutine reads through the accessors, so every reader takes the lock too.
type fakeStore struct {
	mu sync.Mutex

	lease        func(n int) ([]job.Job, error) // nil means "no work"
	renew        func(id string) error          // nil means "renewal succeeds"
	leases       int
	renewed      []string
	complete     []string
	failed       []string
	released     []string
	releaseWhy   string              // message passed to the most recent Release call
	releaseAfter time.Duration       // retryAfter passed to the most recent Release call
	lastCause    error               // cause passed to the most recent Fail call
	budgets      []time.Duration     // recording budget left when Complete/Fail was called
	reap         func() (int, error) // nil means "nothing expired"
	reaps        int
}

// budgetLeft reports how much of the recording deadline is still unspent when
// a store call arrives. It is the observable for "the budget starts when the
// job ends": a run that eats it leaves nothing to write the outcome with.
func budgetLeft(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return time.Until(deadline)
}

// Every method starts by refusing a cancelled context. pgx does this — a query on a
// cancelled context fails before touching the database — and the fake must too, or
// Task 2's shutdown tests cannot tell a recorded result from a discarded one.
func refused(ctx context.Context) error {
	return ctx.Err()
}

func (f *fakeStore) Lease(ctx context.Context, _ string, n int, _ time.Duration) ([]job.Job, error) {
	if err := refused(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases++
	if f.lease == nil {
		return nil, nil
	}
	return f.lease(n)
}

func (f *fakeStore) Renew(ctx context.Context, id, _ string, _ time.Duration) error {
	if err := refused(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	f.renewed = append(f.renewed, id)
	hook := f.renew
	f.mu.Unlock()
	if hook != nil {
		return hook(id)
	}
	return nil
}

func (f *fakeStore) Complete(ctx context.Context, id, _ string, _ job.State, _ string) error {
	if err := refused(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.complete = append(f.complete, id)
	f.budgets = append(f.budgets, budgetLeft(ctx))
	return nil
}

func (f *fakeStore) Fail(ctx context.Context, id, _ string, cause error, _ job.Policy) (job.Decision, error) {
	if err := refused(ctx); err != nil {
		return job.Decision{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failed = append(f.failed, id)
	f.lastCause = cause
	f.budgets = append(f.budgets, budgetLeft(ctx))
	return job.Decision{State: job.Pending}, nil
}

func (f *fakeStore) Release(ctx context.Context, id, _, message string, retryAfter time.Duration) error {
	if err := refused(ctx); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, id)
	f.releaseWhy = message
	f.releaseAfter = retryAfter
	f.budgets = append(f.budgets, budgetLeft(ctx))
	return nil
}

// Accessors. Every one locks: a getter racing a mutator is the data race
// that brought this suite down once already.
func (f *fakeStore) leaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leases
}

func (f *fakeStore) completedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.complete)
}

func (f *fakeStore) completedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.complete)
}

func (f *fakeStore) failedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.failed)
}

func (f *fakeStore) releasedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.released)
}

// releaseReason reports the message the dispatcher handed to the most recent
// Release. It ends up in the job's event timeline, which is the only durable
// record of why nothing took the job — the log line is at debug.
func (f *fakeStore) releaseReason() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releaseWhy
}

// releaseDelay reports how long the most recent Release asked the store to
// hide the job for. Zero means the loop re-leases it the moment runOne's
// slotFreed token lands, which is a spin against Postgres for as long as no
// agent is connected.
func (f *fakeStore) releaseDelay() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releaseAfter
}

func (f *fakeStore) renewedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.renewed)
}

func (f *fakeStore) ReapExpired(ctx context.Context) (int, error) {
	if err := refused(ctx); err != nil {
		return 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reaps++
	if f.reap == nil {
		return 0, nil
	}
	return f.reap()
}

func (f *fakeStore) PendingCount(ctx context.Context) (int, error) {
	if err := refused(ctx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *fakeStore) reapCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reaps
}

// recordingBudgets reports the unspent recording deadline seen by each
// Complete/Fail call, in call order.
func (f *fakeStore) recordingBudgets() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.budgets)
}

// failCause reports the cause the dispatcher handed to the most recent Fail.
// The assertion downstream is errors.Is, so wrapping in the dispatcher is fine;
// replacing the error is not — the cause drives retry-versus-dead-letter.
func (f *fakeStore) failCause() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastCause
}

// runnerFunc adapts a function to Runner.
type runnerFunc func(context.Context, job.Job) error

func (f runnerFunc) Run(ctx context.Context, j job.Job) error { return f(ctx, j) }

// Scripted lease hooks. Each hands out jobs until its budget runs out, then
// reports an empty queue.
func oneJob(id string) func(int) ([]job.Job, error) {
	return func(n int) ([]job.Job, error) {
		if n <= 0 {
			return nil, nil
		}
		return []job.Job{{ID: id, State: job.Leased}}, nil
	}
}

// onceOneJob hands out id on the first call only; every later call sees an
// empty queue. Use it when a test must prove something stops after one job.
func onceOneJob(id string) func(int) ([]job.Job, error) {
	var done atomic.Bool
	return func(n int) ([]job.Job, error) {
		if n <= 0 || !done.CompareAndSwap(false, true) {
			return nil, nil
		}
		return []job.Job{{ID: id, State: job.Leased}}, nil
	}
}

func manyJobs(total int64) func(int) ([]job.Job, error) {
	var handed atomic.Int64
	return func(n int) ([]job.Job, error) {
		if handed.Load() >= total {
			return nil, nil
		}
		out := make([]job.Job, n)
		for i := range out {
			out[i] = job.Job{ID: fmt.Sprintf("j%d", handed.Add(1)), State: job.Leased}
		}
		return out, nil
	}
}

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

// --- Task 1: the loop -------------------------------------------------------

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
	s := &fakeStore{lease: manyJobs(8)}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error { return nil }))
	d.cfg.Interval = time.Hour // only the drain path can produce further passes

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

	// The immediate pass has landed. Waiting for it first keeps a slow CI
	// machine from reaching the assertion below before the pass ran at all.
	waitFor(t, func() bool { return s.leaseCount() >= 1 })
	first := s.leaseCount()

	// Proving a negative (constraint 2's one exception): nothing further
	// until the tick.
	time.Sleep(200 * time.Millisecond)
	if got := s.leaseCount(); got != first {
		t.Errorf("%d lease attempts total, want still %d (no tick-driven pass within 200ms)", got, first)
	}
}

// --- Task 2: running jobs, bounded ------------------------------------------

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

// The cause is not decoration: store.Fail feeds it to the retry policy, and
// IsPermanent on the wrapped chain decides retry versus dead-letter. A
// dispatcher that swapped the error for a generic one would make every
// permanent failure look retryable.
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

// Undelivered work costs nothing. Every ErrNoAgents bounce used to reach Fail,
// which increments the attempt and applies the policy, so a five-second agent
// restart was enough to bury the queue: leased attempt 0 to dead attempt 5
// inside a second, measured live. The job never ran, so it goes back as it was.
func TestAnUndeliveredJobIsReleasedNotFailed(t *testing.T) {
	s := &fakeStore{lease: onceOneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(_ context.Context, j job.Job) error {
		return fmt.Errorf("assign %s: %w: no agents available", j.ID, job.ErrUndelivered)
	}))

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return len(s.releasedIDs()) > 0 })
	if got := s.releasedIDs()[0]; got != "j1" {
		t.Errorf("released %q, want j1", got)
	}
	if ids := s.failedIDs(); len(ids) != 0 {
		t.Errorf("Fail called for %v: an undelivered job must not spend an attempt", ids)
	}
	// The timeline is where an operator reads this: "no agents available" means
	// deploy an agent, "agent is slow" means add one, and a bare "released"
	// means neither question can be answered from the database.
	if why := s.releaseReason(); !strings.Contains(why, "no agents available") {
		t.Errorf("Release got message %q, want it to carry the runner's reason", why)
	}
	// Without a delay the release is a spin: runOne signals slotFreed as it
	// returns, the loop wakes, and it re-leases a job nothing can take.
	if got := s.releaseDelay(); got != d.cfg.Interval {
		t.Errorf("Release asked to hide the job for %s, want the poll interval %s", got, d.cfg.Interval)
	}
}

// The other half of the distinction. An agent that took the job and vanished
// may have run it, so that attempt is spent and the policy gets its say —
// releasing here would let a job that crashes its executor retry forever.
func TestADeliveredFailureStillCostsAnAttempt(t *testing.T) {
	s := &fakeStore{lease: onceOneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error {
		return errors.New("agent disconnected before reporting")
	}))

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return len(s.failedIDs()) > 0 })
	if ids := s.releasedIDs(); len(ids) != 0 {
		t.Errorf("Release called for %v: the job was delivered, so it costs an attempt", ids)
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
//
// RED until runOne records Complete/Fail on a detached context
// (context.WithoutCancel): the recording call inherits the cancelled ctx and
// the fake refuses it, faithfully mirroring pgx.
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

// The recording budget belongs to the recording, not to the run. A job lasts
// as long as its executor holds it — with a gRPC agent that is unbounded — so
// a deadline created before the run is already spent when there is finally an
// outcome to write. pgx then refuses the write, the outcome is lost, and the
// row sits leased until the reaper: exactly the failure this budget exists to
// prevent. Observed live as "fail failed: context deadline exceeded" against
// an agent that held one job for 47s.
func TestRecordingBudgetSurvivesALongJob(t *testing.T) {
	s := &fakeStore{lease: onceOneJob("j1")}
	d := newTestDispatcher(t, s, runnerFunc(func(context.Context, job.Job) error {
		time.Sleep(time.Second) // a fraction of the 5s budget, and enough to see it
		return nil
	}))

	stop := run(t, d)
	defer stop()

	waitFor(t, func() bool { return len(s.recordingBudgets()) == 1 })
	// Budget granted after the run: ~5s left. Granted before it: ~4s, because
	// the one-second run ate it.
	if got := s.recordingBudgets()[0]; got < 4500*time.Millisecond {
		t.Errorf("Complete saw %v of recording budget left, want more than 4.5s", got)
	}
}

// One job panicking must not take the process — or any other job — down.
//
// RED until runOne recovers panics and converts them into a returned error:
// an unrecovered panic in the runner goroutine kills the whole test binary.
// This test is deliberately last so the crash cannot mask the results above.
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

var noopRunner = runnerFunc(func(context.Context, job.Job) error { return nil })

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

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
