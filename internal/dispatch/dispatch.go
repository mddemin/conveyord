package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/mddemin/conveyor/internal/job"
	"golang.org/x/sync/errgroup"
)

// Store is the slice of storage the dispatcher needs.
type Store interface {
	Lease(ctx context.Context, leasedBy string, n int, d time.Duration) ([]job.Job, error)
	Renew(ctx context.Context, id, leasedBy string, d time.Duration) error
	Complete(ctx context.Context, id, leasedBy string, state job.State, message string) error
	Fail(ctx context.Context, id, leasedBy string, cause error, p job.Policy) (job.Decision, error)
	ReapExpired(ctx context.Context) (int, error)
}

// Runner executes one job. Phase 9 replaces the local implementation with
// assignment over an agent's gRPC stream; this interface does not change.
type Runner interface {
	Run(ctx context.Context, j job.Job) error
}

type Config struct {
	Name         string        // this dispatcher's identity, stored as leased_by
	Interval     time.Duration // how often to poll when there is no work
	Lease        time.Duration // how long a lease is held
	MaxInFlight  int           // jobs running at once
	ReapInterval time.Duration // how often the reaper sweeps for expired leases
	Policy       job.Policy
}

// Dispatcher leases jobs from a Store and runs them locally through a
// Runner, holding at most cfg.MaxInFlight leases at once. It exposes two
// blocking loops, Run and Reap; neither starts goroutines that outlive them,
// and both expect the caller to close shared resources (the pool) only after
// they have returned.
type Dispatcher struct {
	store     Store
	runner    Runner
	log       *slog.Logger
	cfg       Config
	slotFreed chan struct{}
	inFlight  atomic.Int64
}

// New returns a dispatcher ready to Run and Reap. slotFreed starts empty:
// tokens appear only when workers finish, and the wait loop recomputes free
// capacity from inFlight on every wake, so a dropped token costs latency
// once, never correctness.
func New(store Store, runner Runner, log *slog.Logger, cfg Config) *Dispatcher {
	return &Dispatcher{store: store, runner: runner, log: log, cfg: cfg, slotFreed: make(chan struct{}, 1)}
}

// safeRun runs one job, converting a panicking runner into an ordinary
// error. The recover boundary sits only around the runner: a panic in the
// store or the recording path is a bug in this package and must crash loud.
func (d *Dispatcher) safeRun(ctx context.Context, j job.Job) (err error) {
	defer func() {
		v := recover()
		if v == nil {
			return
		}
		d.log.Error("runner panicked",
			"job", j.ID,
			"panic", v,
			"stack", string(debug.Stack()), // the stack dies with the panic; log it now or never
		)
		if e, ok := v.(error); ok {
			err = fmt.Errorf("%w: %w: job %s", errPanicked, e, j.ID) // preserve errors.Is chains
		} else {
			err = fmt.Errorf("%w: %v: job %s", errPanicked, v, j.ID)
		}
	}()
	return d.runner.Run(ctx, j)
}

// runOne runs j to completion and records the outcome. Context roles:
//
//	jctx     the job's kill switch; the runner runs on it. Cancelled with an
//	         ErrLeaseLost cause when the lease changes hands, by the parent
//	         on shutdown, and by the deferred cancelJob on any exit.
//	renewCtx the renewal pump's handle; a child of jctx so the pump stops
//	         the moment the job does, without inventing a second reason for
//	         the runner to stop.
//	recCtx   detached from cancellation: a result recorded during shutdown
//	         is still recorded, because pgx refuses writes on a cancelled
//	         context and the job's outcome would be lost to the reaper.
func (d *Dispatcher) runOne(ctx context.Context, j job.Job) {
	defer func() {
		// Decrement BEFORE signalling: the decrement then happens-before the
		// woken pass's read of inFlight, so a reloop never sees a stale count
		// and over-leases. Reversed order can leave the loop asleep with no
		// token coming — a hang at MaxInFlight=1.
		d.inFlight.Add(-1)
		select {
		case d.slotFreed <- struct{}{}: // leave a token for the sleeping loop
		default: // a token is already there; drop mine
		}
	}()

	jctx, cancelJob := context.WithCancelCause(ctx)
	defer cancelJob(nil)

	// The renewal pump. Owner: this runOne invocation. Stop signal:
	// renewCtx, closed whenever jctx closes. Its errors are not logged and
	// retried — losing the lease means stop working now, so they become the
	// job's cancellation cause.
	go func() {
		renewCtx, cancelRenew := context.WithCancel(jctx)
		defer cancelRenew()

		tk := time.NewTicker(d.cfg.Lease / 3)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				if err := d.store.Renew(renewCtx, j.ID, d.cfg.Name, d.cfg.Lease); err != nil {
					if renewCtx.Err() != nil {
						return // job finished or dispatcher shutting down; not a lease event
					}
					cancelJob(fmt.Errorf("%w: %v", ErrLeaseLost, err)) // someone else owns j.ID now
					return
				}
			case <-renewCtx.Done():
				return
			}
		}
	}()

	recCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	err := d.safeRun(jctx, j)
	if err != nil {
		if c := context.Cause(jctx); errors.Is(c, ErrLeaseLost) {
			err = fmt.Errorf("%w: %w", err, c) // both chains survive errors.Is
		}
		_, err := d.store.Fail(recCtx, j.ID, d.cfg.Name, err, d.cfg.Policy)
		if errors.Is(err, job.ErrNotFound) {
			// Expected exactly when the lease changed hands mid-job: the new
			// owner owns the row, our write lands as zero rows. Not noise.
			d.log.Debug("fail failed", "err", err)
		} else {
			d.log.Error("fail failed", "err", err)
		}
		return
	}

	err = d.store.Complete(recCtx, j.ID, d.cfg.Name, job.Succeeded, "Completed")
	if err != nil {
		d.log.Error("complete failed", "err", err)
		return
	}
}

// runTurn leases up to the free capacity and submits each lease to g at
// once. Because free is capped by inFlight, g.Go never blocks here — the
// loop stays watching ctx.Done(). It returns true when every requested slot
// was filled; the caller reads a full batch as "probably more work" and
// turns straight around instead of waiting for the ticker.
func (d *Dispatcher) runTurn(ctx context.Context, g *errgroup.Group) bool {
	free := d.cfg.MaxInFlight - int(d.inFlight.Load())
	for free <= 0 {
		// No slot is free. Wait for one to open or for shutdown, whichever
		// comes first. slotFreed is runOne's completion signal (below).
		select {
		case <-d.slotFreed:
		case <-ctx.Done():
			return false
		}
		free = d.cfg.MaxInFlight - int(d.inFlight.Load())
	}

	leased, err := d.store.Lease(ctx, d.cfg.Name, free, d.cfg.Lease)
	if err != nil {
		d.log.Error("lease failed", "err", err)
		return false
	}
	for _, j := range leased {
		d.inFlight.Add(1)
		// Counts leases held, not goroutines started: incrementing here,
		// synchronously with the lease decision, leaves the scheduler no gap
		// in which a reloop could read a stale count.
		g.Go(func() error {
			d.runOne(ctx, j)
			return nil
		})
	}
	full := len(leased) == free
	return full
}

// Run polls until ctx is cancelled, then drains: it stops leasing and waits
// for in-flight jobs to record their results. Returns nil on a clean cancel
// (workers always report success to the group); a non-nil return is a real
// failure worth exiting over.
func (d *Dispatcher) Run(ctx context.Context) error {
	tk := time.NewTicker(d.cfg.Interval)
	defer tk.Stop()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(d.cfg.MaxInFlight)

	for {
		full := d.runTurn(gctx, g)
		if full {
			continue // drain at work speed, not tick speed
		}
		select {
		case <-tk.C:
			d.runTurn(gctx, g)

		case <-ctx.Done():
			return g.Wait()
		}
	}
}

// reapTurn runs one reaper pass. A nonzero count is logged at Info and
// returned so the caller can turn straight around: the store caps a pass at
// DefaultLimit, so a large backlog drains over consecutive passes rather
// than consecutive ticks. Quiet passes log nothing — a reaper that logs its
// idle ticks teaches operators to ignore its output.
func (d *Dispatcher) reapTurn(ctx context.Context) int {
	reaped, err := d.store.ReapExpired(ctx)
	if err != nil {
		d.log.Error("reap failed", "err", err)
		return 0
	}
	if reaped > 0 {
		d.log.Info("reaped expired leases", "count", reaped)
	}
	return reaped
}

// Reap returns expired leases to pending on a slow ticker until ctx is
// cancelled, then returns ctx.Err(). Deliberately a separate loop from Run:
// the recovery path must keep running precisely when leasing is saturated.
func (d *Dispatcher) Reap(ctx context.Context) error {
	tk := time.NewTicker(d.cfg.ReapInterval)
	defer tk.Stop()

	for {
		if d.reapTurn(ctx) == 0 {
			select {
			case <-tk.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
