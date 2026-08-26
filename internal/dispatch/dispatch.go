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

type Dispatcher struct {
	store     Store
	runner    Runner
	log       *slog.Logger
	cfg       Config
	slotFreed chan struct{}
	inFlight  atomic.Int64
}

func New(store Store, runner Runner, log *slog.Logger, cfg Config) *Dispatcher {
	return &Dispatcher{store: store, runner: runner, log: log, cfg: cfg, slotFreed: make(chan struct{}, 1)}
}

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

func (d *Dispatcher) runOne(ctx context.Context, j job.Job) {
	defer func() {
		d.inFlight.Add(-1)
		select {
		case d.slotFreed <- struct{}{}: // leave a token for the sleeping loop
		default: // a token is already there; drop mine
		}
	}()

	jctx, cancelJob := context.WithCancelCause(ctx)
	defer cancelJob(nil)

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
		if err != nil {
			if errors.Is(err, job.ErrNotFound) {
				d.log.Debug("fail failed", "err", err)
			}
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
		g.Go(func() error {
			d.runOne(ctx, j)
			return nil
		})
	}
	full := len(leased) == free
	return full
}

// Run polls until ctx is cancelled, then returns after in-flight jobs finish.
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
