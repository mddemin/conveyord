package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mddemin/conveyor/internal/dispatch"
	"github.com/mddemin/conveyor/internal/httpapi"
	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/store"
	"golang.org/x/sync/errgroup"
)

// jobStore adapts *store.Store to httpapi.Store: the API declares the filter
// it needs, storage declares its own, and main is where the two meet.
type jobStore struct{ *store.Store }

func (s jobStore) List(ctx context.Context, f httpapi.Filter) ([]job.Job, error) {
	return s.Store.List(ctx, store.Filter{State: f.State, Limit: f.Limit})
}

type dummyRunner struct{}

// ponytail: placeholder runner, replaced by gRPC assignment in phase 9.
func (r dummyRunner) Run(ctx context.Context, j job.Job) error {
	time.Sleep(250 * time.Millisecond) // hold the job long enough to interrupt it mid-run
	return nil
}

type config struct {
	addr         string
	dsn          string
	name         string
	drainTimeout time.Duration
	dispatch     dispatch.Config
}

func parseArgs(args []string, dsnEnv string) (config, error) {
	fs := flag.NewFlagSet("conveyord", flag.ContinueOnError)
	var (
		c            config
		poll         = fs.Duration("poll", 500*time.Millisecond, "dispatch poll interval")
		lease        = fs.Duration("lease", 30*time.Second, "how long a lease is held")
		maxInFlight  = fs.Int("max-in-flight", 10, "jobs running at once")
		reapInterval = fs.Duration("reap-interval", 30*time.Second, "expired-lease reaper interval")
		drain        = fs.Duration("drain", 15*time.Second, "graceful shutdown deadline")
	)
	fs.StringVar(&c.addr, "addr", ":8080", "listen address")
	fs.StringVar(&c.dsn, "dsn", "", "Postgres connection string")
	fs.StringVar(&c.name, "name", "", "dispatcher identity in leased_by")

	if err := fs.Parse(args); err != nil {
		return c, err
	}

	if c.name == "" {
		host, _ := os.Hostname()
		c.name = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	if c.dsn == "" {
		c.dsn = dsnEnv
	}
	if c.dsn == "" {
		return c, errors.New("no database: set -dsn or CONVEYOR_DSN")
	}

	switch {
	case *poll <= 0 || *lease <= 0 || *reapInterval <= 0 || *drain <= 0:
		return c, errors.New("durations must be positive")
	case *maxInFlight <= 0:
		return c, errors.New("-max-in-flight must be positive")
	}

	c.dispatch = dispatch.Config{
		Name:         c.name,
		Interval:     *poll,
		Lease:        *lease,
		MaxInFlight:  *maxInFlight,
		ReapInterval: *reapInterval,
		Policy:       job.Policy{Base: time.Second, Max: time.Minute, MaxAttempts: 5},
	}
	c.drainTimeout = *drain
	return c, nil
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], log, stop, func(a net.Addr) {}); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

// serve runs the server until Shutdown. A clean shutdown makes Serve return
// http.ErrServerClosed, which is not a failure — swallowing it here keeps
// first-error-wins in the group from recording every deploy as a crash.
func serve(srv *http.Server, ln net.Listener) error {
	err := srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func run(ctx context.Context, args []string, log *slog.Logger, stop context.CancelFunc, onListen func(net.Addr)) error {
	dsnEnv := os.Getenv("CONVEYOR_DSN")

	cfg, err := parseArgs(args, dsnEnv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // -h printed usage; clean exit 0
		}
		return err
	}

	pool, err := pgxpool.New(context.WithoutCancel(ctx), cfg.dsn)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()

	// Startup work gets its own deadline. A daemon that hangs forever waiting
	// for a database is worse than one that exits with a message.
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := pool.Ping(startupCtx); err != nil {
		return fmt.Errorf("pinging postgres: %w", err)
	}
	if err := store.Migrate(startupCtx, pool); err != nil {
		return fmt.Errorf("migrating: %w", err)
	}

	events := hub.New()

	st := jobStore{store.New(pool, events)}

	api := httpapi.New(st, events, log)

	g, gctx := errgroup.WithContext(ctx)
	baseCtx, cancelBase := context.WithCancel(gctx)
	defer cancelBase()

	srv := &http.Server{
		Addr:              cfg.addr,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
		Handler:           api,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
	}

	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", srv.Addr, err)
	}

	onListen(ln.Addr())
	log.Info("listening", "addr", ln.Addr().String()) // bind already succeeded

	// placeholder runner, replaced by gRPC assignment in phase 9.
	runner := dummyRunner{}

	d := dispatch.New(st, runner, log, cfg.dispatch)

	g.Go(func() error { return serve(srv, ln) }) // returns nil on ErrServerClosed
	g.Go(func() error { return d.Run(gctx) })
	g.Go(func() error { return d.Reap(gctx) })
	g.Go(func() error { // the shutdown sequencer
		<-gctx.Done()
		stop() // a second signal now kills us
		// baseCtx is a child of gctx and is already cancelled here, so every
		// streaming handler's r.Context() is done before Shutdown drains.
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.drainTimeout)

		defer cancel()
		return srv.Shutdown(shutCtx)
	})
	return g.Wait()
}
