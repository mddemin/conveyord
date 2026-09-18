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
	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"github.com/mddemin/conveyor/internal/dispatch"
	"github.com/mddemin/conveyor/internal/httpapi"
	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/rpc"
	"github.com/mddemin/conveyor/internal/store"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// jobStore adapts *store.Store to httpapi.Store: the API declares the filter
// it needs, storage declares its own, and main is where the two meet.
type jobStore struct{ *store.Store }

func (s jobStore) List(ctx context.Context, f httpapi.Filter) ([]job.Job, error) {
	return s.Store.List(ctx, store.Filter{State: f.State, Limit: f.Limit})
}

// agentRegistry adapts *rpc.Registry to httpapi.Agents, the same way jobStore
// adapts the store: the API declares the shape it serves, the registry keeps
// its own, and main is where they meet.
type agentRegistry struct{ *rpc.Registry }

func (r agentRegistry) Agents() []httpapi.AgentInfo {
	agents := r.List()
	out := make([]httpapi.AgentInfo, 0, len(agents))
	for _, a := range agents {
		select {
		case <-a.Done():
			continue // CloseAll has ended it; Unregister has not yet dropped the map entry
		default:
		}
		out = append(out, httpapi.AgentInfo{ID: a.ID, Capabilities: r.Capabilities(a)})
	}
	return out
}

type config struct {
	grpcAddr     string
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
	// Loopback by default, because neither listener authenticates: anything
	// that can reach HTTP can enqueue jobs, and anything that can reach gRPC
	// registers, claims any capability, and is handed those jobs. Exposing
	// either off-host is an explicit act — see exposedWarning.
	fs.StringVar(&c.grpcAddr, "grpc-addr", "127.0.0.1:50051", "gRPC listen address; unauthenticated, so off-host exposure needs a network boundary you trust")
	fs.StringVar(&c.addr, "addr", "127.0.0.1:8080", "HTTP listen address; unauthenticated, same trust boundary as -grpc-addr")
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

	if err := run(ctx, os.Args[1:], log, stop, func(_, _ net.Addr) {}); err != nil && !errors.Is(err, context.Canceled) {
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

// serveGRPC runs the gRPC server until it is stopped. GracefulStop makes Serve
// return grpc.ErrServerStopped, which is a clean shutdown, not a failure — the
// gRPC counterpart of serve's http.ErrServerClosed.
func serveGRPC(srv *grpc.Server, ln net.Listener) error {
	err := srv.Serve(ln)
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// exposedWarning reports why a bind is a risk, or "" when it is not. Neither
// listener authenticates: HTTP enqueues jobs, gRPC takes them. Phase 10 does
// not ship mTLS, so the honest position is a loopback default plus a line in
// the log that says exactly what a wider bind means — silence would be the
// wrong answer.
func exposedWarning(addr net.Addr) string {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "" // not a TCP address: nothing to judge
	}
	ip := net.ParseIP(host)
	switch {
	case ip == nil:
		return ""
	case ip.IsUnspecified():
		return "bound to every interface with no authentication"
	case !ip.IsLoopback():
		return "bound off-loopback with no authentication: only expose this inside a network boundary you trust"
	}
	return ""
}

// run wires the whole daemon and blocks until shutdown completes. Ordering
// contract: the pool is created first and closed last (deferred); everything
// long-lived is an errgroup member cancelled by gctx; streaming handlers die
// through baseCtx before Shutdown drains them.
func run(ctx context.Context, args []string, log *slog.Logger, stop context.CancelFunc, onListen func(httpAddr, grpcAddr net.Addr)) error {
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

	reg := rpc.NewRegistry()

	api := httpapi.New(st, agentRegistry{reg}, events, log)

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

	grpcLn, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		ln.Close() // this listener has no owner yet: nothing will Serve it now
		return fmt.Errorf("listening on %s: %w", cfg.grpcAddr, err)
	}

	// Both binds succeeded, so the daemon is reachable on both protocols: one
	// callback, after the last thing that can fail before the group starts.
	onListen(ln.Addr(), grpcLn.Addr())
	log.Info("listening", "addr", ln.Addr().String(), "grpc_addr", grpcLn.Addr().String())
	if why := exposedWarning(ln.Addr()); why != "" {
		log.Warn("http listener is unauthenticated", "addr", ln.Addr().String(), "risk", why)
	}
	if why := exposedWarning(grpcLn.Addr()); why != "" {
		log.Warn("grpc listener is unauthenticated", "grpc_addr", grpcLn.Addr().String(), "risk", why)
	}

	rpcSrv := rpc.NewServer(events, reg, log)
	d := dispatch.New(st, rpcSrv, log, cfg.dispatch)

	grpcSrv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second, // ping an idle stream
			Timeout: 10 * time.Second, // then give up on it
		}),
		// Agents prove liveness with application Heartbeat frames, not with
		// transport pings, so nothing conveyor ships trips the default
		// five-minute minimum. This is for everyone else: the proto is the
		// public contract, and a third-party agent that sets grpc keepalive
		// at a sane interval would otherwise be GOAWAYed for "too_many_pings".
		//
		// PermitWithoutStream because the moment a client most wants to ping is
		// while it has no stream: holding the connection warm across a
		// reconnect. Refusing those is refusing exactly the useful case.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterConveyorServer(grpcSrv, rpcSrv)

	g.Go(func() error { return serveGRPC(grpcSrv, grpcLn) }) // returns nil after a graceful stop
	g.Go(func() error {
		<-gctx.Done()
		reg.CloseAll() // every Assign handler returns; without this GracefulStop waits forever
		// One flag for shutdown, not two. The ladder is deliberate: CloseAll
		// ends the streams, GracefulStop drains what is left, and the hard
		// Stop is the backstop for a handler that ignores both — so it must
		// outlast neither the drain budget nor the operator's expectation of
		// how long a restart takes.
		hard := time.AfterFunc(cfg.drainTimeout, grpcSrv.Stop)
		defer hard.Stop()
		grpcSrv.GracefulStop()
		return gctx.Err()
	})
	g.Go(func() error { return serve(srv, ln) }) // returns nil on ErrServerClosed
	g.Go(func() error { return d.Run(gctx) })
	g.Go(func() error { return d.Reap(gctx) })
	g.Go(func() error { // shutdown sequencer. Owner: this group. Stop: gctx. Error: Shutdown's result.
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
