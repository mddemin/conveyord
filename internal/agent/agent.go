// Package agent executes jobs handed down a conveyord stream and reports what
// happened to each one.
//
// Division of labour, from the phase-9 design: the server owns delivery and
// the lease, this package owns execution. Nothing here decides whether a job
// is retried — it only says what happened, and whether saying it again would
// change anything (Result.permanent).
//
// The caller supplies the connection and the executor catalogue; this package
// supplies the stream discipline. That split is what lets a test drive a real
// agent over a bufconn without a process, a socket, or a flag.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/obs"
	"google.golang.org/grpc"
)

// Executor runs one job. ctx carries both bounds that can stop it: the agent's
// own timeout and a CancelJob from the server. An executor that ignores ctx
// cannot be interrupted, only outlived.
type Executor func(ctx context.Context, payload []byte) error

// Config is what one agent process needs to know about itself.
type Config struct {
	// Name is the agent_id announced at Register. It must be unique: a second
	// stream claiming it displaces the first.
	Name string
	// Types are the job types this agent accepts. Empty means any, which is
	// rarely what you want — see cmd/agent, where it defaults to the executor
	// map's keys.
	Types []string
	// Concurrency bounds jobs running at once.
	Concurrency int
	// Heartbeat is how often an idle stream proves it is alive.
	Heartbeat time.Duration
	// Timeout bounds one job. Exceeding it is a permanent failure: the agent
	// is the only party that knows how long its own work should take.
	Timeout time.Duration
}

// jobKey identifies one attempt, matching the server's waiter key. The agent
// needs the attempt number to echo back on the Result; without it the server
// drops the frame as belonging to an attempt nobody is waiting for.
type jobKey struct {
	id      string
	attempt int32
}

// Agent runs one stream's worth of work. Safe for concurrent use: every job
// runs on its own goroutine, and the only shared state is the running map.
type Agent struct {
	cfg   Config
	execs map[string]Executor
	log   *slog.Logger
	sem   chan struct{} // one slot per concurrent job

	mu      sync.Mutex
	running map[jobKey]context.CancelFunc
}

// New returns an agent ready to Run. execs is the whole catalogue of work this
// process can do; an assignment for anything else comes back as a permanent
// failure.
func New(cfg Config, execs map[string]Executor, log *slog.Logger) *Agent {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1 // unbuffered sem would deadlock the first job
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = time.Second // NewTicker panics on 0
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = time.Minute
	}
	return &Agent{
		cfg:     cfg,
		execs:   execs,
		log:     log,
		sem:     make(chan struct{}, cfg.Concurrency),
		running: make(map[jobKey]context.CancelFunc),
	}
}

// Run keeps exactly one stream alive on conn until ctx dies, reconnecting with
// the same backoff the queue uses for jobs. A daemon restart, a network blip
// and the server's keepalive giving up on a black-holed connection all land
// here and must not need an operator.
//
// It returns nil on a clean shutdown: the stream ending is the point.
func (a *Agent) Run(ctx context.Context, conn grpc.ClientConnInterface) error {
	client := pb.NewConveyorClient(conn)
	policy := job.Policy{Base: time.Second, Max: 30 * time.Second}

	for attempt := 1; ; attempt++ {
		started := time.Now()
		err := a.session(ctx, client)
		if ctx.Err() != nil {
			return nil
		}
		// A stream that lived a while was healthy, so the next blip starts
		// from zero rather than from whatever the last outage climbed to.
		if time.Since(started) > time.Minute {
			attempt = 1
		}
		delay := policy.Backoff(attempt)
		a.log.Warn("stream lost, reconnecting", "err", err, "in", delay)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil
		}
	}
}

// session runs one stream: register, then pump until either direction fails.
//
// The stream's context is created here, so cancelling it really does unblock
// Recv — unlike the server side, where Recv waits on a transport context no
// handler owns. The deferred cancel is therefore what stops the other pump
// once the first one returns.
func (a *Agent) session(ctx context.Context, client pb.ConveyorClient) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.Assign(ctx)
	if err != nil {
		return fmt.Errorf("opening the stream: %w", err)
	}

	// Register first, before any other frame and before the send pump exists:
	// the server assigns nothing to a stream that has not said who it is, and
	// this is the one moment where a second writer cannot exist.
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Register{
		Register: &pb.Register{AgentId: a.cfg.Name, Capabilities: a.cfg.Types},
	}}); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	a.log.Info("registered", "agent", a.cfg.Name, "types", a.cfg.Types)

	// Buffered so a job finishing never blocks on the network: results are the
	// one frame the server is actually waiting for.
	send := make(chan *pb.AgentToServer, 64)

	errc := make(chan error, 2)
	go func() { errc <- a.sendPump(ctx, stream, send) }()
	go func() { errc <- a.recvPump(ctx, stream, send) }()
	return <-errc // the deferred cancel ends the stream, which frees the other pump
}

// sendPump is the stream's only writer, which is what makes heartbeats,
// progress and results from N job goroutines safe on one gRPC stream.
func (a *Agent) sendPump(ctx context.Context, stream pb.Conveyor_AssignClient, send <-chan *pb.AgentToServer) error {
	tk := time.NewTicker(a.cfg.Heartbeat)
	defer tk.Stop()

	beat := &pb.AgentToServer{Payload: &pb.AgentToServer_Heartbeat{Heartbeat: &pb.Heartbeat{}}}
	for {
		var m *pb.AgentToServer
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tk.C:
			m = beat
		case m = <-send:
		}
		if err := stream.Send(m); err != nil {
			return fmt.Errorf("send: %w", err)
		}
	}
}

// recvPump is the stream's only reader. It never runs a job itself: an
// executor blocking here would stop cancellations and every other assignment
// on the same stream.
func (a *Agent) recvPump(ctx context.Context, stream pb.Conveyor_AssignClient, send chan<- *pb.AgentToServer) error {
	for {
		m, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		switch p := m.Payload.(type) {
		case *pb.ServerToAgent_Assignment:
			a.start(ctx, send, p.Assignment)
		case *pb.ServerToAgent_CancelJob:
			a.cancel(p.CancelJob.GetJobId())
		default:
			// A frame from a newer server. Forward compatibility is the point
			// of the oneof; dying here would make every schema addition a
			// fleet-wide outage.
			a.log.Debug("unknown frame ignored", "payload", fmt.Sprintf("%T", m.Payload))
		}
	}
}

// start takes one assignment and guarantees exactly one Result for it. Silence
// is the one thing it may never do: the server's waiter would sit until the
// lease expired, holding a MaxInFlight slot for the whole time.
func (a *Agent) start(ctx context.Context, send chan<- *pb.AgentToServer, asg *pb.Assignment) {
	exec, ok := a.execs[asg.GetType()]
	if !ok {
		// Permanent: retrying does not teach this agent a new type. Something
		// is enqueuing work nothing can run, and burying it is how that shows
		// up as a dead job instead of a queue that quietly churns.
		a.report(ctx, send, asg, job.Permanent(fmt.Errorf("no executor for type %q", asg.GetType())))
		return
	}

	// ponytail: one timeout for every type. Per-type limits go in a
	// map[string]time.Duration beside execs the day two types disagree.
	ctx, span := obs.Start(obs.Continue(ctx, asg.GetTraceparent()), "agent.execute", obs.JobAttrs(asg.GetJobId(), asg.GetType())...)
	jctx, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	key := jobKey{id: asg.GetJobId(), attempt: asg.GetAttempt()}

	a.mu.Lock()
	a.running[key] = cancel
	a.mu.Unlock()

	go func() {
		defer span.End()
		defer cancel()
		defer func() {
			a.mu.Lock()
			delete(a.running, key)
			a.mu.Unlock()
		}()

		select {
		case a.sem <- struct{}{}:
		case <-jctx.Done():
			// Queued behind other work for the whole budget. Retryable: the
			// job never ran, and the next attempt may find an idle slot.
			a.report(ctx, send, asg, fmt.Errorf("never started: %w", jctx.Err()))
			return
		}
		defer func() { <-a.sem }()

		a.emit(ctx, send, &pb.AgentToServer{Payload: &pb.AgentToServer_Progress{
			Progress: &pb.Progress{JobId: asg.GetJobId(), Message: "started on " + a.cfg.Name},
		}})

		err := safeRun(jctx, exec, asg.GetPayload())
		if errors.Is(jctx.Err(), context.DeadlineExceeded) {
			// The agent is the only party that knows how long this type should
			// take, so a job that blew the budget is this agent's verdict, not
			// a transient fault to retry until MaxAttempts. Deliberately keyed
			// on jctx and not on err: an executor's own inner deadline is its
			// business, and burying that job would be wrong.
			err = job.Permanent(fmt.Errorf("exceeded the %s budget for %q", a.cfg.Timeout, asg.GetType()))
		}
		a.report(ctx, send, asg, err)
	}()
}

// safeRun turns a panicking executor into a permanent failure. A panic is a
// bug in the executor or garbage in the payload; either way the same payload
// panics again on every retry, and one bad job must not take the agent down.
func safeRun(ctx context.Context, exec Executor, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = job.Permanent(fmt.Errorf("executor panicked: %v", r))
		}
	}()
	return exec(ctx, payload)
}

// cancel stops every attempt of jobID running here. CancelJob carries no
// attempt number, and the dispatcher only sends one when it has abandoned the
// job outright, so cancelling all of them is the intent. Each cancelled job
// still reports: abandoning it silently leaves a waiter with nothing to wake.
func (a *Agent) cancel(jobID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for k, cancel := range a.running {
		if k.id == jobID {
			a.log.Info("cancelling", "job", jobID, "attempt", k.attempt)
			cancel()
		}
	}
}

// report sends the one Result an assignment gets. Note the context: it is the
// stream's, never the job's, so a cancelled or timed-out job still reports.
func (a *Agent) report(ctx context.Context, send chan<- *pb.AgentToServer, asg *pb.Assignment, err error) {
	r := &pb.Result{JobId: asg.GetJobId(), Attempt: asg.GetAttempt()}
	if err != nil {
		r.Error = err.Error()
		r.Permanent = job.IsPermanent(err)
	}
	a.log.Info("reporting", "job", r.JobId, "attempt", r.Attempt, "err", err, "permanent", r.Permanent)
	a.emit(ctx, send, &pb.AgentToServer{Payload: &pb.AgentToServer_Result{Result: r}})
}

// emit queues a frame, or drops it if the stream is already gone — in which
// case the server has seen ErrAgentGone and rescheduled the job, so there is
// nothing left to tell it.
func (a *Agent) emit(ctx context.Context, send chan<- *pb.AgentToServer, m *pb.AgentToServer) {
	select {
	case send <- m:
	case <-ctx.Done():
		a.log.Debug("frame dropped, stream is gone", "payload", fmt.Sprintf("%T", m.Payload))
	}
}
