package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"github.com/mddemin/conveyor/internal/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is the gRPC face of the dispatcher: it turns a leased job into an
// assignment on some agent's stream, and that agent's result back into the
// error dispatch.runOne records.
//
// It owns no storage. Dispatch holds the lease and writes every outcome with
// its own leased_by fencing; this type only reports what the agent said. Its
// whole mutable state is waiters — one entry per in-flight attempt, holding
// the channel Run is blocked on and deliver hands the outcome to.
type Server struct {
	pb.UnimplementedConveyorServer
	hub      Publisher
	registry *Registry
	log      *slog.Logger

	mu      sync.Mutex
	waiters map[waiterKey]chan error
}

// waiterKey identifies one attempt, not one job, because two attempts of the
// same job can be outstanding at once: runOne abandons an attempt when its
// lease is lost while that agent keeps working, and phase 9 has no CancelJob
// to stop it. Keyed by job alone, the abandoned attempt's late failure would
// land on the retry's waiter and bury a job another agent is completing.
type waiterKey struct {
	jobID   string
	attempt int32
}

// Publisher is the slice of the event hub this package needs: progress frames
// from agents become job events for SSE subscribers. Same interface store.New
// takes.
type Publisher interface{ Publish(ev job.Event) }

// NewServer returns a Server ready to serve agent streams and to be handed to
// dispatch as its Runner.
func NewServer(hub Publisher, reg *Registry, log *slog.Logger) *Server {
	return &Server{hub: hub, registry: reg, log: log, waiters: make(map[waiterKey]chan error)}
}

// sendPump is a stream's only writer. It owns the channel that Register gave
// the registry, which is what keeps Registry.Assign off the stream entirely:
// assignment is a non-blocking send into a one-slot buffer, so a slow agent is
// passed over instead of stalling the map walk under the registry mutex.
//
// A failed Send is the agent leaving, not a transient fault, so it is returned
// rather than retried. Returning ctx.Err() on cancellation is what turns
// shutdown, displacement and CloseAll into a handler that exits.
func sendPump(ctx context.Context, stream pb.Conveyor_AssignServer, send chan *pb.ServerToAgent) error {
	for {
		select {
		case m := <-send:
			if err := stream.Send(m); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// recvPump is a stream's only reader, and every agent-driven state change
// happens here: identity, capabilities, progress, results. It runs until Recv
// fails — the agent hangs up, the connection dies, or Assign returns and grpc
// tears the stream down.
//
// It takes no context on purpose. Recv wakes on an incoming frame or on the
// transport's own stream context, neither of which this package owns, so a
// context parameter here would advertise a waker that does not exist.
//
// Unregistering belongs to this pump because it is the only code that knows
// whether registration ever happened: an agent that never sent Register was
// never in the registry. A defer also keeps that correct when Assign returns
// first, which is exactly what shutdown does.
func (s *Server) recvPump(stream pb.Conveyor_AssignServer, send chan *pb.ServerToAgent, cancel context.CancelFunc) error {
	var agent *Agent
	defer func() {
		if agent != nil {
			s.registry.Unregister(agent)
		}
	}()

	for {
		m, err := stream.Recv()
		if err != nil {
			return err
		}
		// agent is nil until the first Register lands, and heartbeats,
		// progress and unknown variants can all arrive before it does. No
		// branch below may dereference it unguarded: grpc-go does not recover
		// handler panics, so a nil deref here takes the daemon with it.
		switch p := m.Payload.(type) {
		case *pb.AgentToServer_Heartbeat:
			// Liveness only. The lease is the real clock, and Agent.Done is
			// what actually releases a waiting Run.
			s.log.Debug("heartbeat")
		case *pb.AgentToServer_Progress:
			// No row behind this event, so it needs its own timestamp; store
			// events get theirs from Postgres.
			pr := p.Progress
			s.hub.Publish(job.Event{JobID: pr.JobId, Message: pr.Message, State: job.Leased, CreatedAt: time.Now()})
		case *pb.AgentToServer_Result:
			s.deliver(p.Result)
		case *pb.AgentToServer_Register:
			// Identity is first-Register-wins; capabilities are mutable.
			// Registering twice from one stream would hand the registry this
			// stream's own cancel as the displaced agent's, so the agent would
			// disconnect itself and close a done channel that in-flight Runs
			// are selecting on — healthy jobs failed with ErrAgentGone because
			// an agent announced it had learned a new type.
			r := p.Register
			switch {
			case agent == nil:
				agent = s.registry.Register(r.AgentId, r.Capabilities, send, cancel)
			case r.AgentId == agent.ID:
				s.registry.SetCapabilities(agent, r.Capabilities)
			default:
				s.log.Warn("agent id changed mid-stream, ignoring", "have", agent.ID, "got", r.AgentId)
			}
		default:
			// A variant from a newer agent, or an empty frame. Forward
			// compatibility means continuing, not ending the stream.
			s.log.Debug("unknown payload variant")
		}
	}
}

// Assign is the gRPC handler: one call per connected agent, and two pumps,
// because a single goroutine cannot both Recv and Send without deadlocking
// against an agent that is waiting to be given work.
//
// It returns as soon as either pump finishes and deliberately does not wait
// for the other. Cancelling this handler's context does not unblock Recv —
// that waits on the transport's stream context, a parent this code cannot
// cancel — so waiting for the receive pump would keep a displaced or
// shut-down agent's handler, registry entry and GracefulStop alive until the
// agent happened to send a frame. Returning ends the RPC, and ending the RPC
// is what makes the abandoned Recv return. TestCloseAllUnwindsIdleStream is
// the guard; both channels are buffered so neither pump can block on exit.
//
// A client that merely went away is not a server failure: io.EOF for a clean
// half-close, codes.Canceled for a client that cancelled instead, and
// context.Canceled for our own cancel travelling out through the send pump.
func (s *Server) Assign(stream pb.Conveyor_AssignServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	send := make(chan *pb.ServerToAgent, 1)

	// cancel goes to the registry via Register: it is how displacement and
	// CloseAll stop an agent that is no longer meant to be served.
	recvErr := make(chan error, 1)
	go func() { recvErr <- s.recvPump(stream, send, cancel) }()

	sendErr := make(chan error, 1)
	go func() { sendErr <- sendPump(ctx, stream, send) }()

	var err error
	select {
	case err = <-recvErr:
	case err = <-sendErr:
	}

	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return nil
	}
	return err
}

// Run satisfies dispatch.Runner. It hands j to a capable agent and blocks
// until that agent reports, that agent disappears, or ctx dies.
//
// Blocking is the contract, not an implementation detail: runOne records the
// outcome the instant Run returns, so a Run that returned at assignment time
// would write Succeeded before the agent had read the frame, and the real
// result would then miss the store's state = 'leased' guard and be discarded.
//
// Waiting on the agent as well as on the job is equally load-bearing. Nothing
// else can wake a job whose agent died: the lease is renewed on a ticker under
// the dispatcher's own name, with no knowledge of agent liveness, so the
// reaper never sees the job and the MaxInFlight slot would be lost for the
// life of the process.
func (s *Server) Run(ctx context.Context, j job.Job) error {
	key := waiterKey{jobID: j.ID, attempt: int32(j.Attempt)}
	result := make(chan error, 1)

	// Register the waiter before assigning: Assign hands the frame to the send
	// pump through a buffered channel, so the agent can answer while Assign is
	// still returning.
	s.mu.Lock()
	s.waiters[key] = result
	s.mu.Unlock()

	// And drop it on the way out, so a result for an attempt nobody is waiting
	// for is discarded by deliver instead of leaking an entry per attempt.
	defer func() {
		s.mu.Lock()
		delete(s.waiters, key)
		s.mu.Unlock()
	}()

	agent, err := s.registry.Assign(j)
	if err != nil {
		// Two verbs, two chains: the operator-facing reason (ErrNoAgents or
		// ErrSlowAgent, which say whether to deploy an agent or add one) and
		// the fact dispatch acts on — nothing took this frame, so the attempt
		// was not spent. ErrAgentGone never comes through here: that agent had
		// the job.
		return fmt.Errorf("assign %s: %w: %w", j.ID, job.ErrUndelivered, err)
	}

	select {
	case err := <-result:
		return err
	case <-agent.Done():
		// An agent that reports and then hangs up leaves both cases ready, and
		// select picks at random, so drain the buffered result first or every
		// clean agent shutdown retries a job that is already finished. Sound
		// rather than hopeful: deliver runs in recvPump strictly before the
		// registry closes done.
		select {
		case err := <-result:
			return err
		default:
			return fmt.Errorf("job %s attempt %d: agent %s: %w", j.ID, j.Attempt, agent.ID, ErrAgentGone)
		}
	case <-ctx.Done():
		// Shutdown, or the lease was lost and dispatch has abandoned this
		// attempt. The agent may still be working on it; the attempt key is
		// what keeps its late result off the retry's waiter.
		return ctx.Err()
	}
}

// deliver routes one result frame to whoever is waiting for that attempt. It
// never blocks and never fails, because it runs in the receive pump: stalling
// here would stall every other job on the same stream.
//
// Both drop paths are logged and normal. No waiter means the attempt was
// reaped or abandoned and has already been retried; a full buffer means a
// duplicate result, which at-least-once delivery makes routine. An empty error
// string is success — phase 9 has no way to say "permanent", so every failure
// a result carries is retryable.
func (s *Server) deliver(r *pb.Result) {
	key := waiterKey{jobID: r.JobId, attempt: r.Attempt}

	s.mu.Lock()
	defer s.mu.Unlock()

	w, ok := s.waiters[key]
	if !ok {
		s.log.Warn("result for unknown attempt dropped", "job", r.JobId, "attempt", r.Attempt)
		return
	}

	var err error
	if r.Error != "" {
		err = errors.New(r.Error)

		if r.Permanent {
			err = job.Permanent(err)
		}
	}
	select {
	case w <- err:
	default:
		s.log.Warn("duplicate result dropped", "job", r.JobId, "attempt", r.Attempt)
	}
}
