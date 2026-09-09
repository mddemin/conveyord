package rpc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"github.com/mddemin/conveyor/internal/job"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// waitFor is a third copy of dispatch_test.go:21, not a reuse: it is a
// package-private test helper, so package rpc cannot see it. internal/httpapi
// already carries the second copy (event_server_test.go:22) for the same
// reason. Twelve duplicated lines beat exporting a test helper from a package
// that has no other reason to have an exported testing surface.
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

// fakeHub records published events. Locked because recvPump publishes from the
// stream goroutine while the test goroutine reads.
type fakeHub struct {
	mu     sync.Mutex
	events []job.Event
}

func newFakeHub() *fakeHub { return &fakeHub{} }

func (h *fakeHub) Publish(ev job.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
}

func (h *fakeHub) published() []job.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]job.Event(nil), h.events...)
}

// dialBufconn connects to an in-process server. passthrough:// keeps the
// resolver out of it; the dialer ignores the address entirely.
func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestStreamReceivesAssignment(t *testing.T) {
	ctx := t.Context()
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { lis.Close() })
	reg := NewRegistry()
	srv := grpc.NewServer()
	pb.RegisterConveyorServer(srv, NewServer(newFakeHub(), reg, slog.Default()))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn := dialBufconn(t, lis)
	client := pb.NewConveyorClient(conn)
	stream, err := client.Assign(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Identify first: no Register, no registry entry, no assignment.
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Register{
		Register: &pb.Register{AgentId: "a-1", Capabilities: []string{"resize"}},
	}}); err != nil {
		t.Fatal(err)
	}
	// client.Assign returns before the server handler runs; without this wait the
	// Assign below races the registration and returns ErrNoAgents.
	waitFor(t, func() bool { return len(reg.List()) == 1 })

	if _, err := reg.Assign(job.Job{ID: "j-1", Type: "resize"}); err != nil {
		t.Fatal(err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg.GetAssignment().GetJobId() != "j-1" {
		t.Fatalf("want j-1, got %v", msg.GetAssignment())
	}
}

func TestProgressIsPublished(t *testing.T) {
	ctx := t.Context()
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { lis.Close() })
	hub := newFakeHub()
	reg := NewRegistry()
	srv := grpc.NewServer()
	pb.RegisterConveyorServer(srv, NewServer(hub, reg, slog.Default()))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn := dialBufconn(t, lis)
	client := pb.NewConveyorClient(conn)
	stream, err := client.Assign(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Progress{
		Progress: &pb.Progress{JobId: "j-1", Message: "halfway"},
	}}); err != nil {
		t.Fatal(err)
	}

	// Progress needs no Register: it is telemetry, not work, so an agent that has
	// not identified itself can still narrate. SSE readers get it through the
	// same hub the store publishes to.
	waitFor(t, func() bool { return len(hub.published()) == 1 })
	ev := hub.published()[0]
	if ev.JobID != "j-1" || ev.Message != "halfway" || ev.State != job.Leased {
		t.Fatalf("published %+v", ev)
	}
	if ev.CreatedAt.IsZero() {
		t.Fatal("stream-published events must carry a timestamp; no row supplies one")
	}
}

func TestPreRegisterFramesAreSafe(t *testing.T) {
	ctx := t.Context()
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { lis.Close() })
	reg := NewRegistry()
	srv := grpc.NewServer()
	pb.RegisterConveyorServer(srv, NewServer(newFakeHub(), reg, slog.Default()))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn := dialBufconn(t, lis)
	client := pb.NewConveyorClient(conn)
	stream, err := client.Assign(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Everything an agent can say before it says who it is. A log line that
	// dereferences the agent pointer in any of these branches panics here, and
	// grpc-go does not recover handler panics — the test binary dies with it.
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Heartbeat{
		Heartbeat: &pb.Heartbeat{},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Result{
		Result: &pb.Result{JobId: "nobody-waiting"},
	}}); err != nil {
		t.Fatal(err)
	}
	// An unset oneof: GetPayload() is a nil interface, so this lands in the same
	// `case nil` a future variant would. The two are indistinguishable at this
	// seam, which is why one branch handles both — and it is the branch whose
	// log line named the agent, so it is the likeliest nil deref of the three.
	if err := stream.Send(&pb.AgentToServer{}); err != nil {
		t.Fatal(err)
	}

	// The stream must still be alive and still assignable afterwards.
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Register{
		Register: &pb.Register{AgentId: "a-1"},
	}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(reg.List()) == 1 })
}

func TestReRegisterKeepsTheStreamAndTheJob(t *testing.T) {
	ctx := t.Context()
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { lis.Close() })
	reg := NewRegistry()
	s := NewServer(newFakeHub(), reg, slog.Default())
	srv := grpc.NewServer()
	pb.RegisterConveyorServer(srv, s)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn := dialBufconn(t, lis)
	client := pb.NewConveyorClient(conn)
	stream, err := client.Assign(ctx)
	if err != nil {
		t.Fatal(err)
	}

	register := func(caps ...string) {
		if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Register{
			Register: &pb.Register{AgentId: "a-1", Capabilities: caps},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	register("resize")
	waitFor(t, func() bool { return len(reg.List()) == 1 })

	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx, job.Job{ID: "j-1", Type: "resize"}) }()
	if got, err := stream.Recv(); err != nil || got.GetAssignment().GetJobId() != "j-1" {
		t.Fatalf("assignment: %v %v", got, err)
	}

	// The agent learns a new type while it is still working on j-1. This must not
	// cancel the stream (a second Register would pass this stream's own cancel as
	// the displaced agent's) and must not fail j-1 with ErrAgentGone.
	register("resize", "transcode")
	// Capabilities are unexported, so assignability is the observable. Not a pure
	// predicate: the attempt that finally succeeds leaves one j-2 frame queued for
	// an agent that never reads it, which is harmless and teardown discards it.
	waitFor(t, func() bool { _, err := reg.Assign(job.Job{ID: "j-2", Type: "transcode"}); return err == nil })

	select {
	case err := <-errc:
		t.Fatalf("j-1 died on a capability update: %v", err)
	default:
	}
	s.deliver(&pb.Result{JobId: "j-1"})
	if err := <-errc; err != nil {
		t.Fatalf("want success, got %v", err)
	}
	if n := len(reg.List()); n != 1 {
		t.Fatalf("re-Register changed the registry: %d agents", n)
	}
}

func TestRunBlocksUntilResult(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	send := make(chan *pb.ServerToAgent, 1)
	s.registry.Register("a-1", nil, send, func() {})

	errc := make(chan error, 1)
	go func() { errc <- s.Run(t.Context(), job.Job{ID: "j-1", Type: "x"}) }()
	if got := (<-send).GetAssignment().GetJobId(); got != "j-1" {
		t.Fatalf("want j-1, got %q", got)
	}

	// A Run that returned at assignment time reports nil here, and dispatch would
	// have written Succeeded for a job the agent has not finished.
	s.deliver(&pb.Result{JobId: "j-1", Error: "boom"})
	err := <-errc
	if err == nil || err.Error() != "boom" {
		t.Fatalf("want the agent's error, got %v", err)
	}
}

// Result.permanent is the agent's only way to say "the payload is wrong, not
// the run" — without it Policy.OnFailure retries a malformed job until
// MaxAttempts. The error Run returns is the only thing dispatch sees, so the
// flag has to survive the trip as something job.IsPermanent recognises.
func TestPermanentResultSurvivesTheWire(t *testing.T) {
	for _, tc := range []struct {
		name      string
		errText   string
		permanent bool
		want      bool // job.IsPermanent of the error Run returns
	}{
		{name: "permanent", errText: "malformed payload", permanent: true, want: true},
		{name: "retryable", errText: "malformed payload", permanent: false, want: false},
		// Success is never permanent. Wrapping before the empty-error check
		// would hand dispatch a permanent failure for a job that worked.
		{name: "success with the flag set", errText: "", permanent: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
			send := make(chan *pb.ServerToAgent, 1)
			s.registry.Register("a-1", nil, send, func() {})

			errc := make(chan error, 1)
			go func() { errc <- s.Run(t.Context(), job.Job{ID: "j-1", Type: "x"}) }()
			<-send

			s.deliver(&pb.Result{JobId: "j-1", Error: tc.errText, Permanent: tc.permanent})
			err := <-errc
			switch {
			case tc.errText == "" && err != nil:
				t.Fatalf("empty error is success, got %v", err)
			case tc.errText != "" && (err == nil || err.Error() != tc.errText):
				t.Fatalf("want the agent's error verbatim, got %v", err)
			}
			if got := job.IsPermanent(err); got != tc.want {
				t.Fatalf("IsPermanent = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStaleResultFromAbandonedAttemptIsIgnored(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	send := make(chan *pb.ServerToAgent, 1)
	s.registry.Register("a-1", nil, send, func() {})

	// Attempt 1 is abandoned the way runOne abandons it: ctx dies (ErrLeaseLost)
	// while the agent is still working, so no Result ever arrives for it.
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx, job.Job{ID: "j-1", Type: "x", Attempt: 0}) }()
	if got := (<-send).GetAssignment().GetAttempt(); got != 0 {
		t.Fatalf("attempt 0 not stamped: %d", got)
	}
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("attempt 1: %v", err)
	}

	// The store incremented attempt on Fail (store.go:378), so the retry is
	// attempt 1 and lands on a fresh waiter.
	errc2 := make(chan error, 1)
	go func() { errc2 <- s.Run(t.Context(), job.Job{ID: "j-1", Type: "x", Attempt: 1}) }()
	if got := (<-send).GetAssignment().GetAttempt(); got != 1 {
		t.Fatalf("attempt 1 not stamped: %d", got)
	}

	// Now attempt 0's agent finally reports failure. Keyed by job ID alone this
	// fails attempt 1; keyed by attempt it is dropped with a warning.
	s.deliver(&pb.Result{JobId: "j-1", Error: "boom", Attempt: 0})
	select {
	case err := <-errc2:
		t.Fatalf("stale attempt poisoned the retry: %v", err)
	default:
	}
	s.deliver(&pb.Result{JobId: "j-1", Attempt: 1})
	if err := <-errc2; err != nil {
		t.Fatalf("want success, got %v", err)
	}
}

func TestRunReturnsOnCancel(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	send := make(chan *pb.ServerToAgent, 1)
	s.registry.Register("a-1", nil, send, func() {})
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx, job.Job{ID: "j-1", Type: "x"}) }()
	<-send
	cancel() // lease lost or SIGTERM: dispatch must not wait on a dead agent forever
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestRunReturnsWhenAgentDisconnects(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	send := make(chan *pb.ServerToAgent, 1)
	a := s.registry.Register("a-1", nil, send, func() {})

	errc := make(chan error, 1)
	go func() { errc <- s.Run(t.Context(), job.Job{ID: "j-1", Type: "x"}) }()
	<-send

	// The agent crashes: no Result, ever. Nothing else can wake this Run — the
	// lease keeps being renewed under cfg.Name, so the reaper never fires and the
	// MaxInFlight slot would be lost for the life of the process.
	s.registry.Unregister(a)
	if err := <-errc; !errors.Is(err, ErrAgentGone) {
		t.Fatalf("want ErrAgentGone, got %v", err)
	}
}

// Dispatch cannot import this package, so the only thing it can read is the
// error chain. ErrNoAgents and ErrSlowAgent mean the assignment never reached
// an executor: nothing ran, nothing failed, and the attempt must not be spent.
// job.ErrUndelivered is the shared vocabulary that says so.
func TestUndeliveredAssignmentsSaySo(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(s *Server)
		want  error
	}{
		{
			name:  "nothing connected runs this type",
			setup: func(*Server) {},
			want:  ErrNoAgents,
		},
		{
			name: "every capable agent is still holding its last frame",
			setup: func(s *Server) {
				send := make(chan *pb.ServerToAgent, 1)
				s.registry.Register("a-1", nil, send, func() {})
				send <- &pb.ServerToAgent{} // one slot, undrained: Assign passes it over
			},
			want: ErrSlowAgent,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
			tc.setup(s)

			err := s.Run(t.Context(), job.Job{ID: "j-1", Type: "x"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Run returned %v, want %v", err, tc.want)
			}
			if !errors.Is(err, job.ErrUndelivered) {
				t.Fatalf("Run returned %v: dispatch cannot tell it from a real failure, so it spends an attempt", err)
			}
		})
	}
}

// ErrAgentGone is deliberately not undelivered: the agent had the frame and
// may have run the job, so the attempt is spent and the policy decides. Wrap
// this one too and a job that kills its executor retries forever.
func TestAgentGoneIsDeliveredWork(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	send := make(chan *pb.ServerToAgent, 1)
	a := s.registry.Register("a-1", nil, send, func() {})

	errc := make(chan error, 1)
	go func() { errc <- s.Run(t.Context(), job.Job{ID: "j-1", Type: "x"}) }()
	<-send
	s.registry.Unregister(a)

	err := <-errc
	if !errors.Is(err, ErrAgentGone) {
		t.Fatalf("want ErrAgentGone, got %v", err)
	}
	if errors.Is(err, job.ErrUndelivered) {
		t.Fatalf("the agent took the job; %v must still cost an attempt", err)
	}
}

func TestResultBeatsDisconnect(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	send := make(chan *pb.ServerToAgent, 1)
	a := s.registry.Register("a-1", nil, send, func() {})

	errc := make(chan error, 1)
	go func() { errc <- s.Run(t.Context(), job.Job{ID: "j-1", Type: "x"}) }()
	<-send

	// Report, then hang up — the ordinary shape of an agent finishing its last
	// job and exiting. Both select cases are ready; the Result must win, or a
	// finished job gets retried on every clean agent shutdown.
	s.deliver(&pb.Result{JobId: "j-1"})
	s.registry.Unregister(a)
	if err := <-errc; err != nil {
		t.Fatalf("want success, got %v", err)
	}
}

func TestRunReportsNoAgents(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	err := s.Run(t.Context(), job.Job{ID: "j-1", Type: "x"})
	if !errors.Is(err, ErrNoAgents) {
		t.Fatalf("want ErrNoAgents, got %v", err)
	}
}

func TestResultForReapedJobIsDropped(t *testing.T) {
	s := NewServer(newFakeHub(), NewRegistry(), slog.Default())
	s.deliver(&pb.Result{JobId: "gone"}) // nobody waiting: must not panic or block
}

func TestDisconnectUnregisters(t *testing.T) {
	reg := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { lis.Close() })
	srv := grpc.NewServer()
	pb.RegisterConveyorServer(srv, NewServer(newFakeHub(), reg, slog.Default()))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn := dialBufconn(t, lis)
	client := pb.NewConveyorClient(conn)
	stream, err := client.Assign(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Register{
		Register: &pb.Register{AgentId: "a-1"},
	}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(reg.List()) == 1 })
	cancel()
	waitFor(t, func() bool { return len(reg.List()) == 0 })
}

// TestCloseAllUnwindsIdleStream is the shutdown path: CloseAll cancels every
// agent's context and grpc.Server.GracefulStop then waits for the handlers. An
// agent that is connected but silent sends nothing, so if Assign only returns
// once its receive pump does, shutdown hangs until the agent happens to speak.
// Unregistering is the observable: the handler cannot have returned without it.
func TestCloseAllUnwindsIdleStream(t *testing.T) {
	reg := NewRegistry()
	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { lis.Close() })
	srv := grpc.NewServer()
	pb.RegisterConveyorServer(srv, NewServer(newFakeHub(), reg, slog.Default()))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	stream, err := pb.NewConveyorClient(dialBufconn(t, lis)).Assign(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Register{
		Register: &pb.Register{AgentId: "a-1"},
	}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(reg.List()) == 1 })

	reg.CloseAll() // the agent stays silent from here on
	waitFor(t, func() bool { return len(reg.List()) == 0 })
}
