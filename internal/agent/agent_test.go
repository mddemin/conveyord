package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func testAgent(t *testing.T, execs map[string]Executor) *Agent {
	t.Helper()
	return New(
		Config{Name: "test-agent", Concurrency: 2, Heartbeat: time.Second, Timeout: time.Minute},
		execs,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

// awaitResult reads frames until the Result arrives, skipping the Progress
// frame every started job emits.
func awaitResult(t *testing.T, send <-chan *pb.AgentToServer) *pb.Result {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case m := <-send:
			if r := m.GetResult(); r != nil {
				return r
			}
		case <-deadline:
			t.Fatal("no Result within 2s: the server's waiter would sit until the lease expired")
			return nil
		}
	}
}

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

// The server drops a job's waiter only when a Result arrives, the agent
// disconnects, or the dispatcher gives up — so an assignment this agent cannot
// run must come back as a failure, not as silence. Permanent, because retrying
// does not teach this process a new job type.
func TestAnUnknownJobTypeComesBackAsAPermanentFailure(t *testing.T) {
	a := testAgent(t, map[string]Executor{})
	send := make(chan *pb.AgentToServer, 4)

	a.start(t.Context(), send, &pb.Assignment{JobId: "j-1", Type: "resize", Attempt: 2})

	r := awaitResult(t, send)
	if r.GetJobId() != "j-1" || r.GetAttempt() != 2 {
		t.Errorf("Result is for %s attempt %d, want j-1 attempt 2 — the server keys waiters on both",
			r.GetJobId(), r.GetAttempt())
	}
	if r.GetError() == "" {
		t.Error("Result reports success for a type this agent cannot run")
	}
	if !r.GetPermanent() {
		t.Error("Result is retryable; an unknown type burns MaxAttempts and then dies anyway")
	}
}

// CancelJob is best effort on the wire but not on the bookkeeping: the job
// stops, and it still reports. Abandoning it silently leaves the dispatcher
// waiting on an attempt nobody will ever speak for.
func TestACancelledJobStillReports(t *testing.T) {
	started := make(chan struct{})
	a := testAgent(t, map[string]Executor{"slow": func(ctx context.Context, _ []byte) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}})
	send := make(chan *pb.AgentToServer, 4)

	a.start(t.Context(), send, &pb.Assignment{JobId: "j-1", Type: "slow"})
	<-started
	a.cancel("j-1")

	r := awaitResult(t, send)
	if r.GetError() == "" {
		t.Error("a cancelled job reported success")
	}
	if r.GetPermanent() {
		t.Error("cancellation is not the payload's fault: the job must stay retryable")
	}
}

// The lease outlives nothing else: a wedged executor holds its slot until the
// agent says otherwise, so the timeout is the only thing that ends it — and it
// is permanent, because this agent has already decided the type cannot take
// that long.
func TestAWedgedJobFailsPermanentlyOnTheTimeout(t *testing.T) {
	a := testAgent(t, map[string]Executor{"wedged": func(ctx context.Context, _ []byte) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	a.cfg.Timeout = 20 * time.Millisecond
	send := make(chan *pb.AgentToServer, 4)

	a.start(t.Context(), send, &pb.Assignment{JobId: "j-1", Type: "wedged"})

	r := awaitResult(t, send)
	if !r.GetPermanent() {
		t.Errorf("timed-out job came back retryable (%q); it would wedge again on every attempt", r.GetError())
	}
}

// An executor that panics is a bug, and the same payload panics again on every
// retry — so it is permanent, and above all it does not take the process with
// it. grpc-go does not recover handler panics either.
func TestAPanickingExecutorIsAPermanentFailure(t *testing.T) {
	a := testAgent(t, map[string]Executor{"boom": func(context.Context, []byte) error {
		panic("nil map write")
	}})
	send := make(chan *pb.AgentToServer, 4)

	a.start(t.Context(), send, &pb.Assignment{JobId: "j-1", Type: "boom"})

	r := awaitResult(t, send)
	if !r.GetPermanent() {
		t.Errorf("panicking job came back retryable: %q", r.GetError())
	}
}

// liveAgent runs a real agent against a real rpc.Server over a bufconn: two
// gRPC stacks, one stream, no sockets. It returns the server, which is also
// dispatch's Runner, so a test drives the whole path with Run.
func liveAgent(t *testing.T, execs map[string]Executor, types []string) *rpc.Server {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	t.Cleanup(func() { lis.Close() })

	reg := rpc.NewRegistry()
	srv := rpc.NewServer(nopHub{}, reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	grpcSrv := grpc.NewServer()
	pb.RegisterConveyorServer(grpcSrv, srv)
	go grpcSrv.Serve(lis)
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialling bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	a := New(
		Config{Name: "live-agent", Types: types, Concurrency: 2, Heartbeat: time.Second, Timeout: time.Minute},
		execs,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	go a.Run(t.Context(), conn)

	// Register lands before any assignment can, which is the contract: an
	// unidentified stream is not in the registry and gets no work.
	waitFor(t, func() bool { return len(reg.List()) == 1 })
	return srv
}

// The phase in one test: a job goes down the stream, an executor runs it, and
// the Result comes back as the error dispatch records — here, nil.
func TestAJobRunsEndToEndOverTheStream(t *testing.T) {
	ran := make(chan []byte, 1)
	srv := liveAgent(t, map[string]Executor{"echo": func(_ context.Context, payload []byte) error {
		ran <- payload
		return nil
	}}, []string{"echo"})

	if err := srv.Run(t.Context(), job.Job{ID: "j-1", Type: "echo", Payload: []byte(`{"hi":1}`)}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := string(<-ran); got != `{"hi":1}` {
		t.Errorf("executor got payload %q, want the enqueued bytes", got)
	}
}

// Result.permanent is only useful if it survives the wire as something
// job.IsPermanent recognises, because that is the single bit store.Fail reads
// to bury a job instead of retrying it five times.
func TestAPermanentFailureReachesTheDispatcherAsPermanent(t *testing.T) {
	srv := liveAgent(t, map[string]Executor{"bad": func(context.Context, []byte) error {
		return job.Permanent(errors.New("malformed payload"))
	}}, []string{"bad"})

	err := srv.Run(t.Context(), job.Job{ID: "j-1", Type: "bad"})
	if err == nil {
		t.Fatal("Run reported success for a failed job")
	}
	if !job.IsPermanent(err) {
		t.Errorf("Run returned %v, which the retry policy will retry until MaxAttempts", err)
	}
}

// A capability set is a promise, and the server holds the agent to it: work it
// never announced is never assigned, so the dispatcher sees ErrNoAgents rather
// than a permanent failure from an agent that cannot run it.
func TestWorkOutsideTheAnnouncedTypesIsNotAssigned(t *testing.T) {
	srv := liveAgent(t, map[string]Executor{"echo": func(context.Context, []byte) error { return nil }}, []string{"echo"})

	err := srv.Run(t.Context(), job.Job{ID: "j-1", Type: "resize"})
	if !errors.Is(err, rpc.ErrNoAgents) {
		t.Fatalf("Run returned %v, want ErrNoAgents", err)
	}
	if !errors.Is(err, job.ErrUndelivered) {
		t.Errorf("Run returned %v; nothing took the job, so the attempt must not be spent", err)
	}
}

// nopHub discards progress events. The hub is rpc's business, not this
// package's; these tests assert on what the agent does with jobs.
type nopHub struct{}

func (nopHub) Publish(job.Event) {}
