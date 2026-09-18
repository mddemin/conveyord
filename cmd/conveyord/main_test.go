package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/mddemin/conveyor/gen/conveyor/v1"
	"github.com/mddemin/conveyor/internal/httpapi"
	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/pgtest"
	"github.com/mddemin/conveyor/internal/rpc"
	"github.com/mddemin/conveyor/internal/store"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	code := pgtest.Run(m)
	if err := goleak.Find(
		goleak.IgnoreTopFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"), // pgx pool keeps one health-check goroutine per process by design
		// testcontainers' reaper exits with its container, torn down inside pgtest.Run
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

// freshDB returns a Store publishing to events. The hub is a parameter rather
// than an internal detail because the end-to-end tests need the same hub the
// API subscribes to — one hub per process is the wiring main does.
func freshDB(t *testing.T, events store.Publisher) *store.Store {
	t.Helper()
	pool := pgtest.Pool(t)

	ctx := t.Context()
	if err := store.Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_events RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store.New(pool, events)
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// The subset of the wire format this test asserts on. Deliberately a separate
// declaration from httpapi's: sharing the server's own struct would make the
// test agree with the server by construction, including when both are wrong.
type jobResponse struct {
	ID      string          `json:"id"`
	State   string          `json:"state"`
	Payload json.RawMessage `json:"payload"`
}

// agentResponse is the /agents wire form, declared here for the same reason
// jobResponse is: a test that reuses the server's struct agrees with it by
// construction, including when both are wrong.
type agentResponse struct {
	ID           string   `json:"id"`
	Capabilities []string `json:"capabilities"`
}

func fetchAgents(t *testing.T, base string) []agentResponse {
	t.Helper()
	resp, err := http.Get(base + "/agents")
	if err != nil {
		t.Fatalf("GET /agents: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /agents returned %d, want 200", resp.StatusCode)
	}
	var agents []agentResponse
	if err := json.NewDecoder(resp.Body).Decode(&agents); err != nil {
		t.Fatalf("decoding /agents: %v", err)
	}
	return agents
}

func TestHTTPDefaultsToLoopback(t *testing.T) {
	c, err := parseArgs([]string{"-dsn", "postgres://x"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if c.addr != "127.0.0.1:8080" {
		t.Errorf("-addr default is %q, want 127.0.0.1:8080", c.addr)
	}
}

func TestAgentsOmitsClosedStreams(t *testing.T) {
	reg := rpc.NewRegistry()
	reg.Register("dying", nil, make(chan *pb.ServerToAgent, 1), func() {})
	reg.CloseAll()
	if n := len(reg.List()); n != 1 {
		t.Fatalf("List after CloseAll = %d, want 1 (handlers have not Unregistered)", n)
	}
	if got := (agentRegistry{reg}).Agents(); len(got) != 0 {
		t.Fatalf("Agents = %v, want none during shutdown", got)
	}
}

// waitFor polls because registration is asynchronous: the client's Send
// returns before the server's receive pump has run.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEnqueueAndReadBackOverHTTP(t *testing.T) {
	s := freshDB(t, hub.New()) // the real *store.Store
	api := httpapi.New(jobStore{s}, agentRegistry{rpc.NewRegistry()}, hub.New(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ts := httptest.NewServer(api)
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	body := strings.NewReader(`{"type":"resize","payload":{"w":100}}`)
	req, err := http.NewRequestWithContext(ctx, "POST", ts.URL+"/jobs", body)
	if err != nil {
		t.Fatalf("building the POST: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /jobs returned %d, want 201", resp.StatusCode)
	}

	var created jobResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decoding the created job: %v", err)
	}
	if created.State != "pending" {
		t.Errorf("created job is %s, want pending", created.State)
	}

	// Follow Location rather than rebuilding the URL. A 201 promises the new
	// resource is fetchable there; constructing "/jobs/"+id by hand tests the
	// test's idea of the route instead of the server's.
	loc := resp.Header.Get("Location")
	if loc == "" {
		t.Fatal("201 carried no Location header")
	}

	// Read it back through the API, not through the store — the point is the
	// round trip, including the payload surviving as JSON rather than base64.
	req, err = http.NewRequestWithContext(ctx, "GET", ts.URL+loc, nil)
	if err != nil {
		t.Fatalf("building the GET for %q: %v", loc, err)
	}
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", loc, err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d, want 200", loc, resp2.StatusCode)
	}

	var got jobResponse
	if err := json.NewDecoder(resp2.Body).Decode(&got); err != nil {
		t.Fatalf("decoding the fetched job: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("read back id %s, want %s", got.ID, created.ID)
	}
	if string(got.Payload) != `{"w":100}` {
		t.Errorf("payload round-tripped as %s, want the bytes the client sent", got.Payload)
	}
}

// The end of the phase: a job in flight when the signal arrives is not lost.
// In flight now means held by a connected agent — there is no placeholder
// runner any more, so without an agent the job would bounce off ErrNoAgents
// and never be leased for long enough to interrupt.
//
// The agent registers and then says nothing, which is what makes the job stay
// leased: Server.Run blocks on a Result that never comes while the renewal
// pump keeps the lease alive. SIGTERM cancels it, dispatch fails the job while
// it still holds the lease, and the policy hands it back as pending — the
// at-least-once outcome the phase specifies, not a lost job.
func TestSIGTERMDrainsAnInFlightJob(t *testing.T) {
	dsn := pgtest.DSN(t)
	// Order independence: earlier tests' rows must not leak into this test's
	// verdict read, which targets its own job ID but assumes a clean table.
	pool := pgtest.Pool(t)
	if err := store.Migrate(t.Context(), pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(t.Context(), `TRUNCATE jobs, job_events RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	addrc := make(chan [2]net.Addr, 1)
	// Exits when ctx is cancelled below, which is what a signal does to the
	// context main builds.
	go func() {
		errc <- run(ctx, []string{"-addr", "127.0.0.1:0", "-dsn", dsn, "-grpc-addr", "127.0.0.1:0"}, testLogger(t), func() {},
			func(httpAddr, grpcAddr net.Addr) { addrc <- [2]net.Addr{httpAddr, grpcAddr} })
	}()
	addrs := <-addrc
	base := "http://" + addrs[0].String()

	// A real agent over the real listener: register, then go quiet.
	conn, err := grpc.NewClient(addrs[1].String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialling gRPC: %v", err)
	}
	defer conn.Close()
	streamCtx, endStream := context.WithCancel(t.Context())
	defer endStream()
	stream, err := pb.NewConveyorClient(conn).Assign(streamCtx)
	if err != nil {
		t.Fatalf("opening the agent stream: %v", err)
	}
	if err := stream.Send(&pb.AgentToServer{Payload: &pb.AgentToServer_Register{
		Register: &pb.Register{AgentId: "drain-test"}, // no capabilities: takes any type
	}}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// GET /agents is the operator's answer to "is my agent connected?", and
	// the only thing that can be wrong here is the wiring in run: the adapter,
	// the route, and the registry the gRPC server actually writes to. Poll,
	// because Register returns before the server handler has processed it.
	waitFor(t, func() bool { return len(fetchAgents(t, base)) == 1 })
	if got := fetchAgents(t, base)[0].ID; got != "drain-test" {
		t.Errorf("GET /agents reported %q, want the agent that just registered", got)
	}

	resp, err := http.Post(base+"/jobs", "application/json", strings.NewReader(`{"type":"slow"}`))
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	var created jobResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decoding the created job: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /jobs returned %d, want 201", resp.StatusCode)
	}

	// The assignment arriving is the synchronisation point: the agent now holds
	// the job, so the dispatcher is genuinely blocked in Run.
	assigned, err := stream.Recv()
	if err != nil {
		t.Fatalf("waiting for the assignment: %v", err)
	}
	if got := assigned.GetAssignment().GetJobId(); got != created.ID {
		t.Fatalf("assigned %q, want %q", got, created.ID)
	}

	cancel() // the simulated SIGTERM

	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("run returned %v, want nil or context.Canceled", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return within the drain window")
	}

	// The payoff: the server is gone, so the verdict comes from Postgres. The
	// job was recorded and rescheduled, not abandoned at its lease waiting for
	// the reaper.
	st := store.New(pgtest.Pool(t), hub.New())
	got, err := st.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("reading the job back after shutdown: %v", err)
	}
	if got.State != job.Pending || got.Attempt != 1 {
		t.Errorf("after SIGTERM the job is %s attempt %d, want pending attempt 1 (retried, not abandoned)", got.State, got.Attempt)
	}
}
