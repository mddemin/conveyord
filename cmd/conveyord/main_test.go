package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/httpapi"
	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/pgtest"
	"github.com/mddemin/conveyor/internal/store"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

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

func TestEnqueueAndReadBackOverHTTP(t *testing.T) {
	s := freshDB(t, hub.New()) // the real *store.Store
	api := httpapi.New(jobStore{s}, hub.New(), slog.New(slog.NewJSONHandler(io.Discard, nil)))
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

// The end of the phase: a job running when the signal arrives is not lost.
// The placeholder runner holds each job ~250ms, so the cancel below lands
// while it is genuinely mid-flight.
func TestSIGTERMDrainsAnInFlightJob(t *testing.T) {
	dsn := pgtest.DSN(t)

	ctx, cancel := context.WithCancel(t.Context())
	errc := make(chan error, 1)
	addrc := make(chan net.Addr, 1)
	// Exits when ctx is cancelled below, which is what a signal does to the
	// context main builds.
	go func() {
		errc <- run(ctx, []string{"-addr", "127.0.0.1:0", "-dsn", dsn}, testLogger(t), func() {},
			func(a net.Addr) { addrc <- a })
	}()
	addr := <-addrc
	base := "http://" + addr.String()

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

	// Bounded poll until leased: proves the dispatcher took the job while we
	// were watching. The authoritative endpoint decides; the SSE stream is a
	// debugging tool, not an assertion mechanism.
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, err := http.Get(base + "/jobs/" + created.ID)
		if err == nil {
			var j jobResponse
			if json.NewDecoder(r.Body).Decode(&j) == nil && j.State == "leased" {
				r.Body.Close()
				break
			}
			r.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("the job never reached leased — dispatcher not draining the queue?")
		}
		time.Sleep(2 * time.Millisecond)
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

	// The payoff: the server is gone, so the verdict comes from Postgres —
	// the job was drained and recorded, not abandoned at its lease.
	st := store.New(pgtest.Pool(t), hub.New())
	got, err := st.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("reading the job back after shutdown: %v", err)
	}
	if got.State != job.Succeeded {
		t.Errorf("after SIGTERM the job is %s, want succeeded (drained, not abandoned)", got.State)
	}
}
