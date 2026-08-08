package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/httpapi"
	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/store"
)

// liveServer wires a store, a hub and the API the way main does — one hub,
// shared — and serves them over a real socket. Nothing in these tests touches
// the hub directly: if an event arrives, every link in the chain is connected.
func liveServer(t *testing.T) (*store.Store, *httptest.Server) {
	t.Helper()
	h := hub.New()
	s := freshDB(t, h)
	ts := httptest.NewServer(httpapi.New(s, h, slog.New(slog.NewJSONHandler(io.Discard, nil))))
	t.Cleanup(ts.Close)
	return s, ts
}

// openStream connects to an SSE endpoint and returns its lines. The read runs
// on its own goroutine because Scan blocks until bytes arrive, which is
// exactly the synchronisation these tests want: no polling, no sleeping.
//
// The goroutine ends when the body is closed, which happens when the request
// context is cancelled — t.Context() does that at the end of the test.
func openStream(t *testing.T, ctx context.Context, url string) <-chan string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		t.Fatalf("building the stream request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("opening the stream: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("stream Content-Type is %q, want text/event-stream", ct)
	}
	// The headers arrived before any event did, which is only possible if the
	// handler flushed them. Without that a client cannot tell a quiet stream
	// from a hung one — and this Do would have blocked until the first event.

	lines := make(chan string, 64)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	return lines
}

// awaitData reads until a data: line contains want, ignoring heartbeats and
// field lines that are not part of the payload.
func awaitData(t *testing.T, ctx context.Context, lines <-chan string, want string) string {
	t.Helper()
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("the stream closed before %q arrived", want)
			}
			if strings.HasPrefix(line, "data: ") && strings.Contains(line, want) {
				return strings.TrimPrefix(line, "data: ")
			}
		case <-ctx.Done():
			t.Fatalf("no event containing %q reached the client: %v", want, ctx.Err())
		}
	}
}

func postJob(t *testing.T, ctx context.Context, ts *httptest.Server, body string) jobResponse {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, "POST", ts.URL+"/jobs", strings.NewReader(body))
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
	return created
}

// The whole phase, end to end: one HTTP client holding a stream open, another
// enqueuing, and the event crossing a real socket in between.
func TestEnqueueAppearsOnTheEventStream(t *testing.T) {
	_, ts := liveServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// The stream is opened before the job is enqueued. Reversed, the test
	// races the subscription against the publish — and so would a real client,
	// because the hub keeps no history.
	lines := openStream(t, ctx, ts.URL+"/events")

	created := postJob(t, ctx, ts, `{"type":"resize","payload":{"w":100}}`)

	data := awaitData(t, ctx, lines, created.ID)

	// Decoded into a type declared here, not httpapi's: a test that shares the
	// server's struct agrees with it by construction, including when both are
	// wrong. The tags are the published contract.
	var ev struct {
		JobID     string    `json:"job_id"`
		State     string    `json:"state"`
		CreatedAt time.Time `json:"created_at"`
	}
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		t.Fatalf("the event is not valid JSON: %v (%s)", err, data)
	}
	if ev.JobID != created.ID {
		t.Errorf("event is for job %q, want %s: %s", ev.JobID, created.ID, data)
	}
	if ev.State != string(job.Pending) {
		t.Errorf("event state is %q, want pending: %s", ev.State, data)
	}
	if ev.CreatedAt.IsZero() {
		t.Errorf("event has no timestamp; it was assembled rather than read from the row: %s", data)
	}
}

// A job's own stream carries its lifecycle in order. This is the path phase 7
// will drive: a dispatcher leases, a worker finishes, and a watching client
// sees both without polling.
func TestJobLifecycleReachesItsOwnStream(t *testing.T) {
	s, ts := liveServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	created := postJob(t, ctx, ts, `{"type":"resize","payload":{"w":100}}`)

	// Subscribing after the enqueue is deliberate: this asserts on what
	// happens next, and a late subscriber receiving no backlog is the
	// documented contract.
	lines := openStream(t, ctx, ts.URL+"/jobs/"+created.ID+"/events")

	if _, err := s.Lease(ctx, "agent-1", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	awaitData(t, ctx, lines, string(job.Leased))

	if err := s.Complete(ctx, created.ID, "agent-1", job.Succeeded, "done"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	awaitData(t, ctx, lines, string(job.Succeeded))
}

// A per-job stream must not carry another job's events, over the wire and not
// merely inside the hub.
func TestAJobStreamIgnoresOtherJobs(t *testing.T) {
	s, ts := liveServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	watched := postJob(t, ctx, ts, `{"type":"resize","payload":{"w":1}}`)
	other := postJob(t, ctx, ts, `{"type":"resize","payload":{"w":2}}`)

	lines := openStream(t, ctx, ts.URL+"/jobs/"+watched.ID+"/events")

	// Both jobs change state; only one of them is being watched. Waiting for
	// the watched job's event is what makes this deterministic — by the time
	// it arrives the other job's event has already been through the hub.
	if _, err := s.Lease(ctx, "agent-1", 2, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	data := awaitData(t, ctx, lines, watched.ID)

	if strings.Contains(data, other.ID) {
		t.Errorf("another job's event reached this stream: %s", data)
	}
}
