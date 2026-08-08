package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
)

// waitFor spins until cond holds. Gosched rather than Sleep: a sleep either
// makes the test slow or makes it flaky, and picking the duration is guessing
// at the scheduler. Callers must only pass conditions that are safe to read
// concurrently.
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

// unflushableRecorder is a ResponseWriter that genuinely cannot stream. It
// wraps a recorder by hand instead of embedding it: embedding would promote
// httptest.ResponseRecorder's Flush, and a method cannot be un-promoted.
type unflushableRecorder struct {
	rec *httptest.ResponseRecorder
}

func (u *unflushableRecorder) Header() http.Header         { return u.rec.Header() }
func (u *unflushableRecorder) Write(b []byte) (int, error) { return u.rec.Write(b) }
func (u *unflushableRecorder) WriteHeader(code int)        { u.rec.WriteHeader(code) }

// streamRecorder is httptest.ResponseRecorder with a lock. Every stream test
// reads the body from the test goroutine while the handler writes it from
// another, which the stdlib recorder does not allow — -race says so, loudly.
//
// gate, when non-nil, blocks Write until the test closes it. That is how a
// test holds the handler still long enough for the hub's buffer to overflow.
type streamRecorder struct {
	gate chan struct{}

	mu      sync.Mutex
	body    bytes.Buffer
	code    int
	flushed bool

	hdr http.Header // written by the handler before its first flush only
}

func newStreamRecorder() *streamRecorder {
	return &streamRecorder{hdr: make(http.Header), code: http.StatusOK}
}

func (s *streamRecorder) Header() http.Header { return s.hdr }

func (s *streamRecorder) Write(b []byte) (int, error) {
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.Write(b)
}

func (s *streamRecorder) WriteHeader(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.code = code
}

func (s *streamRecorder) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushed = true
}

func (s *streamRecorder) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.body.String()
}

func (s *streamRecorder) Flushed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushed
}

// serveStream runs the handler for path on its own goroutine and returns the
// recorder it writes to plus a stop function. Every stream test needs this
// shape: the handler only returns when the client disconnects.
func serveStream(t *testing.T, api *API, path string) (*streamRecorder, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := newStreamRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		api.ServeHTTP(w, httptest.NewRequest("GET", path, nil).WithContext(ctx))
	}()
	return w, func() {
		cancel()
		<-done // the handler must have returned before a test reads w
	}
}

func TestEventStreamSetsStreamingHeaders(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	w, stop := serveStream(t, api, "/events")
	waitFor(t, func() bool { return h.Len() == 1 })
	stop()

	for _, want := range [][2]string{
		{"Content-Type", "text/event-stream"},
		{"Cache-Control", "no-cache"},
		// Not standard HTTP. Without it an nginx in front of the server
		// re-introduces the exact buffering that flushing exists to prevent,
		// and only in deployment.
		{"X-Accel-Buffering", "no"},
	} {
		if got := w.Header().Get(want[0]); got != want[1] {
			t.Errorf("%s is %q, want %q", want[0], got, want[1])
		}
	}
}

// The headers must reach the client before any event does, or a client on a
// quiet job cannot tell "connected, waiting" from "hung".
func TestEventStreamFlushesBeforeTheFirstEvent(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	w, stop := serveStream(t, api, "/events")
	waitFor(t, func() bool { return h.Len() == 1 })
	flushed := w.Flushed()
	stop()

	if !flushed {
		t.Error("nothing was flushed before the first event; the client sees no headers until one arrives")
	}
}

func TestEventStreamWritesAnEventPerPublish(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	w, stop := serveStream(t, api, "/events")
	waitFor(t, func() bool { return h.Len() == 1 })
	h.Publish(job.Event{JobID: "j1", State: job.Pending})
	waitFor(t, func() bool { return strings.Contains(w.String(), "j1") })
	stop()

	body := w.String()
	if !strings.Contains(body, "event: job\n") {
		t.Errorf("no named job event in the stream: %q", body)
	}
	if !strings.Contains(body, "data: ") {
		t.Errorf("no data: line in the stream: %q", body)
	}
	// The blank line is what dispatches the event. Without it a client holds a
	// complete event in its buffer and never fires a listener.
	if !strings.HasSuffix(body, "\n\n") {
		t.Errorf("event is not terminated by a blank line: %q", body)
	}
}

// Failure mode 2. Every disconnected client must free its slot, or a server
// that has been up a week is holding a buffer for every tab ever opened.
func TestDisconnectUnsubscribes(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	_, stop := serveStream(t, api, "/events")
	waitFor(t, func() bool { return h.Len() == 1 })
	stop()

	if h.Len() != 0 {
		t.Errorf("Len is %d after the client disconnected, want 0", h.Len())
	}
}

func TestJobEventStreamFiltersByID(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	w, stop := serveStream(t, api, "/jobs/j1/events")
	waitFor(t, func() bool { return h.Len() == 1 })
	h.Publish(job.Event{JobID: "j2", State: job.Pending})
	h.Publish(job.Event{JobID: "j1", State: job.Leased})
	waitFor(t, func() bool { return strings.Contains(w.String(), "leased") })
	stop()

	if strings.Contains(w.String(), "j2") {
		t.Errorf("another job's event reached the stream: %q", w.String())
	}
}

// Proxies and load balancers close connections they consider idle, and a
// client cannot otherwise distinguish "no events yet" from "the server died".
func TestQuietStreamSendsHeartbeats(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h) // heartbeat is 1ms in tests

	w, stop := serveStream(t, api, "/events")
	waitFor(t, func() bool { return strings.Contains(w.String(), ":") })
	stop()

	// A comment line, not an event: it must not fire a client's listener.
	if !strings.Contains(w.String(), ": heartbeat\n\n") {
		t.Errorf("no heartbeat comment in a stream with no events: %q", w.String())
	}
}

// A client whose buffer overflowed cannot be told what it missed — that is
// what dropping means. It must be told *that* it missed something, or a lost
// terminal event leaves its UI showing "running" forever.
func TestALaggingClientIsToldToResync(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	w := newStreamRecorder()
	w.gate = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		api.ServeHTTP(w, httptest.NewRequest("GET", "/events", nil).WithContext(ctx))
	}()

	waitFor(t, func() bool { return h.Len() == 1 })

	// The handler is now blocked in Write, so nothing drains the subscription
	// and the buffer fills. This is a slow client, reproduced exactly.
	for i := 0; i < 200; i++ {
		h.Publish(job.Event{JobID: "j1", State: job.Pending})
	}

	close(w.gate) // let it run again
	waitFor(t, func() bool { return strings.Contains(w.String(), "event: lagged") })
	cancel()
	<-done
}

// /jobs/{id} and /jobs/{id}/events must not shadow each other.
func TestJobRouteAndEventRouteCoexist(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{
		get: func(context.Context, string) (*job.Job, error) {
			return &job.Job{ID: "j1", Type: "resize", State: job.Pending}, nil
		},
	}, h)

	w := do(api, "GET", "/jobs/j1", "")

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("GET /jobs/{id} returned %q; the events route captured it", ct)
	}
}

// A streaming handler that cannot flush produces a response the client never
// sees. Fail at connection time instead. This only works because recorder
// exposes Unwrap rather than defining Flush — a recorder with its own Flush
// makes every writer look streamable and this check unfalsifiable.
func TestStreamRequiresAFlusher(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	// An already-cancelled context so that a regression fails in milliseconds
	// rather than hanging: an implementation that skips the check goes on to
	// stream, and a stream only ends when the client disconnects.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &unflushableRecorder{httptest.NewRecorder()}
	api.ServeHTTP(w, httptest.NewRequest("GET", "/events", nil).WithContext(ctx))

	if w.rec.Code < 500 {
		t.Errorf("status is %d; a writer that cannot flush must not get a stream", w.rec.Code)
	}
	if h.Len() != 0 {
		t.Errorf("Len is %d after a rejected stream, want 0", h.Len())
	}
}
