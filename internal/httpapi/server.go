// Package httpapi serves the job API over HTTP.
//
// [New] returns an [API], which is an http.Handler with routing and middleware
// already assembled; binding it to a port belongs to the caller. It depends on
// storage through the [Store] interface declared here rather than on any
// concrete type, so a test can substitute a stub and the package never imports
// a database driver.
//
// Every request-response endpoint here answers in JSON. Success bodies are a
// job or a list of jobs; every failure is an object with a single "error"
// field, produced in exactly one place — writeError, which owns the mapping
// from domain and storage errors to status codes. A 5xx never carries the
// underlying error to the client, only to the log.
//
// Two responses come from net/http instead and do not follow that shape: an
// unrouted path or an unsupported method gets ServeMux's plain-text 404 or 405,
// and http.TimeoutHandler's 503 carries a JSON body under a sniffed
// text/plain content type.
//
// The two event endpoints are not request-response at all. GET /events and
// GET /jobs/{id}/events hold the connection open and write Server-Sent Events
// until the client goes away, so they are registered without the per-route
// timeout the others carry — a buffered stream never streams.
//
// Those streams are advisory and lossy on purpose. They report what is
// happening now; they are not a history and not a source of truth. A client
// that misses events — because it was slow, or because it was not connected —
// is told so with a "lagged" event and recovers with GET /jobs/{id}, which is
// authoritative. That division is why the hub may drop events for a subscriber
// that is not keeping up rather than slow down every other subscriber, and why
// a client connecting late is sent no backlog.
//
// Each request is assigned or inherits an X-Request-ID, echoed on the response
// and attached to every log line it produces, so a client-visible identifier
// and the server's own record of what happened can be joined.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/store"
)

// Store is the slice of storage this package needs. Declared here, not in
// internal/store, so the API depends on a behaviour rather than on a struct.
type Store interface {
	Enqueue(ctx context.Context, j *job.Job) error
	Get(ctx context.Context, id string) (*job.Job, error)
	List(ctx context.Context, f store.Filter) ([]job.Job, error)
}

const requestTimeout = time.Minute

// API serves the job API. It is an http.Handler; binding it to a port is
// the caller's job.
type API struct {
	store     Store
	hub       *hub.Hub
	log       *slog.Logger
	handler   http.Handler
	heartbeat time.Duration
}

// New returns an API backed by store, streaming the events published to
// events. Both optional arguments degrade rather than fail: a nil log becomes
// slog.Default() instead of panicking on the first 500, and a nil hub becomes
// an empty one, so a caller that forgets to wire it gets streams that connect
// and stay quiet instead of a nil dereference halfway through a response.
// Neither is a supported way to call this — they are wiring mistakes that
// should be visible in a log, not in a stack trace under load.
func New(store Store, events *hub.Hub, log *slog.Logger) *API {
	if log == nil {
		log = slog.Default()
	}
	if events == nil {
		events = hub.New() // a stream that delivers nothing beats a nil-pointer panic mid-response
	}
	a := &API{store: store, log: log, hub: events, heartbeat: heartbeatDuration}
	// Invert logging and recover and every 500 disappears from the access log
	// invert logging and request id and request id disappears from logs
	a.handler = chain(a.routes(), a.withRequestID, a.withLogging, a.withRecover)
	return a
}

// ServeHTTP makes API an http.Handler. Routing and middleware are assembled
// once in New; this only dispatches.
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()

	// Timeout is per-route, not global: http.TimeoutHandler buffers the whole
	// response, and a buffered stream never streams.
	timed := func(h http.HandlerFunc) http.Handler { return a.withTimeout(requestTimeout, h) }

	mux.Handle("POST /jobs", timed(a.enqueue))
	mux.Handle("GET /jobs/{id}", timed(a.get))
	mux.Handle("GET /jobs", timed(a.list))

	// HandleFunc, not Handle(timed(...)): the streams must not be wrapped in a
	// timeout. The difference from the three lines above is deliberate and is
	// the only thing enforcing it.
	//
	// "/jobs/{id}/events" does not shadow "/jobs/{id}": ServeMux prefers the
	// more specific pattern, and TestJobRouteAndEventRouteCoexist guards it.
	mux.HandleFunc("GET /jobs/{id}/events", a.jobEvents)
	mux.HandleFunc("GET /events", a.allEvents)
	return mux
}
