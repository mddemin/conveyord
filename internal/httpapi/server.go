// Package httpapi serves the job API over HTTP.
//
// [New] returns an [API], which is an http.Handler with routing and middleware
// already assembled; binding it to a port belongs to the caller. It depends on
// storage through the [Store] interface declared here rather than on any
// concrete type, so a test can substitute a stub and the package never imports
// a database driver.
//
// Every response this package generates is JSON. Success bodies are a job or a
// list of jobs; every failure is an object with a single "error" field,
// produced in exactly one place — writeError, which owns the mapping from
// domain and storage errors to status codes. A 5xx never carries the underlying
// error to the client, only to the log.
//
// Two responses come from net/http instead and do not follow that shape: an
// unrouted path or an unsupported method gets ServeMux's plain-text 404 or 405,
// and http.TimeoutHandler's 503 carries a JSON body under a sniffed
// text/plain content type.
//
// Each request is assigned or inherits an X-Request-ID, echoed on the response
// and attached to every log line it produces, so a client-visible identifier
// and the server's own record of what happened can be joined.
package httpapi

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

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
	store   Store
	log     *slog.Logger
	handler http.Handler
}

// New returns an API backed by store. A nil log defaults to slog.Default(),
// so a wiring mistake degrades to the standard logger instead of panicking
// on the first 500.
func New(store Store, log *slog.Logger) *API {
	if log == nil {
		log = slog.Default()
	}
	a := &API{store: store, log: log}
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

func (a *API) enqueue(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJSON[enqueueRequest](w, r)
	if err != nil {
		a.writeError(w, r, err)
		return
	}

	if err := req.validate(); err != nil {
		a.writeError(w, r, err)
		return
	}

	var id string
	if req.ID != "" {
		id = req.ID
	} else {
		id = rand.Text()[:16]
	}

	j := &job.Job{ID: id, Type: req.Type, Payload: req.Payload, State: job.Pending}
	if err := a.store.Enqueue(r.Context(), j); err != nil {
		a.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/jobs/"+j.ID)
	if err := writeJSON(w, http.StatusCreated, toResponse(j)); err != nil {
		a.logger(r.Context()).Error("writing response", "err", err, "method", r.Method, "path", r.URL.Path)
	}
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := a.store.Get(r.Context(), id)
	if err != nil {
		a.writeError(w, r, err)
		return
	}

	if err := writeJSON(w, http.StatusOK, toResponse(j)); err != nil {
		a.logger(r.Context()).Error("writing response", "err", err, "method", r.Method, "path", r.URL.Path)
	}
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.Filter{}

	if q.Has("state") {
		s := q.Get("state")
		if !job.State(s).Valid() {
			a.writeError(w, r, fmt.Errorf("%w: state %q is not valid", errInvalidRequest, s))
			return
		}
		f.State = job.State(s)
	}

	if q.Has("limit") {
		limit, err := strconv.Atoi(q.Get("limit"))
		if err != nil || limit <= 0 {
			a.writeError(w, r, fmt.Errorf("%w: limit %q is not valid", errInvalidRequest, q.Get("limit")))
			return
		}
		f.Limit = limit
	}
	jobs, err := a.store.List(r.Context(), f)
	if err != nil {
		a.writeError(w, r, err)
		return
	}
	out := make([]jobResponse, 0, len(jobs))
	for i := range jobs {
		out = append(out, toResponse(&jobs[i]))
	}
	if err := writeJSON(w, http.StatusOK, out); err != nil {
		a.logger(r.Context()).Error("writing response", "err", err, "method", r.Method, "path", r.URL.Path)
	}
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()

	// Timeout is per-route, not global: http.TimeoutHandler buffers the whole
	// response, and a buffered stream never streams. Phase 6's SSE route
	// registers with mux.HandleFunc instead, deliberately unwrapped.
	timed := func(h http.HandlerFunc) http.Handler { return a.withTimeout(requestTimeout, h) }

	mux.Handle("POST /jobs", timed(a.enqueue))
	mux.Handle("GET /jobs/{id}", timed(a.get))
	mux.Handle("GET /jobs", timed(a.list))
	return mux
}
