package httpapi

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

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
	a.handler = a.routes()
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
		a.log.Error("writing response", "err", err, "method", r.Method, "path", r.URL.Path)
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
		a.log.Error("writing response", "err", err, "method", r.Method, "path", r.URL.Path)
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
		a.log.Error("writing response", "err", err, "method", r.Method, "path", r.URL.Path)
	}
}

func (a *API) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.enqueue)
	mux.HandleFunc("GET /jobs/{id}", a.get)
	mux.HandleFunc("GET /jobs", a.list)
	return mux
}
