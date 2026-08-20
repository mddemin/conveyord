package httpapi

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"strconv"

	"github.com/mddemin/conveyor/internal/job"
)

const MaxLimit = 1000

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
	f := Filter{}

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
		if err != nil || limit <= 0 || limit > MaxLimit {
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
