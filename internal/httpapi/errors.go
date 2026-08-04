package httpapi

import (
	"errors"
	"net/http"

	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/store"
)

// errInvalidRequest reports that a request is invalid
var errInvalidRequest = errors.New("invalid request")

// writeError translates a domain or storage error into a status code and a
// client-safe message. It is the only place in the program that decides what
// a job.ErrNotFound looks like over HTTP.
func (a *API) writeError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, job.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrDuplicate), errors.Is(err, job.ErrInvalidTransition):
		status = http.StatusConflict
	case errors.Is(err, errInvalidRequest):
		status = http.StatusBadRequest
	}

	// A 5xx is the only case where the error is not the client's to see: it can
	// carry table names, constraint names, and connection strings.
	msg := err.Error()
	if status >= http.StatusInternalServerError {
		a.log.Error("request failed", "err", err, "method", r.Method, "path", r.URL.Path)
		msg = "internal error"
	}

	if err := writeJSON(w, status, errorResponse{Error: msg}); err != nil {
		a.log.Error("writing error response", "err", err)
	}
}
