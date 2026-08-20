package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/job"
)

func newTestAPI(t *testing.T, store Store) *API {
	t.Helper()
	return newTestAPIWithHub(t, store, hub.New())
}

// newTestAPIWithHub is for the stream tests, which need to publish into the
// same hub the handler subscribes to. The heartbeat is squeezed down from 15
// seconds so a test that waits for one finishes in milliseconds; a test nobody
// runs guards nothing.
func newTestAPIWithHub(t *testing.T, store Store, h *hub.Hub) *API {
	t.Helper()
	a := New(store, h, slog.New(slog.NewTextHandler(io.Discard, nil)))
	a.heartbeat = time.Millisecond
	return a
}

// newTestAPIWithLogger keeps the log instead of discarding it, for the tests
// that care about what the server told itself rather than what it told the
// client.
func newTestAPIWithLogger(t *testing.T, store Store) (*API, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	return New(store, hub.New(), slog.New(slog.NewJSONHandler(&buf, nil))), &buf
}

// A nil logger must not turn the first 500 into a panic — slog methods on a
// nil *Logger dereference it, and cmd/conveyord passing nil is a wiring
// mistake that would otherwise surface under load rather than at startup.
// There is nothing to assert: the test is that this returns.
func TestNewDefaultsTheLogger(t *testing.T) {
	New(nil, nil, nil).writeError(httptest.NewRecorder(),
		httptest.NewRequest("GET", "/", nil), errors.New("boom"))
}

func TestWriteErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"not found", fmt.Errorf("job %s: %w", "j1", job.ErrNotFound), http.StatusNotFound},
		{"duplicate", fmt.Errorf("job %s: %w", "j1", job.ErrDuplicate), http.StatusConflict},
		{"bad transition", fmt.Errorf("x: %w", job.ErrInvalidTransition), http.StatusConflict},
		{"invalid request", fmt.Errorf("type: %w", errInvalidRequest), http.StatusBadRequest},
		{"unknown", errors.New("connection refused"), http.StatusInternalServerError},
	}

	s := newTestAPI(t, nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			s.writeError(w, httptest.NewRequest("GET", "/", nil), tt.err)
			if w.Code != tt.want {
				t.Errorf("status is %d, want %d", w.Code, tt.want)
			}
		})
	}
}

// A 500 must not tell the client what the database is called — and must still
// tell the operator. Hiding the error and recording it are one contract: a 500
// that leaks nothing and logs nothing is an outage nobody can diagnose.
func TestWriteErrorHidesInternalDetailButLogsIt(t *testing.T) {
	s, logged := newTestAPIWithLogger(t, nil)
	w := httptest.NewRecorder()
	leaky := errors.New(`duplicate key value violates unique constraint "jobs_pkey"`)

	s.writeError(w, httptest.NewRequest("GET", "/jobs/j1", nil), leaky)

	if strings.Contains(w.Body.String(), "jobs_pkey") {
		t.Errorf("500 body leaked internal detail: %s", w.Body)
	}
	if !strings.Contains(logged.String(), "jobs_pkey") {
		t.Errorf("500 dropped the real error instead of logging it: %s", logged)
	}
}

// Every error body is an object with an "error" field. Unmarshalling into the
// struct is the assertion strings.Contains cannot make: a body that is a JSON
// *string* containing JSON text passes a substring check and fails here, which
// is exactly the bug this test exists for.
func TestWriteErrorBodyIsAnObject(t *testing.T) {
	s := newTestAPI(t, nil)

	for _, err := range []error{
		fmt.Errorf("job j1: %w", job.ErrNotFound),
		fmt.Errorf("job j1: %w", job.ErrDuplicate),
		fmt.Errorf("type: %w", errInvalidRequest),
		errors.New("connection refused"),
	} {
		w := httptest.NewRecorder()
		s.writeError(w, httptest.NewRequest("GET", "/", nil), err)

		var got errorResponse
		if uerr := json.Unmarshal(w.Body.Bytes(), &got); uerr != nil {
			t.Errorf("body for %v is not an error object: %v (%s)", err, uerr, w.Body)
			continue
		}
		if got.Error == "" {
			t.Errorf("body for %v has an empty error field: %s", err, w.Body)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type for %v is %q, want application/json", err, ct)
		}
	}
}

// A 404 must still be useful — the wrapped context is safe and clients need it.
func TestWriteErrorKeepsDomainDetail(t *testing.T) {
	s := newTestAPI(t, nil)
	w := httptest.NewRecorder()

	s.writeError(w, httptest.NewRequest("GET", "/", nil),
		fmt.Errorf("%w: j000", job.ErrNotFound))

	if !strings.Contains(w.Body.String(), "j000") {
		t.Errorf("404 body dropped the job id: %s", w.Body)
	}
}
