package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

func TestRequestIDIsGeneratedAndEchoed(t *testing.T) {
	api := newTestAPI(t, stubStore{list: emptyList})

	w := do(api, "GET", "/jobs", "")

	if w.Header().Get("X-Request-ID") == "" {
		t.Error("no X-Request-ID on the response")
	}
}

func TestRequestIDIsInheritedFromTheClient(t *testing.T) {
	api, logged := newTestAPIWithLogger(t, stubStore{list: emptyList})

	r := httptest.NewRequest("GET", "/jobs", nil)
	r.Header.Set("X-Request-ID", "upstream-123")
	w := httptest.NewRecorder()
	api.ServeHTTP(w, r)

	if got := w.Header().Get("X-Request-ID"); got != "upstream-123" {
		t.Errorf("X-Request-ID is %q, want the client's upstream-123", got)
	}
	// Echoing the header is only half of it. Adopting the upstream id and then
	// logging a different one breaks the join that distributed tracing exists
	// for, and the response header alone cannot show that.
	if !strings.Contains(logged.String(), `"request_id":"upstream-123"`) {
		t.Errorf("the log used a different id than the response: %s", logged)
	}
}

// A client-supplied id reaches a response header and every log line for the
// request. Anything oversized or non-printable is discarded rather than
// propagated — a generated id is always better than an attacker's.
func TestRequestIDRejectsAHostileClientValue(t *testing.T) {
	for _, bad := range []string{
		strings.Repeat("a", maxRequestIDLen+1),
		"a\nfake-field=1",
		"a\x00b",
	} {
		t.Run(fmt.Sprintf("%q", bad), func(t *testing.T) {
			api := newTestAPI(t, stubStore{list: emptyList})

			r := httptest.NewRequest("GET", "/jobs", nil)
			r.Header.Set("X-Request-ID", bad)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, r)

			switch got := w.Header().Get("X-Request-ID"); got {
			case bad:
				t.Errorf("a hostile id was adopted and echoed: %q", got)
			case "":
				t.Error("the id was rejected but nothing was generated to replace it")
			}
		})
	}
}

func TestPanicBecomes500AndIsLogged(t *testing.T) {
	api, logged := newTestAPIWithLogger(t, stubStore{
		list: func(context.Context, Filter) ([]job.Job, error) { panic("boom") },
	})

	w := do(api, "GET", "/jobs", "")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status after a panic is %d, want 500", w.Code)
	}
	if !strings.Contains(logged.String(), "boom") {
		t.Errorf("panic value never reached the log: %s", logged)
	}
	// The panic value says what broke; only the stack says where. A recovered
	// panic logged without one is an alert nobody can act on.
	if !strings.Contains(logged.String(), `"stack":"goroutine `) {
		t.Errorf("no stack was captured: %s", logged)
	}
	// The client must not be told what panicked.
	if strings.Contains(w.Body.String(), "boom") {
		t.Errorf("panic value leaked to the client: %s", w.Body)
	}
	if !strings.Contains(logged.String(), `"status":500`) {
		t.Errorf("a panicking request produced no access-log line: %s", logged)
	}
}

// http.ErrAbortHandler is the stdlib's "drop this connection on purpose, do not
// log it" sentinel. Recovering it turns a deliberate abort into a fake 500 in
// the metrics, so it has to pass straight through.
func TestAbortHandlerIsNotSwallowed(t *testing.T) {
	api := newTestAPI(t, stubStore{})
	h := api.withRecover(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	defer func() {
		if v := recover(); v != http.ErrAbortHandler {
			t.Errorf("withRecover re-panicked with %v, want http.ErrAbortHandler", v)
		}
	}()

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	t.Error("withRecover swallowed http.ErrAbortHandler and returned normally")
}

// A status line cannot be unsent. Once the handler has committed to one, the
// recovery must leave it alone — writing a 500 on top produces a "superfluous
// WriteHeader" warning and leaves the client with the first status anyway.
func TestPanicAfterAStatusDoesNotOverwriteIt(t *testing.T) {
	tests := []struct {
		name     string
		handler  http.HandlerFunc
		wantCode int
		wantBody string
	}{
		{
			name: "explicit 202",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				panic("boom")
			},
			wantCode: http.StatusAccepted,
		},
		{
			// 200 is ambiguous: it is also the recorder's initial status, so
			// "the handler chose 200" and "the handler wrote nothing" are the
			// same value unless the recorder tracks the call, not the code.
			name: "explicit 200",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				panic("boom")
			},
			wantCode: http.StatusOK,
		},
		{
			// The path a handler takes when it never calls WriteHeader at all:
			// net/http commits an implicit 200 inside the first Write, so the
			// status line is gone even though WriteHeader was never called.
			// A recorder that only watches WriteHeader cannot see this.
			name: "implicit 200 from a bare Write",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Write([]byte(`{"partial":`))
				panic("boom")
			},
			wantCode: http.StatusOK,
			wantBody: `{"partial":`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newTestAPI(t, stubStore{})
			h := chain(tt.handler, api.withRequestID, api.withLogging, api.withRecover)

			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/x", nil))

			if w.Code != tt.wantCode {
				t.Errorf("status is %d, want the %d the handler already sent", w.Code, tt.wantCode)
			}
			if got := w.Body.String(); got != tt.wantBody {
				t.Errorf("body is %q, want %q — nothing may be appended to a committed response", got, tt.wantBody)
			}
		})
	}
}

// The timeout is the one response the API produces without going through
// writeJSON, so nothing else pins its shape.
func TestTimeoutIs503WithAJSONBody(t *testing.T) {
	api := newTestAPI(t, stubStore{})
	h := api.withTimeout(time.Nanosecond, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/jobs", nil))

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status is %d, want 503", w.Code)
	}
	var got errorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("timeout body is not an error object: %v (%s)", err, w.Body)
	}
	if got.Error == "" {
		t.Errorf("timeout body has an empty error field: %s", w.Body)
	}
}

// A panic must not take the process down with it.
func TestServerSurvivesAPanic(t *testing.T) {
	api := newTestAPI(t, stubStore{
		list: func(context.Context, Filter) ([]job.Job, error) { panic("boom") },
	})

	do(api, "GET", "/jobs", "")
	w := do(api, "GET", "/jobs", "")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("second request after a panic got %d", w.Code)
	}
}

func TestRequestIsLoggedOnce(t *testing.T) {
	api, logged := newTestAPIWithLogger(t, stubStore{list: emptyList})

	do(api, "GET", "/jobs", "")

	if n := strings.Count(logged.String(), "\n"); n != 1 {
		t.Errorf("one request produced %d log lines, want 1: %s", n, logged)
	}
	for _, want := range []string{
		`"status":200`, `"method":"GET"`, `"path":"/jobs"`, `"request_id":"`,
	} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log line is missing %s: %s", want, logged)
		}
	}
}

// Phase 6 streams, and streaming needs http.Flusher to survive every wrapper
// between the socket and the handler. Both halves matter: the chain must
// preserve it, and http.TimeoutHandler must be shown to destroy it — that
// second fact is the entire reason routes() applies the timeout per route
// instead of globally.
func TestFlusherSurvivesTheChainButNotATimeout(t *testing.T) {
	api := newTestAPI(t, stubStore{})

	// http.NewResponseController, not a w.(http.Flusher) assertion: recorder
	// wraps the real writer, and any wrapper that declares Flush makes every
	// writer underneath it look streamable. The controller walks Unwrap to the
	// writer that actually decides, which is the only question worth asking.
	canFlush := func(h func(http.Handler) http.Handler) bool {
		var err error
		probe := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			err = http.NewResponseController(w).Flush()
		}))
		if h != nil {
			probe = h(probe)
		}
		c := chain(probe, api.withRequestID, api.withLogging, api.withRecover)
		c.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
		return err == nil
	}

	if !canFlush(nil) {
		t.Error("the middleware chain hides flushing; SSE will not stream")
	}
	timed := func(next http.Handler) http.Handler { return api.withTimeout(time.Minute, next) }
	if canFlush(timed) {
		t.Error("a timeout-wrapped route can flush; " +
			"per-route timeouts were introduced on the premise that it cannot")
	}
}

// Every JSON route is wrapped in http.TimeoutHandler, which runs the handler
// on its own goroutine and re-panics the value on the serving one. By the time
// withRecover's deferred recover runs, the goroutine that panicked is gone and
// debug.Stack() describes the middleware chain instead: a 500 whose stack names
// nothing but net/http, on every route a client can reach. Asserting that a
// stack exists — as TestPanicBecomes500AndIsLogged does — passes either way;
// the question is whether it points anywhere.
func TestARecoveredPanicNamesTheCodeThatPanicked(t *testing.T) {
	api, logged := newTestAPIWithLogger(t, stubStore{
		list: func(context.Context, Filter) ([]job.Job, error) { return panicInAHandler() },
	})

	if w := do(api, "GET", "/jobs", ""); w.Code != http.StatusInternalServerError {
		t.Fatalf("status after a panic is %d, want 500: %s", w.Code, w.Body)
	}

	if !strings.Contains(logged.String(), "panicInAHandler") {
		t.Errorf("the recovered stack does not name the function that panicked: %s", logged)
	}
}

// A named function so the assertion above has something to look for. Appearing
// in a stack trace is its whole job.
func panicInAHandler() ([]job.Job, error) { panic("boom") }
