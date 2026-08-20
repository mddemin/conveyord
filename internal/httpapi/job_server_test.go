package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

// stubStore lets a test choose exactly what storage does, including fail.
type stubStore struct {
	enqueue func(ctx context.Context, j *job.Job) error
	get     func(ctx context.Context, id string) (*job.Job, error)
	list    func(ctx context.Context, f Filter) ([]job.Job, error)
}

func (s stubStore) Enqueue(ctx context.Context, j *job.Job) error        { return s.enqueue(ctx, j) }
func (s stubStore) Get(ctx context.Context, id string) (*job.Job, error) { return s.get(ctx, id) }
func (s stubStore) List(ctx context.Context, f Filter) ([]job.Job, error) {
	return s.list(ctx, f)
}

// emptyList succeeds with no jobs. It is what the middleware tests want: a
// route that reaches a handler and returns, where the handler itself is beside
// the point.
func emptyList(context.Context, Filter) ([]job.Job, error) { return nil, nil }

// do runs one request through the API's full routing and middleware and
// returns what the client would have seen.
func do(a *API, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	return w
}

func TestEnqueueCreatesAJob(t *testing.T) {
	var got *job.Job
	api := newTestAPI(t, stubStore{enqueue: func(_ context.Context, j *job.Job) error {
		got = j
		j.State, j.CreatedAt, j.UpdatedAt = job.Pending, time.Now(), time.Now()
		return nil
	}})

	w := do(api, "POST", "/jobs", `{"type":"resize","payload":{"w":100}}`)

	if w.Code != http.StatusCreated {
		t.Fatalf("status is %d, want 201: %s", w.Code, w.Body)
	}
	if got.Type != "resize" {
		t.Errorf("store received type %q, want resize", got.Type)
	}
	if got.ID == "" {
		t.Error("store received an empty id; the server must generate one")
	}
	if loc := w.Header().Get("Location"); loc != "/jobs/"+got.ID {
		t.Errorf("Location is %q, want /jobs/%s", loc, got.ID)
	}
}

func TestEnqueueHonorsAClientSuppliedID(t *testing.T) {
	var got *job.Job
	api := newTestAPI(t, stubStore{enqueue: func(_ context.Context, j *job.Job) error {
		got = j
		return nil
	}})

	do(api, "POST", "/jobs", `{"id":"my-idempotency-key","type":"resize"}`)

	if got.ID != "my-idempotency-key" {
		t.Errorf("store received id %q, want the client's", got.ID)
	}
}

func TestEnqueueRejectsAMissingType(t *testing.T) {
	api := newTestAPI(t, stubStore{enqueue: func(context.Context, *job.Job) error {
		t.Error("store was called for an invalid request")
		return nil
	}})

	w := do(api, "POST", "/jobs", `{"payload":{}}`)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want 400", w.Code)
	}
}

// Everything validate() rejects. The stub fails the test if it is reached at
// all: a 400 that still wrote to the database would satisfy a status-only
// assertion while leaving a junk row behind.
func TestEnqueueValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"blank type", `{"type":"   "}`},
		{"type too long", `{"type":"` + strings.Repeat("a", 129) + `"}`},
		{"control character in type", `{"type":"resize\ninjected"}`},
		{"id with a slash", `{"id":"a/b","type":"resize"}`},
		{"id too long", `{"id":"` + strings.Repeat("a", 65) + `","type":"resize"}`},
		// Legal characters, but ServeMux resolves the path away: the client
		// would get a 307 to / when following the Location we handed it.
		{"id is a path segment", `{"id":"..","type":"resize"}`},
		{"id is a dot", `{"id":".","type":"resize"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := newTestAPI(t, stubStore{enqueue: func(context.Context, *job.Job) error {
				t.Error("store was called for an invalid request")
				return nil
			}})

			w := do(api, "POST", "/jobs", tt.body)
			if w.Code != http.StatusBadRequest {
				t.Errorf("status is %d, want 400: %s", w.Code, w.Body)
			}
		})
	}
}

// Trimming is a silent transformation, so it needs a witness: without this,
// "resize " and "resize" become two job types that look identical in a log.
func TestEnqueueTrimsSurroundingSpace(t *testing.T) {
	var got *job.Job
	api := newTestAPI(t, stubStore{enqueue: func(_ context.Context, j *job.Job) error {
		got = j
		return nil
	}})

	do(api, "POST", "/jobs", "{\"id\":\" k1 \",\"type\":\"  resize\\r\\n\"}")

	if got.Type != "resize" {
		t.Errorf("store received type %q, want %q", got.Type, "resize")
	}
	if got.ID != "k1" {
		t.Errorf("store received id %q, want %q", got.ID, "k1")
	}
}

func TestEnqueueRejectsADuplicate(t *testing.T) {
	api := newTestAPI(t, stubStore{enqueue: func(context.Context, *job.Job) error {
		return fmt.Errorf("job id x: %w", job.ErrDuplicate)
	}})

	w := do(api, "POST", "/jobs", `{"id":"x","type":"resize"}`)

	if w.Code != http.StatusConflict {
		t.Errorf("status is %d, want 409", w.Code)
	}
}

func TestGetJobNotFound(t *testing.T) {
	api := newTestAPI(t, stubStore{get: func(_ context.Context, id string) (*job.Job, error) {
		return nil, fmt.Errorf("%w: %s", job.ErrNotFound, id)
	}})

	w := do(api, "GET", "/jobs/nope", "")

	if w.Code != http.StatusNotFound {
		t.Errorf("status is %d, want 404", w.Code)
	}
}

func TestListRejectsAnUnknownState(t *testing.T) {
	// "?state=" is present-but-empty: absent means "any state", so an explicit
	// empty value is a broken client, not a wildcard.
	for _, q := range []string{"?state=pnding", "?state=failed", "?state=PENDING", "?state="} {
		t.Run(q, func(t *testing.T) {
			api := newTestAPI(t, stubStore{list: func(context.Context, Filter) ([]job.Job, error) {
				t.Error("store was queried with an invalid state")
				return nil, nil
			}})

			if w := do(api, "GET", "/jobs"+q, ""); w.Code != http.StatusBadRequest {
				t.Errorf("status is %d, want 400 for an unknown state", w.Code)
			}
		})
	}
}

// The query string is only useful if it reaches the store. A handler that
// parses ?state= and ?limit= and then forgets to assign them passes every
// other test in this file: the response looks identical, it is just the wrong
// jobs.
func TestListPassesTheQueryToTheStore(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  Filter
	}{
		{"nothing", "", Filter{}},
		{"state only", "?state=leased", Filter{State: job.Leased}},
		{"limit only", "?limit=5", Filter{Limit: 5}},
		{"both", "?state=dead&limit=25", Filter{State: job.Dead, Limit: 25}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Filter
			api := newTestAPI(t, stubStore{list: func(_ context.Context, f Filter) ([]job.Job, error) {
				got = f
				return nil, nil
			}})

			if w := do(api, "GET", "/jobs"+tt.query, ""); w.Code != http.StatusOK {
				t.Fatalf("status is %d, want 200: %s", w.Code, w.Body)
			}
			if got != tt.want {
				t.Errorf("store received %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestListRejectsABadLimit(t *testing.T) {
	// "?limit=" is present-but-empty, which is what ?limit=${x} produces when x
	// is undefined. Has() is what lets the handler tell it from absent.
	for _, q := range []string{"?limit=abc", "?limit=-1", "?limit=0", "?limit=1.5", "?limit="} {
		t.Run(q, func(t *testing.T) {
			api := newTestAPI(t, stubStore{list: func(context.Context, Filter) ([]job.Job, error) {
				t.Error("store was queried with an invalid limit")
				return nil, nil
			}})

			if w := do(api, "GET", "/jobs"+q, ""); w.Code != http.StatusBadRequest {
				t.Errorf("status is %d, want 400", w.Code)
			}
		})
	}
}

// A nil slice marshals to null. Every client then crashes on .length.
func TestListReturnsAnEmptyArrayNotNull(t *testing.T) {
	api := newTestAPI(t, stubStore{list: func(context.Context, Filter) ([]job.Job, error) {
		return nil, nil
	}})

	w := do(api, "GET", "/jobs", "")

	if body := strings.TrimSpace(w.Body.String()); body != "[]" {
		t.Errorf("empty list serialized as %s, want []", body)
	}
}

func TestPayloadIsNotBase64(t *testing.T) {
	api := newTestAPI(t, stubStore{get: func(context.Context, string) (*job.Job, error) {
		return &job.Job{ID: "j1", Type: "crawl", Payload: []byte(`{"url":"x"}`), State: job.Pending}, nil
	}})

	w := do(api, "GET", "/jobs/j1", "")

	if !strings.Contains(w.Body.String(), `"url":"x"`) {
		t.Errorf("payload was re-encoded rather than passed through: %s", w.Body)
	}
}

func TestWrongMethodIs405(t *testing.T) {
	api := newTestAPI(t, stubStore{})

	w := do(api, "DELETE", "/jobs", "")

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status is %d, want 405", w.Code)
	}
	if allow := w.Header().Get("Allow"); !strings.Contains(allow, "POST") {
		t.Errorf("Allow header is %q, want it to list POST", allow)
	}
}

// ?limit is the one number a client makes arbitrarily large, and an unbounded
// one is a query for every row in the table plus a response the server holds in
// memory — from a single GET. The bound is enforced by rejecting, not clamping:
// a client that asked for 10,000 rows and silently got 1,000 pages wrong.
//
// Both sides of the boundary, because a ceiling written as >= is a ceiling one
// short of its documented value.
func TestListBoundsTheLimit(t *testing.T) {
	tests := []struct {
		name      string
		limit     int
		wantCode  int
		wantLimit int // what the store should have been asked for; 0 means never called
	}{
		{"at the ceiling", MaxLimit, http.StatusOK, MaxLimit},
		{"one past it", MaxLimit + 1, http.StatusBadRequest, 0},
		{"absurd", 100_000_000, http.StatusBadRequest, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Filter
			api := newTestAPI(t, stubStore{list: func(_ context.Context, f Filter) ([]job.Job, error) {
				got = f
				return nil, nil
			}})

			w := do(api, "GET", fmt.Sprintf("/jobs?limit=%d", tt.limit), "")
			if w.Code != tt.wantCode {
				t.Fatalf("?limit=%d returned %d, want %d: %s", tt.limit, w.Code, tt.wantCode, w.Body)
			}
			if got.Limit != tt.wantLimit {
				t.Errorf("store received Limit=%d, want %d", got.Limit, tt.wantLimit)
			}
		})
	}
}
