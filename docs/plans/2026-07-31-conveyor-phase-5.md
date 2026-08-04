# Phase 5 — HTTP API

**Package:** `internal/httpapi` (new), `cmd/conveyord` (new)
**Done when:** a job can be enqueued over HTTP and read back, and the server logs one
structured line per request with a request ID that a client can correlate against.

Four phases of this project have been *libraries*. Nothing has had a `main`. This is the
phase where Conveyor becomes a program you can `curl`, and the phase where the layering
rule from the spec — dependencies point inward — stops being theoretical. `internal/job`
still knows nothing about HTTP after this phase. If a `404` appears anywhere below
`internal/httpapi`, the phase failed.

The other thing this phase teaches is that **`net/http` is enough**. No chi, no gin, no
echo. Go 1.22 put method-and-wildcard routing in `ServeMux`, and middleware was never
more than `func(http.Handler) http.Handler`. The entire framework is one type and one
function signature.

---

## Global constraints

Carried forward from phase 4, plus:

- `internal/httpapi` imports **no third-party HTTP library**. `net/http`, `encoding/json`, `log/slog`, `errors`, `context`. That's the phase.
- Domain errors are translated to status codes in **exactly one function**. If two handlers both write `http.StatusNotFound`, they are wrong.
- No handler calls `panic` deliberately, and no handler ignores an error from `store`.
- Every response with a body sets `Content-Type` before `WriteHeader`. After `WriteHeader` it is too late and the header is silently dropped.
- Tests use `httptest`. No listening on a real port except in the one end-to-end test.
- No `time.Sleep`.

---

## The problem being solved

`store.Store` has eight methods and returns Go values and wrapped sentinel errors. HTTP
speaks status codes, JSON, and headers. The whole phase is the seam between those two
worlds, and there are three ways to get it wrong:

1. **Leaking the domain onto the wire.** `json.Marshal(job.Job{})` compiles and produces
   garbage: `Payload []byte` becomes a base64 string, and every future field you add to
   the domain silently becomes public API. The wire format is a separate type that you
   change deliberately.

2. **Leaking the wire into the domain.** The tempting fix for (1) is `json:"..."` tags on
   `job.Job`. That puts an encoding concern in the package the spec says has zero
   non-stdlib deps and no concept of a 404. Tags are not an import, so the compiler will
   not catch it — this one is caught in review only.

3. **Scattering the error mapping.** `if errors.Is(err, job.ErrNotFound) { 404 }` copied
   into three handlers means the fourth handler forgets, returns 500 for a missing job,
   and nobody notices until a client retries a 500 forever.

---

## Task 1 — The wire types and the JSON helpers

**Goal:** decide what Conveyor's HTTP API *looks like* before writing a single handler.

### Steps

1. Create `internal/httpapi/api.go` with the request and response types.
2. Create `internal/httpapi/json.go` with `writeJSON` and `decodeJSON`.
3. Test them with no HTTP server involved.
4. Commit.

### The wire types

```go
// enqueueRequest is the body of POST /jobs.
type enqueueRequest struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// jobResponse is a job as clients see it. It is deliberately not job.Job:
// the domain type is free to change without breaking the API.
type jobResponse struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	State     string          `json:"state"`
	Attempt   int             `json:"attempt"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// errorResponse is the body of every 4xx and 5xx.
type errorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"request_id,omitempty"`
}
```

`json.RawMessage` rather than `[]byte` is the whole point of this task. `[]byte` marshals
to base64; `json.RawMessage` passes the bytes through verbatim, which is what a JSON
payload column wants. It also validates on the way in — an ill-formed payload fails at
`decodeJSON`, not at the database.

`ID` being optional is a deliberate design choice worth understanding: the client may
supply one to make enqueue **idempotent** (a retried POST with the same id gets a clean
409 instead of a duplicate job), and the server generates one otherwise. This is the only
reason `store.ErrDuplicate` exists.

### `decodeJSON` requirements

A generic helper, `func decodeJSON[T any](w http.ResponseWriter, r *http.Request) (T, error)`.
It must:

- Cap the body with `http.MaxBytesReader`. Pick a constant — 1 MiB is generous for this API. Without it, a client streaming an infinite body allocates until the process dies. This is not paranoia; it is the single most common Go HTTP vulnerability.
- Reject unknown fields (`Decoder.DisallowUnknownFields`). A client sending `{"typ": "resize"}` should get a 400, not a job with an empty type.
- Reject a second JSON value in the body. `Decode` stops at the first one; a second `Decode` returning anything but `io.EOF` means the client sent `{...}{...}`.
- Return errors that are safe to show a client. `*json.SyntaxError` and `*json.UnmarshalTypeError` carry useful, non-sensitive detail — use `errors.As` and build a message naming the offending field. A bare `err.Error()` from the decoder can leak internal type names.

### `writeJSON` requirements

`func writeJSON(w http.ResponseWriter, status int, v any)`. Set `Content-Type`, then
`WriteHeader(status)`, then encode. In that order, always.

There is one genuinely awkward case and you should decide what to do about it rather than
discover it later: if encoding fails **after** `WriteHeader`, the status is already sent
and the response body is truncated garbage. You cannot turn it into a 500. The realistic
options are to log it and move on, or to encode into a buffer first and only then write.
Pick one and say why in a comment.

### Tests

```go
func TestDecodeJSONRejectsUnknownFields(t *testing.T) {
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"type":"resize","nope":1}`))
	_, err := decodeJSON[enqueueRequest](httptest.NewRecorder(), r)
	if err == nil {
		t.Fatal("decodeJSON accepted an unknown field")
	}
}

func TestDecodeJSONRejectsTrailingGarbage(t *testing.T) {
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"type":"resize"}{"type":"other"}`))
	_, err := decodeJSON[enqueueRequest](httptest.NewRecorder(), r)
	if err == nil {
		t.Fatal("decodeJSON accepted two JSON values in one body")
	}
}

func TestDecodeJSONRejectsAnOversizedBody(t *testing.T) {
	body := `{"type":"resize","payload":` + strings.Repeat("0", maxBodyBytes) + `}`
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(body))
	_, err := decodeJSON[enqueueRequest](httptest.NewRecorder(), r)
	if err == nil {
		t.Fatal("decodeJSON accepted a body over the limit")
	}
}

func TestDecodeJSONPassesPayloadThroughVerbatim(t *testing.T) {
	const payload = `{"url":"https://example.com","depth":3}`
	r := httptest.NewRequest("POST", "/jobs", strings.NewReader(
		`{"type":"crawl","payload":`+payload+`}`))

	got, err := decodeJSON[enqueueRequest](httptest.NewRecorder(), r)
	if err != nil {
		t.Fatalf("decodeJSON: %v", err)
	}
	// Not base64, not re-serialized with different key order — the same bytes.
	if string(got.Payload) != payload {
		t.Errorf("payload round-tripped as %s, want %s", got.Payload, payload)
	}
}

func TestWriteJSONSetsContentTypeAndStatus(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusCreated, jobResponse{ID: "j1", State: "pending"})

	if w.Code != http.StatusCreated {
		t.Errorf("status is %d, want %d", w.Code, http.StatusCreated)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type is %q, want application/json", ct)
	}
	var got jobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("response body is not JSON: %v", err)
	}
	if got.ID != "j1" {
		t.Errorf("body has id %q, want j1", got.ID)
	}
}
```

---

## Task 2 — The API skeleton and error translation

**Goal:** the type every handler hangs off, plus one function, tested exhaustively, that
every handler routes through.

### Steps

1. Decide the store seam (below). This is a design decision, not a mechanical step — make it deliberately.
2. Add `internal/httpapi/server.go` with `Store`, `API`, and `New`. No routes and no handlers yet — those are Task 3.
3. Add `writeError` to `internal/httpapi/errors.go`.
4. Test the mapping directly — no handlers, no routing.
5. Commit.

### The store seam — read this before writing code

The spec says: *interfaces only where a test needs a seam*, and *the consumer declares the
interface at the point of use*. Both apply here.

You need a seam because the 500 path below is otherwise untestable — there is no way to
make a real `*store.Store` return "connection refused" on demand. So `internal/httpapi`
declares its own small interface, naming only what it uses:

```go
// Store is the slice of storage this package needs. Declared here, not in
// internal/store, so the API depends on a behaviour rather than on a struct.
type Store interface {
	Enqueue(ctx context.Context, j *job.Job) error
	Get(ctx context.Context, id string) (*job.Job, error)
	List(ctx context.Context, f store.Filter) ([]job.Job, error)
}
```

Three methods, not eight. `*store.Store` satisfies it without knowing it exists, which is
the Go idiom and the reason Go interfaces are structural. A `StoreInterface` mirroring all
eight methods would be a review finding.

The cost is honest and you should note it: `httpapi` now imports `store` for `Filter`, so
the packages are still coupled, just less so. Whether `Filter` should move is a real
question — do not spend the phase on it, but have an opinion by the end.

`API` holds that interface and a `*slog.Logger`; `New` takes both and returns `*API` —
named `API`, not `Server`, so that `cmd/conveyord` can sit it next to an `*http.Server`
without the reader having to work out which one listens on a port. The logger is a
field rather than a package global because the 500 path is the only place in the API that
knows the real error, and a test needs to read what it wrote — `newTestAPI` will hand
it a logger writing to a `bytes.Buffer`.

### Requirements

```go
// writeError translates a domain or storage error into a status code and a
// client-safe message. It is the only place in the program that decides what
// a job.ErrNotFound looks like over HTTP.
func (s *API) writeError(w http.ResponseWriter, r *http.Request, err error)
```

The mapping:

| Error | Status | Body message |
|---|---|---|
| `job.ErrNotFound` | 404 | the error's message |
| `store.ErrDuplicate` | 409 | the error's message |
| `job.ErrInvalidTransition` | 409 | the error's message |
| `errInvalidRequest` (yours, for validation and decode failures) | 400 | the error's message |
| anything else | 500 | **a fixed string** |

That last row is the one that matters. `store` errors are wrapped with the SQL operation
and the job id; a raw `pgconn.PgError` can carry table names, column names, and constraint
names. Returning `err.Error()` on a 500 hands your schema to anyone who can trigger one.
Log the real error with the request ID; return `"internal error"` and the request ID to
the client. The request ID is what makes that debuggable instead of infuriating.

Note `job.ErrInvalidTransition` → 409 rather than 400: the request was well-formed, the
*state* made it impossible. That distinction is real and clients act on it differently.

### Tests

```go
func TestWriteErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"not found", fmt.Errorf("job %s: %w", "j1", job.ErrNotFound), http.StatusNotFound},
		{"duplicate", fmt.Errorf("job %s: %w", "j1", store.ErrDuplicate), http.StatusConflict},
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

// A 500 must not tell the client what the database is called.
func TestWriteErrorHidesInternalDetail(t *testing.T) {
	s := newTestAPI(t, nil)
	w := httptest.NewRecorder()
	leaky := errors.New(`duplicate key value violates unique constraint "jobs_pkey"`)

	s.writeError(w, httptest.NewRequest("GET", "/", nil), leaky)

	if strings.Contains(w.Body.String(), "jobs_pkey") {
		t.Errorf("500 body leaked internal detail: %s", w.Body)
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
```

---

## Task 3 — Handlers and routing

**Goal:** `POST /jobs`, `GET /jobs/{id}`, `GET /jobs`.

### Steps

1. Add `routes` to `internal/httpapi/server.go` and make `API` an `http.Handler`.
2. Add the three handlers.
3. Commit.

### Routing

Go 1.22 `ServeMux` patterns. `mux.HandleFunc("POST /jobs", ...)` and
`mux.HandleFunc("GET /jobs/{id}", ...)`. Three things to know:

- A method in the pattern means the mux returns **405** with a correct `Allow` header for the wrong method, free.
- `r.PathValue("id")` reads the wildcard. It returns `""` for an unknown name, so a typo is a silent bug rather than a compile error — this is the one place the new router is weaker than a code-generated one.
- `GET /jobs` and `GET /jobs/{id}` do not conflict. `GET /jobs/` (trailing slash) would match everything below `/jobs/`, which is not what you want. Precedence is "most specific wins", not "first registered".

### Handler requirements

**`POST /jobs`** → 201, `Location: /jobs/{id}`, the created job as the body.
- Validate: `Type` non-empty. That is the only required field.
- Generate an id when the request omits one. `crypto/rand` plus `hex`, 16 bytes. Not `math/rand`, not a counter, not a timestamp — ids are client-visible and guessable ids are a real problem the moment there is any authorization at all. Do not add a UUID dependency for this; `hex.EncodeToString(b)` on 16 random bytes is one line and is a UUID in everything but formatting.
- A client-supplied duplicate id must surface as 409, which means `store.ErrDuplicate` must survive to `writeError` unmangled.

**`GET /jobs/{id}`** → 200 or 404.

**`GET /jobs`** → 200 with a JSON array.
- Query params: `?state=` and `?limit=`.
- An unknown state is a **400**, not an empty list. `?state=pnding` returning `[]` is the kind of thing that costs someone an afternoon.
- A non-integer or negative `limit` is a 400. A missing one is `store.DefaultLimit`.
- An empty result is `[]`, not `null`. `json.Marshal` on a nil slice produces `null`, and every JavaScript client in the world then does `null.length`. This is the single most common Go JSON API bug.

### Tests

Use a stub store for everything except the one integration test in Task 5.

```go
// stubStore lets a test choose exactly what storage does, including fail.
type stubStore struct {
	enqueue func(ctx context.Context, j *job.Job) error
	get     func(ctx context.Context, id string) (*job.Job, error)
	list    func(ctx context.Context, f store.Filter) ([]job.Job, error)
}

func (s stubStore) Enqueue(ctx context.Context, j *job.Job) error { return s.enqueue(ctx, j) }
func (s stubStore) Get(ctx context.Context, id string) (*job.Job, error) { return s.get(ctx, id) }
func (s stubStore) List(ctx context.Context, f store.Filter) ([]job.Job, error) { return s.list(ctx, f) }

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

func TestEnqueueRejectsADuplicate(t *testing.T) {
	api := newTestAPI(t, stubStore{enqueue: func(context.Context, *job.Job) error {
		return fmt.Errorf("job id x: %w", store.ErrDuplicate)
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
	api := newTestAPI(t, stubStore{list: func(context.Context, store.Filter) ([]job.Job, error) {
		t.Error("store was queried with an invalid state")
		return nil, nil
	}})

	w := do(api, "GET", "/jobs?state=pnding", "")

	if w.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want 400 for an unknown state", w.Code)
	}
}

// A nil slice marshals to null. Every client then crashes on .length.
func TestListReturnsAnEmptyArrayNotNull(t *testing.T) {
	api := newTestAPI(t, stubStore{list: func(context.Context, store.Filter) ([]job.Job, error) {
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
```

`do(api, method, path, body)` is yours to write, alongside the `newTestAPI` you already
have from Task 2 — both trivial, both worth having before the tenth test rather than after.

---

## Task 4 — The middleware chain

**Goal:** request ID, structured logging, panic recovery, timeout. Four functions, no
framework, and one landmine that will detonate in phase 6 if you plant it now.

### Steps

1. Add `internal/httpapi/middleware.go`.
2. Chain them in `New`.
3. Commit.

### The shape

Middleware is `func(http.Handler) http.Handler` and nothing else. A chain is:

```go
func chain(h http.Handler, mw ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mw) - 1; i >= 0; i-- {
		h = mw[i](h)
	}
	return h
}
```

Iterating backwards is what makes `chain(mux, a, b, c)` run `a` outermost. Get this
wrong and the order silently inverts.

### Order matters, and here is why

`requestID → logging → recovery → timeout → mux`

- **requestID first** so every subsequent layer, including the log line and the error body, can name it.
- **logging outside recovery** so a panic still produces a log line. Invert these and your 500s are invisible.
- **timeout innermost** so the timeout applies to the handler, not to the logging.

Justify your chosen order in a comment. An order that "works" is not the same as an order
that is right, and the difference only shows up during an incident.

### requestID

Accept an inbound `X-Request-ID` if present (a load balancer or an upstream service may
have set one — discarding it breaks distributed tracing before you have any), otherwise
generate. Put it in the request context under an **unexported key type**:

```go
type ctxKey int
const requestIDKey ctxKey = 0
```

A `string` key is a data race waiting to happen — any other package using the same string
collides. The unexported type makes collision impossible. Echo it back as a response
header.

One thing to be careful about: an inbound header is attacker-controlled. It ends up in
your logs and in a response header. Cap its length and reject anything non-printable, or
you have a log-injection bug.

### Logging

`log/slog`, injected — `New` takes a `*slog.Logger`. Log one line per request *after* it
completes: method, path, status, bytes written, duration, request id.

To know the status you need to wrap `http.ResponseWriter`, and this is the landmine:

```go
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
}
```

Embedding gives you `Write`, `Header`, and `WriteHeader` for free, and you override the
last two to record. But **embedding also throws away every optional interface the real
writer implements**. `http.Flusher` in particular. Your wrapper compiles, works perfectly
in phase 5, and silently breaks every SSE stream in phase 6, because
`w.(http.Flusher)` now fails and events buffer forever.

Handle it now:

```go
// Flush forwards to the wrapped writer. Without this, embedding hides the
// real writer's http.Flusher and SSE (phase 6) buffers forever.
func (rec *recorder) Flush() {
	if f, ok := rec.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
```

Also handle the handler that never calls `WriteHeader`: `net/http` sends 200 implicitly,
so a `recorder` initialized to zero logs `status=0`. Initialize to
`http.StatusOK`.

### Recovery

`defer func() { if v := recover(); v != nil { ... } }()`. Log the value **and**
`debug.Stack()`, then write a 500 through `writeError`.

Two details that separate a real recovery middleware from a tutorial one:

- Re-panic on `http.ErrAbortHandler`. The stdlib uses that sentinel to mean "abort this connection deliberately, don't log it." Swallowing it turns an intentional abort into a fake 500 in your metrics.
- If the handler already wrote a status, you cannot write a 500 on top of it. You get a `superfluous WriteHeader` warning and the client keeps the truncated 200. Your `recorder` already knows whether a status was written — use it.

Note that `net/http` recovers panics itself, per connection. It just closes the connection
without a response and logs to the server's error log. That is worse than useless to a
client, which is why you write your own.

### Timeout

`http.TimeoutHandler` from the stdlib. Do not hand-roll it.

But know what it does: it runs the handler in a **new goroutine**, and on expiry writes a
503 and its own body. Which means it **buffers the entire response** — and a buffered
response is incompatible with server-sent events. In phase 6 this middleware must not
wrap the SSE routes.

Decide now whether to apply it globally and split later, or to apply it per-route
immediately. Either is defensible; leave a marker:

```go
// ponytail: global timeout; SSE routes in phase 6 must be excluded — TimeoutHandler
// buffers the response body and a buffered stream never streams.
```

### Tests

```go
func TestRequestIDIsGeneratedAndEchoed(t *testing.T) {
	api := newTestAPI(t, stubStore{list: emptyList})

	w := do(api, "GET", "/jobs", "")

	if w.Header().Get("X-Request-ID") == "" {
		t.Error("no X-Request-ID on the response")
	}
}

func TestRequestIDIsInheritedFromTheClient(t *testing.T) {
	api := newTestAPI(t, stubStore{list: emptyList})

	r := httptest.NewRequest("GET", "/jobs", nil)
	r.Header.Set("X-Request-ID", "upstream-123")
	w := httptest.NewRecorder()
	api.ServeHTTP(w, r)

	if got := w.Header().Get("X-Request-ID"); got != "upstream-123" {
		t.Errorf("X-Request-ID is %q, want the client's upstream-123", got)
	}
}

func TestPanicBecomes500AndIsLogged(t *testing.T) {
	var logged bytes.Buffer
	api := newTestAPIWithLogger(t, stubStore{
		list: func(context.Context, store.Filter) ([]job.Job, error) { panic("boom") },
	}, slog.New(slog.NewJSONHandler(&logged, nil)))

	w := do(api, "GET", "/jobs", "")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status after a panic is %d, want 500", w.Code)
	}
	if !strings.Contains(logged.String(), "boom") {
		t.Errorf("panic value never reached the log: %s", logged.String())
	}
	// The client must not be told what panicked.
	if strings.Contains(w.Body.String(), "boom") {
		t.Errorf("panic value leaked to the client: %s", w.Body)
	}
}

// A panic must not take the process down with it.
func TestServerSurvivesAPanic(t *testing.T) {
	api := newTestAPI(t, stubStore{
		list: func(context.Context, store.Filter) ([]job.Job, error) { panic("boom") },
	})

	do(api, "GET", "/jobs", "")
	w := do(api, "GET", "/jobs", "")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("second request after a panic got %d", w.Code)
	}
}

func TestRequestIsLoggedOnce(t *testing.T) {
	var logged bytes.Buffer
	api := newTestAPIWithLogger(t, stubStore{list: emptyList},
		slog.New(slog.NewJSONHandler(&logged, nil)))

	do(api, "GET", "/jobs", "")

	if n := strings.Count(logged.String(), "\n"); n != 1 {
		t.Errorf("one request produced %d log lines, want 1: %s", n, logged.String())
	}
	for _, want := range []string{`"status":200`, `"method":"GET"`, `"path":"/jobs"`} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("log line is missing %s: %s", want, logged.String())
		}
	}
}

// The wrapper must not hide http.Flusher — phase 6 depends on it.
func TestResponseWriterStillFlushes(t *testing.T) {
	var flushable bool
	api := newTestAPI(t, stubStore{list: func(context.Context, store.Filter) ([]job.Job, error) {
		return nil, nil
	}})
	_ = api

	h := logging(slog.New(slog.NewJSONHandler(io.Discard, nil)))(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, flushable = w.(http.Flusher)
		}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	if !flushable {
		t.Error("the logging wrapper hides http.Flusher; SSE will not stream in phase 6")
	}
}
```

---

## Task 5 — `cmd/conveyord` and one end-to-end test

**Goal:** a binary. Then prove the whole stack works against a real database.

### Steps

1. Write `cmd/conveyord/main.go`.
2. Write the one integration test that uses a real `*store.Store`.
3. `curl` it yourself. Enqueue a job, read it back.
4. Commit.

### `main.go` requirements

Wiring and flags only, per the spec's package layout. Roughly:

- Flags: `-addr` (default `:8080`), `-dsn` (default from `CONVEYOR_DSN`).
- Open the pool, `Ping` it, run `Migrate`. Failing to reach the database is a startup failure — exit non-zero with a clear message, do not serve a broken API.
- Build the `*slog.Logger` here. It is a wiring concern; `httpapi` receives one.
- `&http.Server{Addr: ..., Handler: ..., ReadHeaderTimeout: ...}`.

**Set `ReadHeaderTimeout`.** `http.Server`'s zero value has no timeouts at all, and a
client that opens a connection and sends nothing occupies a goroutine forever — Slowloris,
and it needs no tooling. `ReadHeaderTimeout` is the one that matters; also consider
`ReadTimeout`, `WriteTimeout`, `IdleTimeout` and know why `WriteTimeout` is hostile to SSE.

Graceful shutdown is **phase 7's** subject. A `main` that exits on SIGINT without draining
is fine here. Do not build the errgroup shutdown tree yet; you will do it properly, once,
with `goleak` watching.

`main` should be short. If it grows past about 50 lines, logic has leaked into it that
belongs in a package.

### The integration test

Everything above used a stub. Exactly one test proves the real thing works, and it lives
in `internal/httpapi` with a build path to a container, or in the store package's
container-sharing style — your call, but do not spin up a second container per package if
you can reuse the pattern from `internal/store/main_test.go`.

```go
func TestEnqueueAndReadBackOverHTTP(t *testing.T) {
	s := freshDB(t) // the real *store.Store
	api := httpapi.New(s, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ts := httptest.NewServer(api)
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	body := strings.NewReader(`{"type":"resize","payload":{"w":100}}`)
	req, _ := http.NewRequestWithContext(ctx, "POST", ts.URL+"/jobs", body)
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
	if created.State != "pending" {
		t.Errorf("created job is %s, want pending", created.State)
	}

	// Read it back through the API, not through the store — the point is the
	// round trip, including the payload surviving as JSON rather than base64.
	req, _ = http.NewRequestWithContext(ctx, "GET", ts.URL+"/jobs/"+created.ID, nil)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /jobs/{id}: %v", err)
	}
	defer resp2.Body.Close()

	var got jobResponse
	if err := json.NewDecoder(resp2.Body).Decode(&got); err != nil {
		t.Fatalf("decoding the fetched job: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("read back id %s, want %s", got.ID, created.ID)
	}
	if string(got.Payload) != `{"w":100}` {
		t.Errorf("payload round-tripped as %s, want the bytes the client sent", got.Payload)
	}
}
```

That assertion is byte-exact because `payload` is `BYTEA` in `0001` — Postgres stores the
bytes and hands them back unchanged. Had you chosen `JSONB`, it would normalize whitespace
and key order and this test would need loosening.

Which raises a design question worth answering rather than drifting past. `BYTEA` says the
payload is **opaque bytes**, matching `job.Job`'s comment: *the executor's opaque input*.
But Task 1 makes the API accept `json.RawMessage`, which means the *API* requires JSON
while *storage* does not. That is a real inconsistency, and there are three honest
positions:

- **Payloads are JSON, everywhere.** Then the column should be `JSONB` and you gain `WHERE payload->>'url' = ...` for free. Costs byte-exactness and forbids a binary payload forever.
- **Payloads are opaque bytes.** Then the API should accept a base64 string, not `json.RawMessage`, and `[]byte` marshalling is correct after all.
- **Payloads are opaque to the store and JSON at the edge.** `BYTEA` plus `json.RawMessage` — what the plan above specifies. Defensible: storage stays agnostic, and the JSON constraint is an API-level decision one transport makes and gRPC in phase 8 need not.

The plan takes the third. Understand why before you accept it, because phase 8 will ask the
same question again with protobuf `bytes` and you want a consistent answer.

---

## Phase review checklist

```bash
gofmt -l .
go vet ./...
staticcheck -checks=all ./...
go test -race -count=2 ./...
go list -deps ./internal/job | grep -v '^github.com/mddemin/conveyor' | grep '\.'
go list -deps ./internal/httpapi | grep -v '^github.com/mddemin/conveyor' | grep '\.' | grep -v '^golang.org/x/'
go build ./cmd/...
```

The second-to-last command should print **nothing**. `internal/httpapi` has no
third-party dependencies; if it does, a framework got in.

By eye:

- `internal/job` unchanged. No `json` tags, no status codes, no `net/http` import. This is the invariant the whole layout exists to protect.
- Exactly one function writes a status code for a domain error. Grep for `StatusNotFound` — one hit.
- No 500 response body contains anything derived from a `store` or `pgx` error.
- Every `ResponseWriter` wrapper forwards `Flush`.
- `http.Server` has a `ReadHeaderTimeout`.
- The middleware chain's order is commented with a reason, not just listed.
- `main` is wiring only, under ~50 lines.

Then, by hand, because a test passing is not the same as a program working:

```bash
go run ./cmd/conveyord &
curl -sS -X POST localhost:8080/jobs -d '{"type":"resize","payload":{"w":100}}' | tee /tmp/j
curl -sS localhost:8080/jobs/$(jq -r .id /tmp/j)
curl -sS 'localhost:8080/jobs?state=pending'
curl -sS -i localhost:8080/jobs/nope        # 404, with a request id
curl -sS -i -X DELETE localhost:8080/jobs   # 405, with Allow
```

**Done when** that sequence behaves and the log shows one structured line per request
carrying the same request id the client saw.
