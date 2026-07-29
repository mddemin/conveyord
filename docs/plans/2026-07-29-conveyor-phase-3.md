# Phase 3 — Postgres store and migrations

**Package:** `internal/store`
**Done when:** integration tests against a real Postgres pass with `go test -race ./...`, and `go test -short ./...` still runs in under a second.

---

## Global constraints

- `internal/job` still imports only the standard library. `internal/store` imports `internal/job`, never the reverse. Dependencies point inward.
- Every query takes a `context.Context` as its first argument and passes it through. No `context.TODO()` outside `TestMain`.
- No ORM, no query builder, no `database/sql`. Raw SQL through pgx.
- Integration tests skip under `-short`. A developer with no Docker running must still be able to run the domain tests.
- One container per package, started in `TestMain`. Not one per test.
- The database is not mocked. Ever. A mocked SQL test passes while the query is wrong.
- `gofmt -l .`, `go vet ./...`, `staticcheck -checks=all ./...` clean before every commit.
- Every exported identifier has a doc comment starting with its own name.
- Commit after every task, conventional commit messages.

---

## What you are building

```
internal/store/
    store.go          Store type, New, Enqueue, Get, List
    migrate.go        embed.FS + ~30-line runner
    migrations/
        0001_jobs.sql
    store_test.go     integration tests
    main_test.go      TestMain: container lifecycle
```

Three new dependencies, and only three:

```
github.com/jackc/pgx/v5
github.com/testcontainers/testcontainers-go
github.com/testcontainers/testcontainers-go/modules/postgres
```

---

## Task 1 — Test harness: a real Postgres from `TestMain`

**Goal:** one container, started once, shared by every test in the package, torn down at the end.

### Steps

1. `go get github.com/jackc/pgx/v5github.com/testcontainers/testcontainers-go github.com/testcontainers/testcontainers-go/modules/postgres`
2. Create `internal/store/main_test.go`.
3. Get `TestPoolIsAlive` passing.
4. `go test -short ./...` → the store package reports `[no tests to run]` or skips, and finishes instantly.
5. Commit.

### `main_test.go` — the harness

```go
package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// testPool is the connection pool every integration test in this package uses.
// It is nil under -short, where TestMain never starts a container.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	if testing.Short() {
		os.Exit(m.Run())
	}

	code, err := runWithPostgres(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "store integration setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// runWithPostgres exists so that every defer runs before os.Exit.
// os.Exit in TestMain skips deferred calls, which would leak the container.
func runWithPostgres(m *testing.M) (int, error) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("conveyor"),
		tcpostgres.WithUsername("conveyor"),
		tcpostgres.WithPassword("conveyor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return 0, fmt.Errorf("starting postgres: %w", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintln(os.Stderr, "terminating container:", err)
		}
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, fmt.Errorf("connection string: %w", err)
	}

	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("opening pool: %w", err)
	}
	defer testPool.Close()

	return m.Run(), nil
}

func TestPoolIsAlive(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	if err := testPool.Ping(t.Context()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
```

### Notes

- **`runWithPostgres` is not ceremony.** `os.Exit` does not run deferred functions. Put the container teardown in a function that returns normally, and `os.Exit` only on its result — otherwise a failing test suite leaves a Postgres container running on the developer's machine every time.
- **`WithOccurrence(2)`** is not superstition. The Postgres entrypoint starts the server once to run initdb scripts, shuts it down, and starts it again. Waiting for the first "ready" line connects to a server that is about to disappear.
- **`t.Context()`** (Go 1.24+) is cancelled when the test ends, so a hung query fails the test instead of blocking forever. Prefer it to `context.Background()` in every test from here on.
- The testcontainers API has moved more than once across versions. If `tcpostgres.Run` or `testcontainers.TerminateContainer` do not exist in the version `go get` resolved, `go doc github.com/testcontainers/testcontainers-go/modules/postgres` and adapt. This is the one block in this plan whose exact signatures I cannot guarantee.

### Done when

- `go test ./internal/store/` starts a container and passes.
- `go test -short ./...` passes in well under a second.
- Nothing is left running: `docker ps` is empty afterwards.

---

## Task 2 — Schema and the migration runner

**Goal:** SQL files embedded in the binary, applied in order, exactly once, recorded in a table.

### Steps

1. Write `internal/store/migrations/0001_jobs.sql`.
2. Write `internal/store/migrate.go`.
3. Get both tests passing.
4. Commit.

### The schema — `migrations/0001_jobs.sql`

```sql
CREATE TABLE jobs (
    id         TEXT PRIMARY KEY,
    type       TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    state      TEXT        NOT NULL,
    attempt    INT         NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_state_created_at_idx ON jobs (state, created_at);

CREATE TABLE job_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     TEXT        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    state      TEXT        NOT NULL,
    message    TEXT        NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX job_events_job_id_id_idx ON job_events (job_id, id);
```

Four decisions worth understanding before you type them:

- **`TIMESTAMPTZ`, never `TIMESTAMP`.** `TIMESTAMP` has no time zone, so it round-trips through pgx as a wall-clock reading with no instant attached. Every scheduling comparison you write in phase 4 would be subtly wrong for anyone not on UTC.
- **`BYTEA`, not `JSONB`.** `job.Job.Payload` is `[]byte` and the domain calls it "the executor's opaque input". `JSONB` would make Postgres parse, validate and reorder data the domain has promised not to interpret.
- **`state TEXT`, not an enum.** The state machine lives in `internal/job` and is already fully tested. A Postgres enum would be a second, out-of-sync copy of it — the same duplicated-knowledge problem you removed from `Terminal()`.
- **`ON DELETE CASCADE`** because a job's events have no meaning without the job.

### `migrate.go` — what to build

```go
//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies every embedded migration that has not been applied yet.
// It is safe to call on every startup and safe to call concurrently.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error
```

Requirements:

1. Create `schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())` if it does not exist.
2. Read the migration filenames from `migrationsFS`, **sorted**. The numeric prefix is what makes lexical sort the same as intended order — that is the whole reason for `0001_`.
3. Load the set of already-applied versions.
4. For each unapplied file, in order: run the file's SQL **and** insert its version row **in one transaction**. If the SQL half commits and the bookkeeping half does not, the next startup re-runs a `CREATE TABLE` and hard-fails forever.
5. Return early on the first error, wrapped with the filename. A migration failure you cannot locate is worse than no migration runner.

Design hints:

- `fs.ReadDir` returns entries already sorted by filename. Read `go doc io/fs.ReadDir` and confirm that before relying on it — if it is guaranteed, you do not need to sort; say so in a comment so the next reader does not add a redundant `slices.Sort`.
- `pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { ... })` commits on nil and rolls back on error. It removes the entire `defer tx.Rollback()` dance.
- `tx.Exec` accepts multiple statements in one string when there are no parameters, which is exactly the migration case.
- Concurrency ("safe to call concurrently") is real: in phase 12 two `conveyord` replicas start at once. `SELECT pg_advisory_xact_lock(<some constant>)` as the first statement inside the transaction serializes them. Pick a constant, name it, and comment why.

### `migrate_test.go`

```go
package store

import "testing"

func TestMigrateCreatesSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := t.Context()

	if err := Migrate(ctx, testPool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	for _, table := range []string{"jobs", "job_events", "schema_migrations"} {
		var exists bool
		err := testPool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`,
			table).Scan(&exists)
		if err != nil {
			t.Fatalf("checking for %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s was not created", table)
		}
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := t.Context()

	if err := Migrate(ctx, testPool); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := Migrate(ctx, testPool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}

	var applied int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("counting migrations: %v", err)
	}
	if applied != 1 {
		t.Errorf("schema_migrations has %d rows after two runs, want 1", applied)
	}
}
```

Note that these two tests share a database and run in file order, so the second finds the schema already migrated. That is deliberate — it is the same situation as a real restart. It also means **test isolation is now a problem you own**, which is Task 3.

### Done when

Both tests pass, and running `go test ./internal/store/` twice in a row produces the same result (the container is fresh each time, but the second `Migrate` call inside the run is the real assertion).

---

## Task 3 — `Store`, `New`, and `Enqueue`

**Goal:** a job goes into Postgres and the row is correct.

### Steps

1. Add the truncate helper to `main_test.go`.
2. Write `store.go` with `Store`, `New`, `Enqueue`.
3. Get the tests passing.
4. Commit.

### Test isolation helper — add to `main_test.go`

```go
// freshDB truncates every table and returns a Store ready for one test.
// Truncating is faster than recreating the schema and keeps tests independent
// of the order they run in.
func freshDB(t *testing.T) *Store {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx := t.Context()
	if err := Migrate(ctx, testPool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := testPool.Exec(ctx, `TRUNCATE jobs, job_events RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return New(testPool)
}
```

### `store.go` — signatures

```go
// Store persists jobs in Postgres. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool. The caller owns the pool and must close it.
func New(pool *pgxpool.Pool) *Store

// Enqueue inserts j as a new pending job and fills in its
// State, Attempt, CreatedAt and UpdatedAt from the stored row.
func (s *Store) Enqueue(ctx context.Context, j *job.Job) error
```

Requirements:

- `Enqueue` **writes `job.Pending` and attempt 0** regardless of what the caller put in those fields. Enqueueing is by definition the creation of a pending job; accepting a caller-supplied state would let an HTTP handler create a job that is already `dead`.
- The timestamps come from the database (`DEFAULT now()`), not from Go. One clock, not one per process. Use `RETURNING created_at, updated_at` and scan them back into `j` so the caller sees what was actually stored — this is why `Enqueue` takes a `*job.Job` rather than a value.
- The caller supplies `ID`. Generating it here would make `Enqueue` untestable for the duplicate case and would put an identifier scheme in the persistence layer.
- A duplicate ID must produce a distinguishable error, not a bare pgx error. Add `ErrDuplicate` to the store package — *not* to `internal/job`, because "primary key collision" is a storage concept the domain has no opinion about. Detect it with `errors.As` on `*pgconn.PgError` and a check for SQLSTATE `23505`; do not match on the message text.

### `store_test.go`

```go
package store

import (
	"errors"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

func TestEnqueueStoresAPendingJob(t *testing.T) {
	s := freshDB(t)
	before := time.Now()

	j := &job.Job{ID: "j1", Type: "resize", Payload: []byte(`{"w":100}`)}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if j.State != job.Pending {
		t.Errorf("State = %s, want %s", j.State, job.Pending)
	}
	if j.Attempt != 0 {
		t.Errorf("Attempt = %d, want 0", j.Attempt)
	}
	if j.CreatedAt.Before(before) {
		t.Errorf("CreatedAt = %v, not filled in from the database", j.CreatedAt)
	}
	if !j.UpdatedAt.Equal(j.CreatedAt) {
		t.Errorf("UpdatedAt = %v, want it to equal CreatedAt = %v", j.UpdatedAt, j.CreatedAt)
	}
}

func TestEnqueueIgnoresCallerSuppliedState(t *testing.T) {
	s := freshDB(t)

	j := &job.Job{ID: "j1", Type: "resize", State: job.Dead, Attempt: 99}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if j.State != job.Pending || j.Attempt != 0 {
		t.Errorf("stored job is {%s, %d}, want {%s, 0}", j.State, j.Attempt, job.Pending)
	}
}

func TestEnqueueRejectsDuplicateID(t *testing.T) {
	s := freshDB(t)
	ctx := t.Context()

	if err := s.Enqueue(ctx, &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}

	err := s.Enqueue(ctx, &job.Job{ID: "j1", Type: "other"})
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second Enqueue with the same ID returned %v, want ErrDuplicate", err)
	}
}
```

`TestEnqueueStoresAPendingJob` deliberately does **not** read the row back — that would test `Get` before it exists. It asserts only on what `Enqueue` promises to fill in. Task 4 is what proves the row is really there.

### Done when

All three tests pass and `Enqueue` contains no `time.Now()`.

---

## Task 4 — `Get` and `job.ErrNotFound`

**Goal:** a round trip, and the domain sentinel crossing the storage boundary.

### Signature

```go
// Get returns the job with the given id.
// It returns an error wrapping job.ErrNotFound if no such job exists.
func (s *Store) Get(ctx context.Context, id string) (*job.Job, error)
```

Requirements:

- Translate `pgx.ErrNoRows` into `job.ErrNotFound`, wrapped with `%w` and the id. This is the point of the sentinel: `internal/job` declares "not found" as a domain concept, `internal/store` produces it, and phase 5's HTTP layer turns it into a 404 without knowing pgx exists.
- Do not leak `pgx.ErrNoRows` upward. If it escapes, every layer above has to import pgx to check for it, and the inward-pointing dependency rule is dead.

### `store_test.go` — append

```go
func TestGetReturnsTheEnqueuedJob(t *testing.T) {
	s := freshDB(t)
	ctx := t.Context()

	want := &job.Job{ID: "j1", Type: "resize", Payload: []byte(`{"w":100}`)}
	if err := s.Enqueue(ctx, want); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := s.Get(ctx, "j1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Get returned\n %+v\nwant\n %+v", got, want)
	}
}

func TestGetUnknownIDIsNotFound(t *testing.T) {
	s := freshDB(t)

	_, err := s.Get(t.Context(), "nope")
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Get of an unknown id returned %v, want job.ErrNotFound", err)
	}
}
```

(`reflect.DeepEqual` for the same reason as phase 1: `job.Job` contains a `[]byte`, so it is not comparable with `==`.)

`TestGetReturnsTheEnqueuedJob` is where the timestamp round trip gets tested for real. If it fails on the time fields, read the failure carefully before changing anything — `time.Time` values that differ only in monotonic clock reading or location are a genuine and instructive trap, and `DeepEqual` will catch both.

### Done when

Both tests pass and neither `pgx` nor `pgconn` appears in any signature `store` exports.

---

## Task 5 — `List`

**Goal:** a filtered, bounded, deterministically ordered query.

### Signatures

```go
// Filter narrows a List query. The zero Filter matches every job.
type Filter struct {
	State job.State // empty matches any state
	Limit int       // non-positive means DefaultLimit
}

// DefaultLimit caps a List that did not ask for a limit.
const DefaultLimit = 100

// List returns jobs matching f, newest first.
func (s *Store) List(ctx context.Context, f Filter) ([]job.Job, error)
```

Requirements:

- **One SQL string, not two.** Resist `if f.State != "" { sql += " AND state = $1" }` — string-building queries is how injection bugs and unreadable code both start. The trick: `WHERE ($1 = '' OR state = $1)` passes the filter as a parameter and lets Postgres short-circuit. Read the resulting query plan with `EXPLAIN` if you are curious whether the index still gets used; the answer is instructive either way.
- **Always `ORDER BY`.** A `SELECT` without one has no defined row order, and a `LIMIT` without one returns an arbitrary subset that will happen to look correct in your tests and go wrong in production. `ORDER BY created_at DESC, id DESC` — the `id` tiebreak matters because two jobs enqueued in the same transaction share a timestamp.
- **Always `LIMIT`.** An unbounded `List` is a memory exhaustion bug with a table scan attached.
- Return `[]job.Job` (values, not pointers) and a nil slice when nothing matches. Returning `[]*job.Job` here would hand out pointers into a result set with no owner.

Design hint: `pgx.CollectRows(rows, pgx.RowToStructByName[job.Job])` does the whole scan loop in one line — but only if the column names match the field names case-insensitively, which `created_at` → `CreatedAt` does not. Try it, read the error, then decide between a `db` struct tag, `RowToStructByPos`, or a plain `for rows.Next()` loop. All three are defensible; pick one and be able to say why.

### `store_test.go` — append

```go
func TestListFiltersAndOrders(t *testing.T) {
	s := freshDB(t)
	ctx := t.Context()

	for _, id := range []string{"j1", "j2", "j3"} {
		if err := s.Enqueue(ctx, &job.Job{ID: id, Type: "resize"}); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	all, err := s.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("List returned %d jobs, want 3", len(all))
	}

	pending, err := s.List(ctx, Filter{State: job.Pending})
	if err != nil {
		t.Fatalf("List pending: %v", err)
	}
	if len(pending) != 3 {
		t.Errorf("List by state returned %d jobs, want 3", len(pending))
	}

	dead, err := s.List(ctx, Filter{State: job.Dead})
	if err != nil {
		t.Fatalf("List dead: %v", err)
	}
	if len(dead) != 0 {
		t.Errorf("List of an unused state returned %d jobs, want 0", len(dead))
	}
}

func TestListRespectsLimit(t *testing.T) {
	s := freshDB(t)
	ctx := t.Context()

	for _, id := range []string{"j1", "j2", "j3"} {
		if err := s.Enqueue(ctx, &job.Job{ID: id, Type: "resize"}); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	got, err := s.List(ctx, Filter{Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("List with Limit 2 returned %d jobs", len(got))
	}
}
```

### Done when

Both tests pass, `List` builds no SQL by concatenation, and its query has both `ORDER BY` and `LIMIT`.

---

## Task 6 — `job_events` and the transactional enqueue

**Goal:** the audit trail, written atomically with the job it describes.

### Signatures

```go
// Event is one recorded step in a job's lifecycle.
type Event struct {
	JobID     string
	State     job.State
	Message   string
	CreatedAt time.Time
}

// Events returns every recorded event for a job, oldest first.
func (s *Store) Events(ctx context.Context, jobID string) ([]Event, error)
```

Requirements:

- `Enqueue` now writes **two** rows — the job and its first event — inside one transaction. If the process dies between them, neither exists. This is the first place in the project where correctness depends on a transaction boundary, and it is worth pausing on: what would a caller see if you wrote them as two separate `Exec` calls and the second failed?
- The event table's `id` is a `BIGSERIAL`, so ordering by it gives insertion order even when two events share a timestamp. Order by `id`, not `created_at`.
- `Events` on an unknown job returns an empty slice and no error, not `ErrNotFound`. "This job has no events" and "this job does not exist" are different questions; `Get` answers the second one.

### `store_test.go` — append

```go
func TestEnqueueRecordsAnEvent(t *testing.T) {
	s := freshDB(t)
	ctx := t.Context()

	if err := s.Enqueue(ctx, &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	events, err := s.Events(ctx, "j1")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("Events returned %d events after Enqueue, want 1", len(events))
	}
	if events[0].State != job.Pending {
		t.Errorf("first event State = %s, want %s", events[0].State, job.Pending)
	}
	if events[0].JobID != "j1" {
		t.Errorf("first event JobID = %s, want j1", events[0].JobID)
	}
}

func TestFailedEnqueueWritesNothing(t *testing.T) {
	s := freshDB(t)
	ctx := t.Context()

	if err := s.Enqueue(ctx, &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	if err := s.Enqueue(ctx, &job.Job{ID: "j1", Type: "other"}); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second Enqueue returned %v, want ErrDuplicate", err)
	}

	events, err := s.Events(ctx, "j1")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("job has %d events after a rejected duplicate Enqueue, want 1", len(events))
	}
}

func TestEventsOfUnknownJobIsEmpty(t *testing.T) {
	s := freshDB(t)

	events, err := s.Events(t.Context(), "nope")
	if err != nil {
		t.Fatalf("Events of an unknown job: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("got %d events, want 0", len(events))
	}
}
```

`TestFailedEnqueueWritesNothing` is the one that actually proves the transaction. Write it, then deliberately break the atomicity — do the two inserts without a transaction — and watch it fail. Put it back afterwards. A test you have never seen fail is a test you do not know works.

### Done when

All three pass, and `Enqueue` opens exactly one transaction.

---

## Phase review checklist

Before asking for the phase review, run:

```bash
gofmt -l .
go vet ./...
staticcheck -checks=all ./...
go test -race ./...
go test -short ./...                                   # fast, no Docker
go list -deps ./internal/job | grep -v '^github.com/mddemin/conveyor' | grep '\.'   # still stdlib only
go test -cover ./internal/store/
docker ps                                              # nothing left running
```

And check by eye:

- No `context.TODO()` outside `TestMain`.
- No `pgx` or `pgconn` type in any exported signature of `internal/store`.
- No SQL built by string concatenation.
- Every `SELECT` that can return more than one row has `ORDER BY` and `LIMIT`.
- `internal/job` unchanged this phase. If it needed changing, that is worth discussing — it usually means a storage concern leaked inward.

---

## What phase 4 will need from this

Leasing adds `lease_expires_at` and `leased_by` to `jobs`, in migration `0002`. Do not add those columns now — the point of the migration runner is that the schema can grow, and adding an unused column today is the same speculative work as an interface with one implementation. If you find yourself wanting to, that is the instinct this project is trying to retrain.
