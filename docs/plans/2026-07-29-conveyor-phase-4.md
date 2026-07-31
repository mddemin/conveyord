# Phase 4 — Leasing

**Package:** `internal/store` (extends phase 3)
**Done when:** ten concurrent leasers hand out every job exactly once, proven by a `-race` test with no `time.Sleep`.

This is the phase where the project stops being a CRUD app. Everything before it was
storage; leasing is the mechanism that makes a queue a queue, and `SELECT ... FOR
UPDATE SKIP LOCKED` is the single most valuable thing in this codebase to actually
understand rather than copy.

---

## Global constraints

Unchanged from phase 3, plus:

- Every new query is a named `const`, next to the ones it belongs with.
- Every error crossing the package boundary names the operation and its key. The rule you converged on at the end of phase 3.
- Every multi-row `SELECT` has `ORDER BY` and `LIMIT`.
- Wrapping only inside `if err != nil`.
- The lease clock is the **database's**, never Go's. `now()` in SQL, not `time.Now()` in the driver — two `conveyord` replicas do not share a wall clock.

---

## The problem being solved

Two `conveyord` replicas poll for work at the same instant. Both run `SELECT ... WHERE
state = 'pending' LIMIT 10`. Both get the same ten rows. Both dispatch them. Every job
runs twice.

The naive fixes all fail in an instructive way:

- **`SELECT` then `UPDATE`** — the gap between them is the race. Nothing closes it.
- **`SELECT ... FOR UPDATE`** — correct, but replica B *blocks* waiting for A's rows instead of taking different ones. Throughput collapses to one leaser.
- **`SERIALIZABLE` isolation** — correct, but B's transaction aborts and retries. Under load you get a retry storm.
- **An application-level mutex** — works in one process. You have two.

`FOR UPDATE SKIP LOCKED` is the answer: B's query *skips* rows A has locked and returns
the next ten instead. No blocking, no aborts, no double dispatch. It is the reason
Postgres is a viable queue at all, and it's four words.

---

## Task 1 — Migration `0002`: lease columns

### Steps

1. Write `internal/store/migrations/0002_leases.sql`.
2. Fix `TestMigrateIsIdempotent` if you have not already made it count-relative.
3. Confirm both migrations apply in order on a fresh container.
4. Commit.

### The migration

```sql
ALTER TABLE jobs
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN leased_by        TEXT;

CREATE INDEX jobs_lease_expiry_idx
    ON jobs (lease_expires_at)
    WHERE state = 'leased';
```

Three decisions:

- **Both columns are nullable**, and that is deliberate — unlike `payload`, where you removed the third state. Here NULL means "not leased", which is genuinely distinct from any timestamp value. A sentinel like `'epoch'` would be a lie you'd have to remember.
- **`leased_by` is the agent id.** It is not used for correctness — the lock does that — but when a job is stuck you want to know which agent has it. Phase 9 populates it properly; for now the leaser passes its own identifier.
- **The index is partial** (`WHERE state = 'leased'`). Only leased rows are ever scanned for expiry, and in a healthy system that is a tiny fraction of the table. A full index on `lease_expires_at` would be mostly NULLs and mostly useless. Run `EXPLAIN` on the reaper query in Task 4 and confirm it gets used — this is the phase where reading a query plan starts to pay.

**Do not edit `0001`.** It has been applied. From here, the schema only moves forward.

### Done when

`go test ./internal/store/` passes on a fresh container and `schema_migrations` has two rows.

---

## Task 2 — `Lease`

**Goal:** claim up to n pending jobs atomically, and never hand the same one out twice.

### Signature

```go
// Lease claims up to n jobs that are ready to run, marks them leased by
// leasedBy for the given duration, and returns them.
// It returns an empty slice when no work is available.
func (s *Store) Lease(ctx context.Context, leasedBy string, n int, d time.Duration) ([]job.Job, error)
```

### The query

This is the centre of the phase. Build it up rather than pasting it:

```sql
UPDATE jobs
SET state            = 'leased',
    lease_expires_at = now() + $3,
    leased_by        = $2,
    updated_at       = now()
WHERE id IN (
    SELECT id FROM jobs
    WHERE state = 'pending'
    ORDER BY created_at, id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, type, payload, state, attempt, created_at, updated_at;
```

Read it inside out, because every clause is load-bearing:

- **The inner `SELECT` picks candidates.** `ORDER BY created_at, id` makes it a queue rather than a bag — oldest first, with the id tiebreak for jobs enqueued in the same transaction.
- **`FOR UPDATE`** locks each selected row for the duration of the transaction. Another transaction touching the same row would block.
- **`SKIP LOCKED`** changes that to: rows already locked are invisible to this query. Replica B sees jobs 11–20 while A holds 1–10. This is the whole trick.
- **The outer `UPDATE`** flips the state. Because it operates on ids the inner query locked, no one else can be mid-update on them.
- **`RETURNING`** hands back the leased rows in one round trip — no follow-up `SELECT`, and no window where the rows could change underneath you.

Two things that look optional and are not:

- **`FOR UPDATE SKIP LOCKED` must be in the subquery, not the outer `UPDATE`.** The outer statement is an `UPDATE`; it takes its own locks. Putting the clause there is a syntax error, and reaching for it there means the model is wrong.
- **The whole statement is one transaction** even without `BEGIN` — a bare statement runs in an implicit one, and the locks release at its end. That is exactly the scope you want. You do **not** need `BeginFunc` here.

Design notes:

- `$3` is an `interval`. pgx encodes `time.Duration` as one; verify with a test rather than trusting me.
- Normalize `n` the way `List` normalizes `Limit` — `n <= 0` means `DefaultLimit`.
- Return `[]job.Job` and a nil slice for "no work". "Nothing to do" is not an error; the dispatch loop in phase 7 will call this on a ticker and get empty results most of the time.
- `lease_expires_at` and `leased_by` are deliberately **not** on `job.Job`. They are storage bookkeeping; the domain has no rule about them. If you find yourself wanting them on the struct, ask what domain behavior needs them before adding them.

### `lease_test.go`

```go
package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

func enqueueN(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := range n {
		j := &job.Job{ID: fmt.Sprintf("j%03d", i), Type: "resize"}
		if err := s.Enqueue(t.Context(), j); err != nil {
			t.Fatalf("Enqueue %s: %v", j.ID, err)
		}
	}
}

func TestLeaseClaimsPendingJobs(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 5)

	got, err := s.Lease(t.Context(), "agent-1", 3, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Lease returned %d jobs, want 3", len(got))
	}
	for _, j := range got {
		if j.State != job.Leased {
			t.Errorf("job %s has state %s, want %s", j.ID, j.State, job.Leased)
		}
	}
}

func TestLeaseTakesOldestFirst(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 5)

	got, err := s.Lease(t.Context(), "agent-1", 2, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 2 || got[0].ID != "j000" || got[1].ID != "j001" {
		t.Errorf("Lease returned %v, want the two oldest jobs", ids(got))
	}
}

func TestLeaseSkipsAlreadyLeasedJobs(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 3)
	ctx := t.Context()

	first, err := s.Lease(ctx, "agent-1", 2, time.Minute)
	if err != nil {
		t.Fatalf("first Lease: %v", err)
	}
	second, err := s.Lease(ctx, "agent-2", 2, time.Minute)
	if err != nil {
		t.Fatalf("second Lease: %v", err)
	}

	if len(first) != 2 || len(second) != 1 {
		t.Fatalf("leases returned %d and %d jobs, want 2 and 1", len(first), len(second))
	}
	if overlap := intersect(ids(first), ids(second)); len(overlap) != 0 {
		t.Errorf("both leases returned %v", overlap)
	}
}

func TestLeaseOnEmptyQueueReturnsNothing(t *testing.T) {
	s := freshDB(t)

	got, err := s.Lease(t.Context(), "agent-1", 5, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Lease on an empty queue returned %d jobs", len(got))
	}
}

// TestLeaseIsExclusiveUnderConcurrency is the test this phase exists for.
// Ten leasers race for fifty jobs; every job must be handed out exactly once.
func TestLeaseIsExclusiveUnderConcurrency(t *testing.T) {
	const (
		jobs    = 50
		leasers = 10
		each    = 5
	)

	s := freshDB(t)
	enqueueN(t, s, jobs)
	ctx := t.Context()

	var (
		mu      sync.Mutex
		claimed []string
		errs    []error
		wg      sync.WaitGroup
	)

	wg.Add(leasers)
	for i := range leasers {
		go func() {
			defer wg.Done()
			got, err := s.Lease(ctx, fmt.Sprintf("agent-%d", i), each, time.Minute)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			claimed = append(claimed, ids(got)...)
		}()
	}
	wg.Wait()

	for _, err := range errs {
		t.Errorf("Lease: %v", err)
	}

	seen := make(map[string]int, len(claimed))
	for _, id := range claimed {
		seen[id]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("job %s was leased %d times", id, n)
		}
	}
	if len(claimed) != jobs {
		t.Errorf("leasers claimed %d jobs in total, want %d", len(claimed), jobs)
	}
}
```

You will need two small test helpers — `ids([]job.Job) []string` and
`intersect(a, b []string) []string`. Write them in the test file; they are not part of
the package's API.

**Run the concurrency test with `-race`, and run it more than once**: `go test -race
-count=5 -run TestLeaseIsExclusive ./internal/store/`. A concurrency test that has
passed once has told you almost nothing.

Then do the experiment the phase is for: **delete `SKIP LOCKED`** and run it again.
Watch what changes — and notice that the "leased twice" assertion may still pass while
the test slows to a crawl, because plain `FOR UPDATE` blocks rather than duplicating.
Then delete `FOR UPDATE` entirely and watch duplicates appear. Two deletions, two
different failure modes, and you will not have to look this up again.

### Done when

All five tests pass under `-race -count=5`, and you have run both deletion experiments.

---

## Task 3 — `Renew` and `Release`

**Goal:** a long-running job can extend its lease; a finished one gives it back.

### Signatures

```go
// Renew extends the lease on a job held by leasedBy.
// It returns an error wrapping job.ErrNotFound if the job is not leased by
// leasedBy — because the lease expired and someone else took it, or because it
// never held it.
func (s *Store) Renew(ctx context.Context, id, leasedBy string, d time.Duration) error

// Complete moves a leased job to its terminal state and records an event.
// state must be one of job.Succeeded or job.Dead.
func (s *Store) Complete(ctx context.Context, id, leasedBy string, state job.State, message string) error
```

Requirements:

- **Both are ownership-checked.** `WHERE id = $1 AND leased_by = $2 AND state = 'leased'`. An agent whose lease expired ten seconds ago must not be able to mark the job succeeded — the reaper has already returned it to the queue and another agent may be running it. This is the at-least-once contract showing its teeth.
- Use the `CommandTag`'s `RowsAffected()` to detect "no such lease". Zero rows updated is the signal, and it is why these return an error rather than silently doing nothing.
- **`Complete` validates the state against the domain**: call `job.Transition(job.Leased, state)` before touching the database. The state machine already knows which transitions are legal — do not restate the rule in SQL or in an `if`. This is the first place `internal/job` earns its keep from outside its own package.
- `Complete` writes a `job_events` row with the message, in the same transaction as the update. Same argument as `Enqueue`.
- Clear `lease_expires_at` and `leased_by` back to NULL on completion. A terminal job holding a lease is a lie the reaper will trip over.

### Tests to write

Write these yourself — the shapes are established by now. Cover at minimum:

- `Renew` extends a held lease (assert `lease_expires_at` actually moved; query it directly).
- `Renew` on a job leased by someone else returns `job.ErrNotFound`.
- `Complete` with `job.Succeeded` sets the state, clears the lease columns, and appends an event.
- `Complete` with `job.Pending` is rejected by `job.Transition` **before** any SQL runs.
- `Complete` by a non-owner returns `job.ErrNotFound` and changes nothing.

That last one is the important one, and it is the same shape as
`TestFailedEnqueueWritesNothing`: assert the *absence* of an effect, not just the error.

---

## Task 4 — The expiry reaper

**Goal:** a job whose agent died goes back on the queue.

### Signature

```go
// ReapExpired returns jobs whose lease has expired to the pending state and
// records an event for each. It returns the number of jobs reaped.
func (s *Store) ReapExpired(ctx context.Context) (int, error)
```

Requirements:

- One `UPDATE ... WHERE state = 'leased' AND lease_expires_at < now() RETURNING id`. The comparison happens **in the database**, against the database's clock — this is the constraint from the top of the plan, and it is why `lease_expires_at` was written as `now() + interval` rather than a Go timestamp.
- Clear both lease columns.
- **Do not increment `attempt`.** A crashed agent is not a failed attempt — nothing reported a failure. Incrementing here would let network partitions bury jobs. This is a design decision worth writing down in a comment; it is the kind of thing that gets "fixed" wrongly later.
- Record a `job_events` row per reaped job, with a message saying the lease expired. This is the first message that carries real information — exactly the `failed`-vs-`failed` disambiguation case from our earlier discussion.
- Reaping many jobs in one statement is fine and correct. Do not loop in Go.

### The test that matters

Leasing for `-time.Second` — a lease that is already expired when created — is the
trick that makes this testable with no sleeping and no clock injection. Lease with a
negative duration, call `ReapExpired`, assert the job is `pending` again with `attempt`
unchanged and an event recorded.

Also assert the negative: a job with a *live* lease is not reaped. A reaper that
returns everything passes a badly written test.

---

## Task 5 — Wire `Policy` to the store

**Goal:** the retry policy you built in phase 2 finally decides something.

### Signature

```go
// Fail applies p to a failed attempt on a leased job: it increments the
// attempt count, then either schedules a retry or dead-letters the job.
// It returns the decision that was applied.
func (s *Store) Fail(ctx context.Context, id, leasedBy string, cause error, p job.Policy) (job.Decision, error)
```

This is the payoff for `OnFailure` returning a `Decision` value instead of mutating.
The sequence, all in one transaction:

1. Load the job `FOR UPDATE` (ownership-checked as in Task 3).
2. Increment `Attempt` — the plan for phase 2 was explicit that `OnFailure`'s input must already include the failure being reported.
3. Call `p.OnFailure(&j, cause)`. Pure function, no I/O, already tested to death.
4. Apply the returned `Decision`: state, `next_attempt_at`, clear the lease columns.
5. Record an event carrying `cause.Error()` as the message.

You will need a column for `NextAttemptAt` — migration `0003`, and `Lease`'s inner
`SELECT` gains `AND (next_attempt_at IS NULL OR next_attempt_at <= now())`. That
condition is what makes backoff real rather than decorative; without it a job scheduled
for retry in 8 seconds gets picked up immediately.

Tests: a retryable failure schedules a future `next_attempt_at` and returns to
`pending`; a `job.Permanent` cause goes straight to `dead`; a job at `MaxAttempts` goes
to `dead`; and — the one that proves the backoff column works — a job with a future
`next_attempt_at` is **not** returned by `Lease`.

---

## Phase review checklist

```bash
gofmt -l .
go vet ./...
staticcheck -checks=all ./...
go test -race -count=5 ./internal/store/
go test -short ./...
go list -deps ./internal/job | grep -v '^github.com/mddemin/conveyor' | grep '\.'
docker ps
```

By eye:

- No `time.Now()` anywhere in `internal/store`. Every timestamp comes from `now()`.
- No `time.Sleep` in any test.
- Every ownership-checked statement has `AND leased_by = $n AND state = 'leased'`.
- `internal/job` still unchanged — `Transition`, `Policy` and `Decision` are being *used* from outside for the first time, which is the test of whether phase 1–2's design was right. If any of them needs altering to fit, that is the most interesting finding of the phase and worth discussing rather than quietly patching.
