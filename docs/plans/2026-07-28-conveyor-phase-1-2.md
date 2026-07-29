# Conveyor Phases 1–2 Implementation Plan

> **How to use this plan.** You write every line of implementation code. Each task gives you the exact files, the exact public signatures other tasks depend on, the **complete test code**, and the design hints worth knowing in advance. It does not give you the function bodies — that is the part you are here to learn.
>
> Tests are handed to you verbatim on purpose. A test is the specification: it is how you know what "done" means, and writing your own would mean grading your own homework. Type them in, watch them fail for the right reason, then make them pass however you see fit. Claude reviews each task when you finish it.
>
> Steps use checkbox (`- [ ]`) syntax — tick them as you go. Do not skip the "watch it fail" steps; a test you have never seen fail is a test you cannot trust.

**Goal:** Build `internal/job` — the dependency-free domain core of Conveyor: the job lifecycle state machine and the retry policy that decides whether a failed job is retried or buried.

**Architecture:** One package, `internal/job`, importing nothing outside the standard library. It knows what a job is, which state changes are legal, and how long to wait before retrying. It does not know about Postgres, HTTP, gRPC, or goroutines. Every later phase depends on this package; it depends on nothing.

**Tech Stack:** Go 1.26 standard library only. `testing` (table-driven, subtests), `errors` (wrapping, `errors.As`), `slices`, `math/rand/v2`, `time`.

## Global Constraints

Every task inherits these. A violation is a review finding whether or not the task mentions it.

- **`internal/job` imports only the Go standard library.** No third-party module, ever. This is the architectural invariant of the whole project.
- Go 1.26 (`mise.toml` already pins it; verify with `go version`).
- Module path: `github.com/mddemin/conveyor`. Change it in Task 1 if you prefer a different one, but change it *before* Task 2 — renaming later touches every file.
- `go test -race ./...` and `go vet ./...` pass before every commit.
- **No `time.Sleep` in tests.** Time is injected, never waited on.
- Code is `gofmt`-formatted. Run `gofmt -l .` — it must print nothing.
- Every exported identifier has a doc comment starting with its own name. `go doc ./internal/job` should read like documentation, not like a listing.
- Commit after every task. Conventional commit messages (`feat:`, `test:`, `refactor:`).

## File Structure

| File | Responsibility |
|---|---|
| `go.mod` | Module declaration |
| `internal/job/state.go` | `State` type, the five states, the legal-transition table, `Transition` |
| `internal/job/state_test.go` | Full transition matrix, terminal-state tests |
| `internal/job/errors.go` | Sentinel errors (`ErrInvalidTransition`, `ErrNotFound`) |
| `internal/job/job.go` | The `Job` struct and its `To` method |
| `internal/job/job_test.go` | `To` mutation and rejection behavior |
| `internal/job/permanent.go` | `Permanent` / `IsPermanent` error classification |
| `internal/job/permanent_test.go` | Wrapping, unwrapping, `errors.Is` interop |
| `internal/job/retry.go` | `Clock`, `Policy`, `Decision`, `Backoff`, `OnFailure` |
| `internal/job/retry_test.go` | Backoff sequence, jitter bounds, retry-versus-bury decisions |

Files are split by responsibility, not by layer. The state machine and the retry policy change for different reasons, so they live apart.

---

### Task 1: Module and the `State` type

**Files:**
- Create: `go.mod`
- Create: `internal/job/state.go`
- Test: `internal/job/state_test.go`

**Interfaces you must produce:**

```go
type State string
const Pending, Leased, Succeeded, Failed, Dead State
func (s State) String() string
func (s State) Terminal() bool
```

- [x] **Step 1: Initialize the module**

```bash
cd /home/mikhail/Projects/go-practice
go mod init github.com/mddemin/conveyor
```

Expected: `go: creating new go.mod: module github.com/mddemin/conveyor`

- [x] **Step 2: Write the failing test**

Create `internal/job/state_test.go`:

```go
package job

import "testing"

func TestStateString(t *testing.T) {
	if got := Pending.String(); got != "pending" {
		t.Errorf("Pending.String() = %q, want %q", got, "pending")
	}
}

func TestStateTerminal(t *testing.T) {
	tests := []struct {
		state State
		want  bool
	}{
		{Pending, false},
		{Leased, false},
		{Failed, false},
		{Succeeded, true},
		{Dead, true},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			if got := tt.state.Terminal(); got != tt.want {
				t.Errorf("%s.Terminal() = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}
```

Note the package clause: `package job`, not `package job_test`. These tests exercise the package's own internals, so they live inside it.

- [x] **Step 3: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestState -v`

Expected: a **build** failure, not a test failure — `undefined: Pending`. That is the correct first failure; the test cannot compile because the thing it tests does not exist.

- [x] **Step 4: Write the implementation**

Create `internal/job/state.go`. It must contain:

- A **package doc comment** on `package job` — one paragraph saying what the domain package is and what it deliberately does not know about (storage, transport). This is the first thing anyone reads about the project; write it for a stranger.
- `State` as a **named string type**, not an int. String states are self-describing in database rows, JSON, and log lines, which matters far more here than the handful of bytes an int would save.
- The five constants in one `const` block, with wire values `"pending"`, `"leased"`, `"succeeded"`, `"failed"`, `"dead"`. One doc comment above the block covering all five is idiomatic; five separate one-liners are noise.
- `String()` — a one-line conversion.
- `Terminal()` — true for exactly the two states with no outgoing transitions.

The lifecycle you are encoding: a job is created `Pending`, is `Leased` to an agent, and ends `Succeeded` or `Dead`. `Failed` is transient — the retry policy in Task 7 moves it back to `Pending` or on to `Dead`.

- [x] **Step 5: Run the tests and watch them pass**

Run: `go test ./internal/job/ -run TestState -v`

Expected: `PASS`, with five subtests listed under `TestStateTerminal`.

- [x] **Step 6: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add go.mod internal/job/state.go internal/job/state_test.go
git commit -m "feat(job): add State type and lifecycle states"
```

---

### Task 2: Sentinel errors and the transition table

**Files:**
- Create: `internal/job/errors.go`
- Modify: `internal/job/state.go` (add the transition table and `Transition`)
- Test: `internal/job/state_test.go` (add the transition tests)

**Interfaces you must produce:**

```go
var ErrInvalidTransition error
var ErrNotFound error
func Transition(from, to State) error
```

The legal transitions, which the test below encodes exhaustively:

| From | To | Why |
|---|---|---|
| Pending | Leased | dispatcher hands the job to an agent |
| Leased | Succeeded | agent reported success |
| Leased | Failed | agent reported failure |
| Leased | Pending | lease expired; the reaper took it back |
| Failed | Pending | retry policy scheduled another attempt |
| Failed | Dead | attempts exhausted, or the error was permanent |

Everything else is illegal. `Succeeded` and `Dead` have no outgoing transitions.

- [x] **Step 1: Write the failing test**

Change the import block at the top of `internal/job/state_test.go` to:

```go
import (
	"errors"
	"testing"
)
```

Then append:

```go
func TestTransitionAllowed(t *testing.T) {
	allowed := []struct{ from, to State }{
		{Pending, Leased},
		{Leased, Succeeded},
		{Leased, Failed},
		{Leased, Pending},
		{Failed, Pending},
		{Failed, Dead},
	}

	for _, tt := range allowed {
		t.Run(tt.from.String()+"_to_"+tt.to.String(), func(t *testing.T) {
			if err := Transition(tt.from, tt.to); err != nil {
				t.Errorf("Transition(%s, %s) = %v, want nil", tt.from, tt.to, err)
			}
		})
	}
}

func TestTransitionRejectsEverythingElse(t *testing.T) {
	all := []State{Pending, Leased, Succeeded, Failed, Dead}
	legal := map[State]map[State]bool{
		Pending: {Leased: true},
		Leased:  {Succeeded: true, Failed: true, Pending: true},
		Failed:  {Pending: true, Dead: true},
	}

	for _, from := range all {
		for _, to := range all {
			if legal[from][to] {
				continue
			}
			t.Run(from.String()+"_to_"+to.String(), func(t *testing.T) {
				err := Transition(from, to)
				if !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("Transition(%s, %s) = %v, want ErrInvalidTransition", from, to, err)
				}
			})
		}
	}
}
```

The second test is the valuable one. It asserts the *complement* of the allowed set, so adding a sixth state later cannot silently open an illegal path — it enumerates all 25 pairs and demands a rejection for the 19 not listed.

Worth noticing in that test, because you will want the same trick in the implementation: `legal[from][to]` where `from` is not a key returns the zero value of the inner map type — a nil map — and indexing a nil map is legal and returns false. No existence check needed.

- [x] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestTransition -v`

Expected: build failure — `undefined: Transition`, `undefined: ErrInvalidTransition`.

- [x] **Step 3: Write the sentinel errors**

Create `internal/job/errors.go` with two package-level sentinels built by `errors.New`:

- `ErrInvalidTransition` — a state change the lifecycle forbids.
- `ErrNotFound` — no job exists with the requested identifier. Nothing uses this yet; it lives here so phase 3's store does not invent a competing version, and so phase 5's HTTP layer has exactly one thing to map to a 404. Say that in its doc comment.

- [x] **Step 4: Write the transition table**

Add to `internal/job/state.go`:

- **The legal transitions as data, not control flow.** An unexported package-level `map[State][]State` from each state to the states reachable from it. Resist writing a `switch` or a chain of `if`s: a table is read at a glance, cannot go subtly asymmetric, and the two terminal states simply do not appear in it.
- `Transition(from, to State) error` — consult the table, return `nil` or an error.

Two things to get right:

1. **Terminal states need no special case.** Indexing the map with a state that is not a key yields a nil slice, and `slices.Contains` on a nil slice is false. If you find yourself writing a comma-ok check or an explicit terminal branch, you are working harder than the language requires.
2. **The error must satisfy `errors.Is(err, ErrInvalidTransition)` while still naming the specific pair.** That means wrapping with `%w` inside an `fmt.Errorf`, not returning the bare sentinel. `errors.Is` walks the wrap chain; a human reading a log needs to see *which* transition was refused.

- [x] **Step 5: Run the tests and watch them pass**

Run: `go test ./internal/job/ -v`

Expected: `PASS`. `TestTransitionRejectsEverythingElse` should report 19 subtests.

- [x] **Step 6: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/
git commit -m "feat(job): add lifecycle transition table"
```

---

### Task 3: The `Job` struct and `To`

**Files:**
- Create: `internal/job/job.go`
- Test: `internal/job/job_test.go`

**Interfaces you must produce:**

```go
type Job struct { /* fields below */ }
func (j *Job) To(next State) error
```

`Job` has exactly these fields, in this order:

| Field       | Type        | Meaning                                           |
| ----------- | ----------- | ------------------------------------------------- |
| `ID`        | `string`    | unique identifier                                 |
| `Type`      | `string`    | selects the executor that runs the job            |
| `Payload`   | `[]byte`    | the executor's opaque input                       |
| `State`     | `State`     | current lifecycle position                        |
| `Attempt`   | `int`       | **completed** attempts; zero before the first run |
| `CreatedAt` | `time.Time` | set by the storage layer in phase 3               |
| `UpdatedAt` | `time.Time` | set by the storage layer in phase 3               |

Deliberately absent: lease fields (phase 4 adds them), `MaxAttempts` (the `Policy` owns it — one place, not two), and `NextAttemptAt` (Task 7's `Decision` carries it; the store persists it). Do not add fields on speculation. Every field here has a consumer in this plan or the next phase, and I will ask you to justify any you add.

`Attempt` counting **completed** attempts is a decision Task 7 depends on. The caller increments it when a job moves `Leased -> Failed`, so by the time the retry policy sees the job, the attempt that just failed is already counted. Fix that in your head now.

- [x] **Step 1: Write the failing test**

Create `internal/job/job_test.go`:

```go
package job

import (
	"errors"
	"testing"
)

func TestJobToAdvancesState(t *testing.T) {
	j := &Job{ID: "j1", State: Pending}

	if err := j.To(Leased); err != nil {
		t.Fatalf("To(Leased) = %v, want nil", err)
	}
	if j.State != Leased {
		t.Errorf("State = %s, want %s", j.State, Leased)
	}
}

func TestJobToRejectsIllegalTransition(t *testing.T) {
	j := &Job{ID: "j1", State: Pending}

	err := j.To(Succeeded)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("To(Succeeded) = %v, want ErrInvalidTransition", err)
	}
	if j.State != Pending {
		t.Errorf("State = %s after rejected transition, want %s unchanged", j.State, Pending)
	}
}
```

The second assertion in each test is the one that matters. A rejected transition must leave the job untouched — a method that returns an error *and* mutates is how you get corrupt state that only surfaces three layers away.

- [x] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestJob -v`

Expected: build failure — `undefined: Job`.

- [ ] **Step 3: Write the implementation**

Create `internal/job/job.go` with the struct above and the `To` method.

`To` is a handful of lines. The design points it must honor:

- **Pointer receiver**, because it mutates. Go's convention is that a type's method set is consistently pointer or value; `Job` will grow more mutating methods, so pointer is right for all of them.
- **Validate, then mutate — never the reverse.** Delegate validation to `Transition` rather than duplicating the table's knowledge, and on failure return early with the error unchanged so `errors.Is` still works upstream.
- Doc comments on the type, on each field whose meaning is not obvious from its name (`Type`, `Payload`, and `Attempt` all need one), and on the method — including the guarantee that a rejected transition leaves the job unchanged. That guarantee is part of the contract, so it belongs in the docs, not only in the test.

- [x] **Step 4: Run the tests and watch them pass**

Run: `go test ./internal/job/ -v`

Expected: `PASS`.

- [x] **Step 5: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/job.go internal/job/job_test.go
git commit -m "feat(job): add Job struct and guarded state transitions"
```

**Phase 1 is complete at this commit.** You have a fully tested state machine importing nothing but `time`, `fmt`, `slices`, and `errors`. Ask for a review before starting Task 4.

---

### Task 4: `Clock` and the backoff sequence

**Files:**
- Create: `internal/job/retry.go`
- Test: `internal/job/retry_test.go`

**Interfaces you must produce:**

```go
type Clock interface {
	Now() time.Time
}

type Policy struct {
	Base        time.Duration  // delay window after the first failure
	Max         time.Duration  // cap on the window, however many attempts
	MaxAttempts int            // attempts after which a job is buried
	Clock       Clock          // nil means the wall clock
	Jitter      func() float64 // returns [0,1); nil means rand.Float64
}

func (p Policy) Backoff(attempt int) time.Duration
```

`MaxAttempts` and `Clock` are declared now but unused until Task 7. That is deliberate: the struct's shape is the contract other tasks compile against, and splitting it across two commits would churn every test.

**The algorithm — exponential backoff with full jitter.** Given `attempt` attempts already made, the delay is drawn uniformly from:

```
[0, min(Max, Base * 2^(attempt-1)))
```

With `Base = 100ms` and `Max = 2s`, the *window* grows 100ms, 200ms, 400ms, 800ms, 1.6s, 2s, 2s, … and the returned delay is a random point inside it.

Full jitter rather than plain exponential because a thousand jobs failing against the same downed dependency must not retry in lockstep and knock it over again the moment it recovers. Spreading them uniformly across the window is the entire point; a fixed delay just moves the thundering herd later.

**Why `Jitter` is a `func() float64` and not a seeded `*rand.Rand`.** Injecting a function lets this task's tests pin the draw to an exact value and assert the exponential sequence precisely, while Task 5 tests the randomness separately. Injecting a seed instead would couple your assertions to the internals of Go's PRNG, which is explicitly not guaranteed stable across releases.

- [x] **Step 1: Write the failing test**

Create `internal/job/retry_test.go`:

```go
package job

import (
	"strconv"
	"testing"
	"time"
)

// noJitter draws the maximum of the jitter range, so Backoff returns the
// ceiling of the window and the exponential sequence can be asserted exactly.
func noJitter() float64 { return 1 }

func TestBackoffCeilingSequence(t *testing.T) {
	p := Policy{
		Base:   100 * time.Millisecond,
		Max:    2 * time.Second,
		Jitter: noJitter,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 2 * time.Second}, // capped at Max
		{7, 2 * time.Second},
		{50, 2 * time.Second}, // no overflow at large attempt counts
	}

	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.attempt), func(t *testing.T) {
			if got := p.Backoff(tt.attempt); got != tt.want {
				t.Errorf("Backoff(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestBackoffTreatsNonPositiveAttemptAsFirst(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second, Jitter: noJitter}

	for _, attempt := range []int{0, -1} {
		if got := p.Backoff(attempt); got != 100*time.Millisecond {
			t.Errorf("Backoff(%d) = %v, want %v", attempt, got, 100*time.Millisecond)
		}
	}
}
```

- [x] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestBackoff -v`

Expected: build failure — `undefined: Policy`.

- [x] **Step 3: Write the implementation**

Create `internal/job/retry.go` with the `Clock` interface, the `Policy` struct, and `Backoff`.

Three things to get right, and one of them is a trap:

1. **The `{50, 2 * time.Second}` case is a trap, deliberately.** The obvious implementation is `Base << (attempt-1)`. That overflows `time.Duration` — an `int64` of nanoseconds — somewhere around attempt 55, and starts returning *negative* delays: a job that retries instantly, forever, in production, months after you wrote it. Find a way to grow the window that cannot overflow no matter how large `attempt` gets. Hint: if you stop growing as soon as you have passed `Max`, you can never get far enough to overflow.
2. **Guard `attempt < 1`** so a caller passing 0 gets the first window rather than a negative exponent's worth of nonsense.
3. **Handle a nil `Jitter` in an unexported helper method** — `func (p Policy) jitter() float64` — rather than checking inline at the call site. `Backoff` stays readable, and Task 7 adds the identical pattern for `Clock`. This is what makes the zero value partially useful: a caller who does not care about determinism sets nothing and gets real randomness. `math/rand/v2`'s top-level `rand.Float64` is safe for concurrent use, so no lock is needed.

On `Clock`: notice it is declared *here*, in the package that consumes it, with exactly one method — not exported from a shared `clock` package carrying `Now`, `Since`, `After`, and `Timer`. This is the most important interface convention in Go and the one people arriving from Java or C# get wrong most often: **the consumer declares the interface, and it contains only what the consumer uses.** Write its doc comment to say why it exists (tests inject a fixed time), not what it does (obvious).

- [x] **Step 4: Run the tests and watch them pass**

Run: `go test ./internal/job/ -run TestBackoff -v`

Expected: `PASS`, ten subtests.

- [x] **Step 5: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/retry.go internal/job/retry_test.go
git commit -m "feat(job): add retry Policy with exponential backoff"
```

---

### Task 5: Prove the jitter is actually jittering

**Files:**
- Modify: `internal/job/retry_test.go` (add two tests)

**Interfaces:** consumes `Policy` and `Backoff` from Task 4. Produces nothing — this task adds tests only.

Task 4 pinned the random draw, which means nothing written so far proves the jitter exists at all. A `Backoff` that ignored `p.Jitter` entirely and returned the ceiling of the window would pass every test in this repo. This task closes that hole and checks the nil default works.

- [x] **Step 1: Write the tests**

Append to `internal/job/retry_test.go`:

```go
func TestBackoffAppliesJitter(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, Jitter: func() float64 { return 0.25 }}

	// The window for attempt 3 is 4s; a quarter of it is 1s.
	if got, want := p.Backoff(3), time.Second; got != want {
		t.Errorf("Backoff(3) = %v, want %v", got, want)
	}
}

func TestBackoffStaysWithinWindowWithRealRandomness(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second} // nil Jitter: real randomness

	const window = 400 * time.Millisecond // attempt 3
	seen := make(map[time.Duration]bool)

	for range 1000 {
		d := p.Backoff(3)
		if d < 0 || d >= window {
			t.Fatalf("Backoff(3) = %v, want within [0, %v)", d, window)
		}
		seen[d] = true
	}

	if len(seen) < 100 {
		t.Errorf("got %d distinct delays over 1000 draws, want at least 100 — jitter looks constant", len(seen))
	}
}
```

The second is a property test rather than an example test: it asserts an invariant that must hold for *every* draw (in range), plus a statistical fact that fails loudly if the jitter is stubbed out (many distinct values). The threshold of 100 out of 1000 sits far below what real randomness produces and far above what a constant produces, so it will not flake.

- [x] **Step 2: Run the tests — they should pass immediately**

Run: `go test ./internal/job/ -run TestBackoff -v`

Expected: **PASS**, because Task 4's implementation is already correct.

This is the one place in the plan where tests come after the code they cover, and it is worth understanding why that is acceptable rather than a lapse. These tests do not drive new behavior; they close a gap in the existing suite. Which means you have not yet watched them fail, and an unfailed test is worthless — so:

- [x] **Step 3: Verify the tests actually bite, then revert**

```bash
# Temporarily break Backoff: return the window itself, ignoring jitter entirely.
go test ./internal/job/ -run TestBackoff
# Expected: FAIL on TestBackoffAppliesJitter and TestBackoffStaysWithinWindowWithRealRandomness

# Revert your edit.
go test ./internal/job/ -run TestBackoff
# Expected: PASS
```

Do this literally. It takes thirty seconds, and it is the difference between a test suite and a suite-shaped pile of green ticks.

- [x] **Step 4: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/retry_test.go
git commit -m "test(job): assert jitter is applied and stays in window"
```

---

### Task 6: Permanent errors

**Files:**
- Create: `internal/job/permanent.go`
- Test: `internal/job/permanent_test.go`

**Interfaces you must produce:**

```go
func Permanent(err error) error  // marks err as not worth retrying; nil in, nil out
func IsPermanent(err error) bool // true if err, or anything it wraps, was marked
```

Not every failure deserves a retry. A `503` from a job's target is worth trying again; a `400` never will be, and retrying it five times with backoff wastes capacity and delays the dead-letter signal an operator needs. Executors mark the hopeless ones with `Permanent`, and Task 7's policy reads that.

This is the `errors.As` half of Go's error model, distinct from the `errors.Is` sentinels in Task 2. Sentinels answer *which* error is this; type-based wrapping answers *what kind of* error is this. "Permanent" is a kind.

- [x] **Step 1: Write the failing test**

Create `internal/job/permanent_test.go`:

```go
package job

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsPermanent(t *testing.T) {
	base := errors.New("bad request")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error", base, false},
		{"marked", Permanent(base), true},
		{"marked then wrapped", fmt.Errorf("calling api: %w", Permanent(base)), true},
		{"wrapped then marked", Permanent(fmt.Errorf("calling api: %w", base)), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanent(tt.err); got != tt.want {
				t.Errorf("IsPermanent(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestPermanentPreservesTheUnderlyingError(t *testing.T) {
	base := errors.New("bad request")
	err := fmt.Errorf("calling api: %w", Permanent(base))

	if !errors.Is(err, base) {
		t.Errorf("errors.Is(err, base) = false, want true — marking must not hide the cause")
	}
	if got, want := Permanent(base).Error(), "bad request"; got != want {
		t.Errorf("Permanent(base).Error() = %q, want %q", got, want)
	}
}

func TestPermanentOfNilIsNil(t *testing.T) {
	if err := Permanent(nil); err != nil {
		t.Errorf("Permanent(nil) = %v, want nil", err)
	}
}
```

Two of these constrain the design more than they look:

- `TestPermanentPreservesTheUnderlyingError` forbids a marker that swallows its cause. That is the classic way custom error types make a codebase worse — every `errors.Is` check upstream silently stops working.
- `TestPermanentOfNilIsNil` matters because `Permanent(nil)` returning a non-nil error makes the very common `return Permanent(doThing())` report failure on success. It is a cousin of the nil-pointer-in-a-non-nil-interface trap, and it is worth meeting here rather than in production.

- [x] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run Permanent -v`

Expected: build failure — `undefined: Permanent`.

- [x] **Step 3: Write the implementation**

Create `internal/job/permanent.go`. The shape:

- An **unexported** struct type wrapping an `error`. Unexported because the only supported way to create one is `Permanent` and the only supported way to detect one is `IsPermanent` — which leaves you free to change the representation later without breaking callers. Exporting it would freeze it into the API for no benefit.
- `Error() string` that **delegates** to the wrapped error rather than decorating it. "Permanent" is metadata for the retry policy, not something an operator reading a log needs prefixed onto every message. Task 7 reads the flag; humans read the message.
- `Unwrap() error` returning the cause. This single method is what makes `errors.Is`, `errors.As`, and the `"wrapped then marked"` test case all work.
- `Permanent` and `IsPermanent` themselves. `IsPermanent` is one `errors.As` call.

One decision to make consciously: **value receiver or pointer receiver on the marker type?** It changes what `errors.As` needs as a target and whether two markings of the same cause compare equal. Pick one and be ready to say why in review.

- [x] **Step 4: Run the tests and watch them pass**

Run: `go test ./internal/job/ -run Permanent -v`

Expected: `PASS`, seven subtests.

- [x] **Step 5: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/permanent.go internal/job/permanent_test.go
git commit -m "feat(job): distinguish permanent failures from retryable ones"
```

---

### Task 7: The retry decision

**Files:**
- Modify: `internal/job/retry.go` (add `Decision`, `OnFailure`, and the `Clock` default helper)
- Test: `internal/job/retry_test.go` (add the `OnFailure` tests)

**Interfaces you must produce:**

```go
type Decision struct {
	State         State     // Pending when the job will be retried, Dead when it will not
	NextAttemptAt time.Time // zero when State is Dead
}

func (p Policy) OnFailure(j *Job, cause error) Decision
```

**The rules, in order:**

1. `cause` is permanent → `Dead`.
2. `j.Attempt >= p.MaxAttempts` → `Dead`. (`Attempt` counts completed attempts, per Task 3, and the caller has already incremented it. So with `MaxAttempts: 3`, a job that has just failed its third attempt has `Attempt == 3` and is buried.)
3. Otherwise → `Pending`, at `now + Backoff(j.Attempt)`.

**Why a `Decision` value instead of mutating the job.** The caller applies it inside the same database transaction that records the failure, so the policy has no business writing to the job itself. And a function that reports a decision is trivially testable, whereas one that reaches out and mutates needs a fixture to observe. The pattern generalizes well beyond Go: push the decision and the effect apart, and the decision becomes a pure function.

- [ ] **Step 1: Write the failing test**

Change the import block at the top of `internal/job/retry_test.go` to:

```go
import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"
)
```

Then append:

```go
// fixedClock is a Clock that never advances, so scheduling assertions are exact.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

func TestOnFailure(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	p := Policy{
		Base:        time.Second,
		Max:         time.Minute,
		MaxAttempts: 3,
		Clock:       fixedClock{now},
		Jitter:      noJitter,
	}
	boom := errors.New("boom")

	tests := []struct {
		name    string
		attempt int
		cause   error
		want    Decision
	}{
		{
			name:    "first failure retries after Base",
			attempt: 1,
			cause:   boom,
			want:    Decision{State: Pending, NextAttemptAt: now.Add(time.Second)},
		},
		{
			name:    "second failure backs off further",
			attempt: 2,
			cause:   boom,
			want:    Decision{State: Pending, NextAttemptAt: now.Add(2 * time.Second)},
		},
		{
			name:    "final attempt is buried",
			attempt: 3,
			cause:   boom,
			want:    Decision{State: Dead},
		},
		{
			name:    "beyond the limit stays buried",
			attempt: 9,
			cause:   boom,
			want:    Decision{State: Dead},
		},
		{
			name:    "permanent failure is buried on the first attempt",
			attempt: 1,
			cause:   Permanent(boom),
			want:    Decision{State: Dead},
		},
		{
			name:    "permanent failure wrapped in context is still buried",
			attempt: 1,
			cause:   fmt.Errorf("fetching url: %w", Permanent(boom)),
			want:    Decision{State: Dead},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &Job{ID: "j1", State: Failed, Attempt: tt.attempt}

			got := p.OnFailure(j, tt.cause)
			if got != tt.want {
				t.Errorf("OnFailure() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestOnFailureDoesNotMutateTheJob(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, MaxAttempts: 3, Jitter: noJitter}
	j := &Job{ID: "j1", State: Failed, Attempt: 1}
	before := *j

	p.OnFailure(j, errors.New("boom"))

	if !reflect.DeepEqual(*j, before) {
		t.Errorf("OnFailure mutated the job: got %+v, want %+v", *j, before)
	}
}

func TestOnFailureDecisionIsAppliable(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, MaxAttempts: 3, Jitter: noJitter}
	j := &Job{ID: "j1", State: Failed, Attempt: 1}

	d := p.OnFailure(j, errors.New("boom"))
	if err := j.To(d.State); err != nil {
		t.Fatalf("applying decision %s to a Failed job: %v", d.State, err)
	}
}
```

Three notes on these tests.

`TestOnFailureDoesNotMutateTheJob` compares whole structs, so it catches a mutation of *any* field, including ones added in later phases. It uses `reflect.DeepEqual` rather than `==` because `Job` contains `Payload []byte`, and a struct holding a slice is not comparable — `*j != before` would not compile. Worth knowing before you meet it somewhere less forgiving: comparability in Go is a property of the *type*, not of the values, so a nil slice does not rescue it.

`Decision`, by contrast, *is* comparable — `State` is a string and `time.Time` is a comparable struct — which is why `got != tt.want` works there. Comparing `time.Time` with `==` is normally a mistake, since it compares monotonic clock readings and location pointers too; it is safe here only because both sides derive from the same `now` value. Do not carry that habit into code where the two times come from different sources. Use `Equal` there.

`TestOnFailureDecisionIsAppliable` is the seam between the two halves of this phase: it proves the retry policy only ever produces states the state machine will actually accept from `Failed`. Without it the two components can drift apart and nothing notices until runtime.

- [x] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestOnFailure -v`

Expected: build failure — `undefined: Decision`.

- [ ] **Step 3: Write the implementation**

Add to `internal/job/retry.go`: the `Decision` struct, `OnFailure`, and a `now()` helper mirroring Task 4's `jitter()` — nil `Clock` means `time.Now()`.

`OnFailure` is about five lines; both bury conditions collapse into one branch. Document on `Decision` that it is a value the caller applies, not a mutation the policy performs — the next person to read this needs to know that before they go looking for the write.

- [ ] **Step 4: Run the whole suite**

Run: `go test -race ./... -v`

Expected: `PASS`. Every test from Tasks 1 through 7 runs.

- [ ] **Step 5: Verify the package really is dependency-free**

```bash
go list -deps ./internal/job | grep -v '^github.com/mddemin/conveyor' | grep '\.' || echo "stdlib only"
```

Expected: `stdlib only`. Any line containing a dot is a module path, meaning something outside the standard library crept in. This is the mechanical form of the project's central invariant — run it at the end of every future phase that touches this package.

- [ ] **Step 6: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/retry.go internal/job/retry_test.go
git commit -m "feat(job): decide retry or dead-letter after a failed attempt"
```

---

## Done When

Phases 1 and 2 are complete when all of the following hold:

- `go test -race ./...` passes.
- `go vet ./...` and `gofmt -l .` are silent.
- `go list -deps ./internal/job` shows only standard library packages.
- The transition matrix test covers all 25 state pairs.
- No test calls `time.Sleep`.
- `go doc ./internal/job` reads like documentation.
- Seven commits, one per task.

## What Phase 3 Will Need From This

So you can see why the shape above was chosen:

- `store.Enqueue` writes a `Job` with `State: Pending, Attempt: 0`.
- `store.Get` returns `ErrNotFound` — already defined here so the store does not invent a competing one.
- The lease reaper calls `j.To(Pending)` on expired leases; the `Leased -> Pending` edge exists for it.
- The dispatcher calls `p.OnFailure(j, cause)` and writes `Decision.State` and `Decision.NextAttemptAt` in one `UPDATE`, which is why `Decision` is a value rather than a mutation.
- Phase 4 adds `LeaseExpiresAt` and `LeasedBy` to `Job`. They are absent now because nothing uses them yet.
