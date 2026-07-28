# Conveyor Phases 1–2 Implementation Plan

> **For the learner:** You write every line of code in this plan. Claude reviews each task after you complete it. Steps use checkbox (`- [ ]`) syntax — tick them as you go. Do not skip the "run the test and watch it fail" steps; watching a test fail for the *expected reason* is the only proof that the test tests anything.

**Goal:** Build `internal/job` — the dependency-free domain core of Conveyor: the job lifecycle state machine and the retry policy that decides whether a failed job is retried or buried.

**Architecture:** One package, `internal/job`, importing nothing outside the standard library. It knows what a job is, which state changes are legal, and how long to wait before retrying. It does not know about Postgres, HTTP, gRPC, or goroutines. Every later phase depends on this package; it depends on nothing.

**Tech Stack:** Go 1.26 standard library only. `testing` (table-driven, subtests), `errors` (wrapping, `errors.As`), `slices`, `math/rand/v2`, `time`.

## Global Constraints

Every task inherits these. A violation is a review finding regardless of whether the task mentions it.

- **`internal/job` imports only the Go standard library.** No third-party module, ever. This is the architectural invariant of the whole project.
- Go 1.26 (`mise.toml` already pins it; verify with `go version`).
- Module path: `github.com/ypocat/conveyor`. Change it in Task 1 if you prefer a different one, but change it *before* Task 2 — renaming later touches every file.
- `go test -race ./...` and `go vet ./...` pass before every commit.
- **No `time.Sleep` in tests.** Time is injected, never waited on.
- Code is `gofmt`-formatted. Run `gofmt -l .` — it must print nothing.
- Every exported identifier has a doc comment starting with its own name.
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

**Interfaces:**
- Consumes: nothing.
- Produces: `type State string`; constants `Pending, Leased, Succeeded, Failed, Dead State`; `func (s State) String() string`; `func (s State) Terminal() bool`.

- [ ] **Step 1: Initialize the module**

```bash
cd /home/mikhail/Projects/go-practice
go mod init github.com/ypocat/conveyor
```

Expected: `go: creating new go.mod: module github.com/ypocat/conveyor`

- [ ] **Step 2: Write the failing test**

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

- [ ] **Step 3: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestState -v`

Expected: a **build** failure, not a test failure — `undefined: Pending`. That is the correct first failure; the test cannot compile because the thing it tests does not exist.

- [ ] **Step 4: Write the minimal implementation**

Create `internal/job/state.go`:

```go
// Package job defines Conveyor's domain model: what a job is, how it moves
// through its lifecycle, and how failures are retried. It imports nothing
// outside the standard library and knows nothing about storage or transport.
package job

// State is a job's position in its lifecycle.
type State string

// The states a job can occupy. A job is created Pending, is Leased to an
// agent, and ends Succeeded or Dead. Failed is transient: the retry policy
// moves it back to Pending or on to Dead.
const (
	Pending   State = "pending"
	Leased    State = "leased"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Dead      State = "dead"
)

// String returns the state's wire representation.
func (s State) String() string { return string(s) }

// Terminal reports whether no further transition is possible from s.
func (s State) Terminal() bool { return s == Succeeded || s == Dead }
```

- [ ] **Step 5: Run the tests and watch them pass**

Run: `go test ./internal/job/ -run TestState -v`

Expected: `PASS`, with six subtests listed under `TestStateTerminal`.

- [ ] **Step 6: Commit**

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
- Test: `internal/job/state_test.go` (add `TestTransition`)

**Interfaces:**
- Consumes: `State` and its constants from Task 1.
- Produces: `var ErrInvalidTransition error`; `var ErrNotFound error`; `func Transition(from, to State) error`.

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

- [ ] **Step 1: Write the failing test**

Append to `internal/job/state_test.go`:

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

Add `"errors"` to the imports of `state_test.go`:

```go
import (
	"errors"
	"testing"
)
```

The second test is the valuable one. It asserts the *complement* of the allowed set, so adding a sixth state later cannot silently open an illegal path — the test enumerates all 25 pairs and demands a rejection for the 19 that are not listed.

- [ ] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestTransition -v`

Expected: build failure — `undefined: Transition`, `undefined: ErrInvalidTransition`.

- [ ] **Step 3: Write the sentinel errors**

Create `internal/job/errors.go`:

```go
package job

import "errors"

var (
	// ErrInvalidTransition reports a state change the lifecycle forbids.
	ErrInvalidTransition = errors.New("invalid state transition")

	// ErrNotFound reports that no job exists with the requested identifier.
	// The domain owns this error so that storage and transport layers can
	// translate it without defining competing versions of it.
	ErrNotFound = errors.New("job not found")
)
```

- [ ] **Step 4: Write the transition table**

Append to `internal/job/state.go`, and add the imports:

```go
import (
	"fmt"
	"slices"
)
```

```go
// allowed lists the states reachable from each state. States absent from the
// map, and states mapping to nil, are terminal.
var allowed = map[State][]State{
	Pending: {Leased},
	Leased:  {Succeeded, Failed, Pending},
	Failed:  {Pending, Dead},
}

// Transition reports whether a job may move from one state to another,
// returning an error wrapping ErrInvalidTransition when it may not.
func Transition(from, to State) error {
	if slices.Contains(allowed[from], to) {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
}
```

Two things worth noticing. Indexing a map with a missing key returns the zero value — a nil slice — and `slices.Contains` on a nil slice is simply false, so terminal states need no special case. And the error is wrapped with `%w`, which is what makes `errors.Is` work in the test while the message still names the specific illegal pair for a human reading a log.

- [ ] **Step 5: Run the tests and watch them pass**

Run: `go test ./internal/job/ -v`

Expected: `PASS`. `TestTransitionRejectsEverythingElse` should report 19 subtests.

- [ ] **Step 6: Commit**

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

**Interfaces:**
- Consumes: `State`, `Transition`, `ErrInvalidTransition`.
- Produces: `type Job struct` with fields `ID string`, `Type string`, `Payload []byte`, `State State`, `Attempt int`, `CreatedAt time.Time`, `UpdatedAt time.Time`; `func (j *Job) To(next State) error`.

Deliberately absent: lease fields (phase 4 adds them), `MaxAttempts` (the `Policy` owns it — one place, not two), and `NextAttemptAt` (the retry `Decision` in Task 7 carries it; the store persists it in phase 3). Do not add fields on speculation. Every field here has a consumer in this plan or the next phase.

`Attempt` counts **completed** attempts. It is zero for a job that has never run, and is incremented by the caller when a job moves `Leased -> Failed`. Task 7 depends on this definition, so fix it in your head now.

- [ ] **Step 1: Write the failing test**

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

The second assertion in each test is the one that matters. A rejected transition must leave the job untouched — a method that returns an error *and* mutates is how you get corrupt state that only shows up three layers away.

- [ ] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestJob -v`

Expected: build failure — `undefined: Job`.

- [ ] **Step 3: Write the minimal implementation**

Create `internal/job/job.go`:

```go
package job

import "time"

// Job is a single unit of work: what to run, what to run it on, and where it
// has got to. Storage and transport layers map to and from this type; nothing
// in here knows how it is persisted or serialized.
type Job struct {
	// ID uniquely identifies the job.
	ID string
	// Type selects the executor that runs the job.
	Type string
	// Payload is the executor's opaque input.
	Payload []byte
	// State is the job's current lifecycle position.
	State State
	// Attempt counts completed attempts. It is zero before the first run.
	Attempt int
	// CreatedAt and UpdatedAt are set by the storage layer.
	CreatedAt time.Time
	UpdatedAt time.Time
}

// To moves the job to the next state. When the transition is not permitted it
// returns an error wrapping ErrInvalidTransition and leaves the job unchanged.
func (j *Job) To(next State) error {
	if err := Transition(j.State, next); err != nil {
		return err
	}
	j.State = next
	return nil
}
```

- [ ] **Step 4: Run the tests and watch them pass**

Run: `go test ./internal/job/ -v`

Expected: `PASS`.

- [ ] **Step 5: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/job.go internal/job/job_test.go
git commit -m "feat(job): add Job struct and guarded state transitions"
```

**Phase 1 is complete at this commit.** You have a fully tested state machine that imports nothing but `time`, `fmt`, `slices`, and `errors`. Ask for a review before starting Task 4.

---

### Task 4: `Clock` and the backoff sequence

**Files:**
- Create: `internal/job/retry.go`
- Test: `internal/job/retry_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks yet.
- Produces: `type Clock interface { Now() time.Time }`; `type Policy struct` with fields `Base time.Duration`, `Max time.Duration`, `MaxAttempts int`, `Clock Clock`, `Jitter func() float64`; `func (p Policy) Backoff(attempt int) time.Duration`.

Backoff is **exponential with full jitter**: the delay is drawn uniformly from `[0, min(Max, Base * 2^(attempt-1)))`. Full jitter, rather than plain exponential, because a thousand jobs failing against the same downed dependency must not retry in lockstep and knock it over again the moment it recovers.

Jitter is injected as a `func() float64` rather than a seeded `*rand.Rand`. That lets this task's tests pin the random draw to an exact value and assert the *ceiling* sequence precisely, while Task 5 tests the randomness itself separately. Injecting a seed would couple your assertions to the internals of Go's PRNG.

- [ ] **Step 1: Write the failing test**

Create `internal/job/retry_test.go`:

```go
package job

import (
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

Add `"strconv"` to the imports:

```go
import (
	"strconv"
	"testing"
	"time"
)
```

The `{50, ...}` case is the important one. A naive `Base << (attempt-1)` overflows `time.Duration` at around attempt 55 and starts returning negative delays — a job that retries instantly, forever. This test forbids that implementation.

- [ ] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestBackoff -v`

Expected: build failure — `undefined: Policy`.

- [ ] **Step 3: Write the minimal implementation**

Create `internal/job/retry.go`:

```go
package job

import (
	"math/rand/v2"
	"time"
)

// Clock reports the current time. Production code leaves Policy.Clock nil and
// gets the wall clock; tests inject a fixed implementation so that scheduling
// can be asserted without waiting.
type Clock interface {
	Now() time.Time
}

// Policy decides what becomes of a job after a failed attempt: another try
// after a delay, or the dead letter state.
type Policy struct {
	// Base is the delay window after the first failure.
	Base time.Duration
	// Max caps the delay window however many attempts have been made.
	Max time.Duration
	// MaxAttempts is the number of attempts after which a job is buried.
	MaxAttempts int

	// Clock defaults to the wall clock when nil.
	Clock Clock
	// Jitter returns a value in [0,1). It defaults to rand.Float64 when nil.
	Jitter func() float64
}

// Backoff returns how long to wait before the next attempt, given that
// attempt attempts have already been made. The delay is drawn uniformly from
// [0, min(Max, Base*2^(attempt-1))) — exponential backoff with full jitter,
// so that jobs failing against a common dependency do not retry in lockstep.
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	// Doubling in a loop that stops at Max cannot overflow, unlike shifting.
	window := p.Base
	for i := 1; i < attempt && window < p.Max; i++ {
		window *= 2
	}
	if window > p.Max {
		window = p.Max
	}

	return time.Duration(p.jitter() * float64(window))
}

func (p Policy) jitter() float64 {
	if p.Jitter == nil {
		return rand.Float64()
	}
	return p.Jitter()
}
```

Two idioms here worth internalizing. `Clock` is declared in the package that *consumes* it, not exported from some `clock` package — Go interfaces belong at the point of use, and this one has exactly one method because that is all the consumer needs. And the nil-check helpers (`p.jitter()`, and `p.now()` in Task 7) make the zero value partially useful: a caller who does not care about determinism sets nothing.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `go test ./internal/job/ -run TestBackoff -v`

Expected: `PASS`, ten subtests.

- [ ] **Step 5: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/retry.go internal/job/retry_test.go
git commit -m "feat(job): add retry Policy with exponential backoff"
```

---

### Task 5: Prove the jitter is actually jittering

**Files:**
- Modify: `internal/job/retry_test.go` (add two tests)

**Interfaces:**
- Consumes: `Policy`, `Policy.Backoff` from Task 4.
- Produces: nothing new. This task adds tests only.

Task 4 pinned the random draw, which means nothing so far proves the jitter exists. A `Backoff` that ignored `p.Jitter` entirely and returned the ceiling would pass every test written so far. This task closes that hole and checks that the nil default works.

- [ ] **Step 1: Write the failing tests**

Append to `internal/job/retry_test.go`:

```go
func TestBackoffAppliesJitter(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, Jitter: func() float64 { return 0.25 }}

	if got, want := p.Backoff(3), time.Second; got != want {
		// window for attempt 3 is 4s; 0.25 of it is 1s.
		t.Errorf("Backoff(3) = %v, want %v", got, want)
	}
}

func TestBackoffStaysWithinWindowWithRealRandomness(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second} // nil Jitter: real randomness

	const window = 400 * time.Millisecond // attempt 3
	var distinct int
	seen := make(map[time.Duration]bool)

	for range 1000 {
		d := p.Backoff(3)
		if d < 0 || d >= window {
			t.Fatalf("Backoff(3) = %v, want within [0, %v)", d, window)
		}
		if !seen[d] {
			seen[d] = true
			distinct++
		}
	}

	if distinct < 100 {
		t.Errorf("got %d distinct delays over 1000 draws, want at least 100 — jitter looks constant", distinct)
	}
}
```

The second test is a property test, not an example test: it asserts an invariant that must hold for every draw (in range) plus a statistical fact that fails loudly if the jitter is stubbed out (many distinct values). The threshold of 100 out of 1000 is far below what real randomness produces and far above what a constant produces, so it will not flake.

- [ ] **Step 2: Run the tests and watch them fail — or not**

Run: `go test ./internal/job/ -run TestBackoff -v`

Expected: **PASS**, because Task 4's implementation is already correct.

This is the one place in the plan where a test is written after the code it covers, and it is worth understanding why that is acceptable here rather than a lapse in discipline. These tests do not drive a new behavior; they close a gap in the *existing* test suite. To confirm they actually bite, deliberately break the implementation: temporarily change `return time.Duration(p.jitter() * float64(window))` to `return window`, re-run, and watch both new tests fail. Then revert. A test you have never seen fail is a test you cannot trust.

- [ ] **Step 3: Verify the tests bite, then revert**

```bash
# edit retry.go: replace the return in Backoff with `return window`
go test ./internal/job/ -run TestBackoff
# expected: FAIL on TestBackoffAppliesJitter and TestBackoffStaysWithinWindowWithRealRandomness
# revert the edit
go test ./internal/job/ -run TestBackoff
# expected: PASS
```

- [ ] **Step 4: Commit**

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

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `func Permanent(err error) error`; `func IsPermanent(err error) bool`.

Not every failure deserves a retry. A `503` from a job's target is worth trying again; a `400` never will be, and retrying it five times with backoff wastes capacity and delays the dead-letter signal an operator needs. Executors mark the hopeless ones with `Permanent`, and Task 7's policy reads that.

This is the `errors.As` half of Go's error model, distinct from the `errors.Is` sentinels in Task 2: sentinels answer "which error is this", type-based wrapping answers "what *kind* of error is this", and this is a kind.

- [ ] **Step 1: Write the failing test**

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

`TestPermanentPreservesTheUnderlyingError` is the one that constrains the design. A marker that swallows its cause breaks every `errors.Is` check upstream, which is the classic way custom error types make a codebase worse instead of better.

`TestPermanentOfNilIsNil` matters because `Permanent(nil)` returning a non-nil error would make the extremely common `return Permanent(doThing())` silently report failure on success — the same trap as returning a nil pointer through a non-nil interface.

- [ ] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run Permanent -v`

Expected: build failure — `undefined: Permanent`.

- [ ] **Step 3: Write the minimal implementation**

Create `internal/job/permanent.go`:

```go
package job

import "errors"

// permanentError marks a failure as not worth retrying. It is unexported: the
// only supported way to create one is Permanent, and the only supported way to
// detect one is IsPermanent, which keeps the representation free to change.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }

// Unwrap exposes the cause so that errors.Is and errors.As still see through
// the marker.
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks err as terminal: a job failing with it is buried rather than
// retried. It returns nil when err is nil, so that Permanent(f()) is safe.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err, or any error it wraps, was marked by
// Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
```

`permanentError` is a value type rather than a pointer, so `errors.As` matches on the value and two markings of the same cause compare equal. `Error()` delegates rather than decorating, because "permanent" is metadata for the retry policy, not something an operator reading a log needs prefixed onto every message.

- [ ] **Step 4: Run the tests and watch them pass**

Run: `go test ./internal/job/ -run Permanent -v`

Expected: `PASS`, seven subtests.

- [ ] **Step 5: Commit**

```bash
gofmt -l . && go vet ./... && go test -race ./...
git add internal/job/permanent.go internal/job/permanent_test.go
git commit -m "feat(job): distinguish permanent failures from retryable ones"
```

---

### Task 7: The retry decision

**Files:**
- Modify: `internal/job/retry.go` (add `Decision`, `OnFailure`, `now`)
- Test: `internal/job/retry_test.go` (add `TestOnFailure*`)

**Interfaces:**
- Consumes: `Policy`, `Policy.Backoff`, `Clock` (Task 4); `Job`, `Job.Attempt` (Task 3); `Pending`, `Dead` (Task 1); `IsPermanent` (Task 6).
- Produces: `type Decision struct { State State; NextAttemptAt time.Time }`; `func (p Policy) OnFailure(j *Job, cause error) Decision`.

The rules, in order:

1. The cause is permanent → `Dead`.
2. `j.Attempt >= p.MaxAttempts` → `Dead`. (`Attempt` counts completed attempts, per Task 3, and is incremented by the caller before `OnFailure` runs. So with `MaxAttempts: 3`, a job that has just failed its third attempt has `Attempt == 3` and is buried.)
3. Otherwise → `Pending`, at `now + Backoff(j.Attempt)`.

`OnFailure` returns a `Decision` rather than mutating the job. The caller applies it inside the same database transaction that records the failure, and a function that reports a decision is trivially testable in a way that one which reaches out and mutates is not.

- [ ] **Step 1: Write the failing test**

Append to `internal/job/retry_test.go`:

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

Add `"errors"`, `"fmt"` and `"reflect"` to the imports of `retry_test.go`:

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

`TestOnFailureDoesNotMutateTheJob` compares whole structs, so it catches a mutation of *any* field, including ones added in later phases. It uses `reflect.DeepEqual` rather than `==` because `Job` contains `Payload []byte`, and a struct holding a slice is not comparable — `*j != before` would not compile. This is worth knowing before you hit it somewhere less forgiving: Go's comparability is a property of the type, not of the values, so a nil slice does not rescue it.

`TestOnFailureDecisionIsAppliable` is the seam between the two halves of this phase: it proves the retry policy only ever produces states the state machine will actually accept from `Failed`. Without it, the two components can drift apart and nothing notices until runtime.

- [ ] **Step 2: Run the test and watch it fail**

Run: `go test ./internal/job/ -run TestOnFailure -v`

Expected: build failure — `undefined: Decision`.

- [ ] **Step 3: Write the minimal implementation**

Append to `internal/job/retry.go`:

```go
// Decision is what a Policy concluded about a failed job. It is a value the
// caller applies — usually in the same transaction that records the failure —
// rather than a mutation the Policy performs itself.
type Decision struct {
	// State is Pending when the job will be retried, Dead when it will not.
	State State
	// NextAttemptAt is when the job becomes eligible again. It is the zero
	// time when State is Dead.
	NextAttemptAt time.Time
}

// OnFailure reports whether j should be retried after failing with cause. A
// job is buried when the cause is permanent or when its attempts are spent;
// otherwise it returns to Pending after a jittered backoff.
func (p Policy) OnFailure(j *Job, cause error) Decision {
	if IsPermanent(cause) || j.Attempt >= p.MaxAttempts {
		return Decision{State: Dead}
	}
	return Decision{
		State:         Pending,
		NextAttemptAt: p.now().Add(p.Backoff(j.Attempt)),
	}
}

func (p Policy) now() time.Time {
	if p.Clock == nil {
		return time.Now()
	}
	return p.Clock.Now()
}
```

- [ ] **Step 4: Run the whole suite**

Run: `go test -race ./... -v`

Expected: `PASS`. Every test from Tasks 1 through 7 runs.

- [ ] **Step 5: Verify the package really is dependency-free**

```bash
go list -deps ./internal/job | grep -v '^internal/' | grep '\.' || echo "stdlib only"
```

Expected: `stdlib only`. Any line containing a dot is a module path, meaning something outside the standard library crept in. This command is the mechanical form of the project's central invariant — run it at the end of every future phase that touches this package.

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
- Seven commits, one per task.

## What Phase 3 Will Need From This

So you can see why the shape above was chosen:

- `store.Enqueue` writes a `Job` with `State: Pending, Attempt: 0`.
- `store.Get` returns `ErrNotFound` — already defined here so that the store does not invent a competing one.
- The lease reaper calls `j.To(Pending)` on expired leases; the `Leased -> Pending` edge exists for it.
- The dispatcher calls `p.OnFailure(j, cause)` and writes `Decision.State` and `Decision.NextAttemptAt` in one `UPDATE`, which is why `Decision` is a value rather than a mutation.
- Phase 4 adds `LeaseExpiresAt` and `LeasedBy` to `Job`. They are absent now because nothing uses them yet.
