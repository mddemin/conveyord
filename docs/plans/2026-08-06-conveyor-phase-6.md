# Phase 6 — The SSE hub and the live stream

**Packages:** `internal/hub` (new), `internal/job` (one type moves in), `internal/httpapi` (two routes added)
**Done when:** `curl -N localhost:8080/events` prints a line the moment a job is enqueued
in another terminal, `go test -race -count=2 ./...` is clean, and a subscriber that never
reads a single event cannot slow down or stall anybody else.

Every phase so far has had a property that made it tractable: **the bug was in what the
code said.** You could read a handler top to bottom and reason about it. Phase 6 removes
that. Here the bug is usually in *when two goroutines happen to run*, which means:

- The code reads correctly and is wrong.
- The test passes ninety-nine times and fails on the hundredth, on a different machine.
- The compiler cannot help you. `go vet` cannot help you. Only `-race` and careful design can.

That is the whole reason this phase exists, and it is why the spec calls the hub
"deliberately the trickiest component". Budget more time than phase 5 took.

## How to read this plan

Phase 5 assumed a lot of background. This one front-loads it instead: **Part 0 is a
primer** with small programs and their real output, run before writing this document. Read
it first even if the concepts are familiar — the tasks refer back to numbered sections in
it, and the whole design of the hub falls out of five facts about channels.

Format as before: **test code is given verbatim and is the specification.** Implementation
bodies are described as requirements and hints, never as code you can paste. Where you see
a code block inside a task, it is illustrating a mechanism, not handing you the answer.

---

## Global constraints

Carried forward from phase 5, plus:

- `internal/hub` imports **stdlib only**, plus `internal/job` for the event type. No `net/http`, no `store`, no logging library. A hub that knows what an HTTP response is cannot be tested without one.
- **No `time.Sleep` in any test.** Not one. Sleeping to "let the goroutine catch up" is how you get a suite that passes locally and flakes in CI. Every test synchronizes with a channel, a `sync.WaitGroup`, or a value read back from the thing under test.
- **`-race` on every run**, and `-count=2` at minimum for the hub package. A concurrency test that has only ever run once has told you very little.
- **Every goroutine you start has a written exit condition.** In a comment, at the `go` statement. "Exits when ctx is done" or "exits when ch is closed". If you cannot write the sentence, you have a leak.
- The hub never blocks. Not on a slow subscriber, not on a disconnected one, not on a hundred of them.
- No global hub. It is a value created in `main` and injected, exactly like the store and the logger.

---

## Part 0 — The primer

Everything below was run; the output is real, not remembered.

### 0.1 A channel is a queue with a fixed size, and sending blocks when it is full

```go
ch := make(chan int, 2)
ch <- 1
ch <- 2
fmt.Printf("len=%d cap=%d\n", len(ch), cap(ch))
```
```
len=2 cap=2 — a third send here would block forever
```

`make(chan int)` with no size is capacity **0**: a send blocks until another goroutine is
ready to receive, at the same instant. That is the default and it is the wrong choice for a
broadcast hub — one unresponsive receiver would freeze the sender permanently.

`make(chan int, 16)` lets the sender get 16 items ahead. It does **not** make the send
non-blocking; it moves the wall further away. This distinction matters: a buffered channel
is not a solution to the slow-subscriber problem, only a mitigation. The solution is 0.2.

### 0.2 `select` with `default` is a send that gives up

```go
full := make(chan int, 1)
full <- 1                    // buffer now full
for i := 2; i <= 3; i++ {
	select {
	case full <- i:
		fmt.Printf("sent %d\n", i)
	default:
		fmt.Printf("buffer full, dropped %d\n", i)
	}
}
```
```
buffer full, dropped 2
buffer full, dropped 3
```

`select` normally blocks until one of its cases can proceed. A `default` clause changes it
into "if none of these can proceed *right now*, do this instead" — and it returns
immediately, always.

**This is the single most important construct in the phase.** It converts "send this event"
from an operation that can take unbounded time into one that takes nanoseconds and might
fail. Trading delivery for liveness is the trade the spec calls "a reusable career-long
pattern", and the whole hub is built on it.

### 0.3 Closing: who may, and what happens after

```go
c := make(chan string, 1)
c <- "x"
close(c)
v, ok := <-c    // "x"  true   — buffered values are still delivered
v, ok = <-c     // ""   false  — zero value, returns instantly, forever
```
```
first  recv: "x" ok=true (buffered value still delivered)
second recv: "" ok=false (zero value, never blocks)
```

Closing does not discard buffered values, and it does not "destroy" the channel. It marks
it: receivers drain what is there, then get the zero value with `ok == false` immediately,
as many times as they ask. That is what makes `for v := range ch` terminate.

The two ways to crash:

```
send on closed:  recovered: send on closed channel
double close:    recovered: close of closed channel
```

Both are panics, both are runtime-only, and both are failure mode 1 from the spec. The rule
that prevents them:

> **Only the sender closes, and only once.** If there are many senders, nobody closes —
> use another signal instead.

The hub has many publishers and one channel per subscriber, so "the sender" is ambiguous.
Task 2 resolves it with a lock, and the reasoning there is the heart of the phase.

### 0.4 A blocked send is a leaked goroutine

```go
before := runtime.NumGoroutine()
stuck := make(chan int)             // unbuffered, nobody will ever receive
go func() { stuck <- 1 }()
```
```
goroutines 1 -> 2; that one is parked forever
```

No error, no panic, no log line. The goroutine parks on the send and stays until the
process exits, holding everything it captured — including, in a real hub, a whole request's
worth of memory. A thousand disconnected browser tabs is a thousand of these.

This is failure mode 2. Nothing detects it at runtime; `goleak` detects it in tests, which
is why phase 7 adds it.

### 0.5 `sync.Mutex` versus "share by communicating"

Go's proverb is *don't communicate by sharing memory; share memory by communicating*, and
beginners read it as "channels always, mutexes never." That is wrong, and the spec is
explicit about it: the subscriber registry uses a mutex.

Use a **channel** when you are handing off ownership of a value, or coordinating the
sequence of two goroutines. Use a **mutex** when several goroutines read and write one
shared data structure and no ownership changes hands.

The hub's registry — `map[int]*subscriber` — is the second thing. Modelling it with
channels means inventing a manager goroutine, a command type per operation, and a reply
channel for anything that returns a value. That is more code, more state, and one more
goroutine that can be leaked, to protect a map. `sync.RWMutex` is nine characters.

The events themselves flow over channels, because that *is* a handoff. Both tools, each
where it fits.

### 0.6 What `-race` does and does not do

Two goroutines touching one `int`, no synchronization:

```
--- without -race ---
ok  	ch	0.001s

--- with -race ---
WARNING: DATA RACE
Previous write at 0x00c000018368 by goroutine 8:
--- FAIL: TestRaceIsFoundOnlyWhenItHappens
```

Note the first line. The unsynchronized test **passed**. It will keep passing, on your
machine, until it doesn't.

What the detector does: instruments memory accesses and reports when two goroutines touch
the same address without a happens-before relationship between them, and at least one
writes. What it does **not** do: find races on code paths that did not execute during that
run. It reports what happened, not what could happen.

Consequences for this phase:

- `-race` is not optional; treat a run without it as not having run.
- `-count=2` and a test that actually creates contention (several goroutines, several events) are what give the detector something to see.
- A green `-race` run is evidence, not proof. Design still matters.

### 0.7 Testing concurrency without sleeping

`time.Sleep(100 * time.Millisecond)` in a test is a bet that the machine is not busy. CI
machines are busy. Four techniques replace it:

**Read the value back.** The strongest option. If a test wants "the subscriber is gone",
have the hub expose `Len()` and assert on it — synchronized by the hub's own lock, so no
race and no timing.

**Receive from a channel.** Blocking on `<-ch` is a synchronization point. It waits exactly
as long as necessary and no longer.

**`sync.WaitGroup` for fan-out**:

```go
var wg sync.WaitGroup
for i := 1; i <= 3; i++ {
	wg.Add(1)
	go func() { defer wg.Done(); results <- i * i }()
}
wg.Wait()
close(results)   // safe: every sender has finished
```
```
all senders done, channel drained, sum=14
```

`wg.Wait()` returning is proof all three goroutines finished — which is also what makes the
`close` legal under 0.3's rule.

**A deadline, only for proving something does *not* happen.** To assert "Publish did not
block", you need an upper bound:

```go
done := make(chan struct{})
go func() { h.Publish(ev); close(done) }()
select {
case <-done:
case <-time.After(2 * time.Second):
	t.Fatal("Publish blocked on a subscriber that never reads")
}
```

That is not a sleep: the happy path takes microseconds, and the timeout only fires on
genuine failure. Make it generous — two seconds, not fifty milliseconds. A short timeout
here is a flake generator; a long one costs nothing when the code is correct.

---

## What Server-Sent Events actually are

Before designing anything: SSE is a **plain-text streaming format over an ordinary HTTP
GET**. No handshake, no new protocol, no library. The server returns
`Content-Type: text/event-stream` and then never finishes the response body.

Here is a real capture — server on the left of the arrow, bytes on the wire on the right:

```
Content-Type: "text/event-stream"

wire: "event: state"
wire: "data: {\"state\":\"pending\"}"
wire: ""
wire: ": heartbeat"
wire: ""
wire: "id: 7"
wire: "event: state"
wire: "data: {\"state\":\"leased\"}"
wire: ""
```

The whole grammar:

| Line | Meaning |
|---|---|
| `data: <text>` | the payload. Multiple `data:` lines in one event are joined with `\n`. |
| `event: <name>` | optional event type; the browser dispatches it as a named listener |
| `id: <value>` | optional; the browser sends it back as `Last-Event-ID` after a reconnect |
| `retry: <ms>` | optional; tells the browser how long to wait before reconnecting |
| `: <text>` | a comment. Ignored by every client — which is what makes it a heartbeat. |
| **empty line** | **ends the event and triggers dispatch** |

That last row is the one people get wrong. An event is not sent because you wrote `data:`;
it is sent because you wrote the blank line after it. `\n\n` at the end of every event, or
the client sits waiting forever with a complete event in its buffer.

Three properties that shape the design:

**It is one-way.** Server to client only. A client that wants to say something makes a
normal request. This is why SSE is right for job events and WebSockets would be overkill.

**The browser reconnects automatically.** `EventSource` retries on its own, and sends
`Last-Event-ID` if you supplied `id:`. You get resumption for free *if* you emit ids —
worth doing, and worth knowing you are not obliged to honour them in this phase.

**It needs flushing.** Go buffers response writes. Without an explicit `Flush()` after each
event, the client receives nothing until the buffer fills or the handler returns — and the
handler never returns. This is the single most common SSE bug, and phase 5 already paid for
it twice: the `recorder` forwards `Flush`, and `withTimeout` is per-route precisely so it
cannot wrap this endpoint. Those two decisions now get spent.

---

## The problem being solved

A hub is `map[id]chan Event` plus a broadcast loop. Ten lines. It has three failure modes,
and you will hit all three:

1. **Send on a closed channel.** A subscriber disconnects and closes its channel while a publisher is mid-broadcast. Panic, in a goroutine, taking down the process (0.3).

2. **Goroutine leak.** A client disconnects and nothing unregisters it. Its channel fills, the publisher's non-blocking send starts dropping, the memory is never freed, and `Len()` climbs forever (0.4).

3. **System-wide stall.** One client on hotel wifi stops reading. Its buffer fills. The broadcast loop blocks on the send. **Every other subscriber stops receiving**, and the enqueue handler that called `Publish` stops responding. One slow reader takes down the API.

Number 3 is the one worth internalizing. The fix — buffered channel plus `select`/`default`
plus a dropped counter — is small, and the reasoning generalizes to every fan-out system
you will ever write.

---

## Task 1 — `Event` moves into the domain, and the hub takes shape

**Goal:** an event type in the right package, and a hub that can subscribe, publish, and
unsubscribe correctly with no concurrency subtleties yet.

### Steps

1. Move `store.Event` to `internal/job`.
2. Create `internal/hub/hub.go`.
3. Test the hub with no HTTP anywhere.
4. Commit.

### The move, and why now

`internal/store/store.go` currently says:

```go
// Event is one recorded step in a job's lifecycle.
// Lives here while storage is the only consumer; revisit if transports need it.
type Event struct { ... }
```

You wrote that note in phase 3. This is the revisit: a transport now needs it, and a type
that both `store` and `hub` depend on cannot live in `store` unless `hub` imports storage —
which the constraints forbid.

Move it to `internal/job` as `job.Event`, unchanged. `store.Events` returns `[]job.Event`.

One thing to check afterwards, because it looks like it should break: `store` collects rows
with `pgx.RowToStructByName[Event]`, which becomes `pgx.RowToStructByName[job.Event]`. That
is a *generic instantiation in package store*, not an import in package job — `internal/job`
still has zero non-stdlib dependencies. Verify it with the `go list -deps` line from the
checklist rather than assuming; this is exactly the kind of accidental dependency the
layout exists to prevent.

### The hub's shape

```go
type Hub struct { /* ... */ }

func New() *Hub

// Subscribe returns a channel of events and a function that stops the
// subscription. jobID filters to one job; "" receives every event.
func (h *Hub) Subscribe(jobID string) (<-chan job.Event, func())

// Publish delivers ev to every matching subscriber. It never blocks.
func (h *Hub) Publish(ev job.Event)

// Len reports the number of live subscriptions. For tests and /healthz.
func (h *Hub) Len() int
```

Four decisions worth understanding, because each is a fork you could take differently:

**`Subscribe` returns a cancel closure, not an ID.** With an ID, the caller has to store it,
and forgetting to call `Unsubscribe(id)` leaks. With a closure, the caller writes
`defer cancel()` on the next line and the compiler-adjacent habit does the work. Same shape
as `context.WithCancel`, so it reads as familiar.

**The returned channel is receive-only** (`<-chan`). The caller cannot close it, cannot send
on it, and therefore cannot cause failure mode 1. The type system is doing a piece of the
safety argument for free.

**The cancel closure must be idempotent.** It will be called twice — once by the handler's
`defer`, once by whatever else you add later. Two `close` calls panic (0.3), so removal from
the map must be conditional: check presence, delete, close. All under the lock.

**Filtering happens in `Publish`, not in the subscriber.** The alternative — send everything
to everyone and let each subscriber discard — wastes a slot in every buffer for events that
subscriber will never want, which makes the slow-subscriber problem worse for no benefit.

### Requirements

- `sync.RWMutex` guarding a `map` of live subscribers. Publish takes the read lock; Subscribe and the cancel closure take the write lock.
- Subscriber identity: a monotonically increasing `int` is enough. Do not use the channel as a map key — legal, but it reads badly and makes debugging harder.
- Buffer size a named constant, not a literal. 16 or 32. Task 2 explains the tradeoff; pick one now and write the reason next to it.
- `Publish` on a hub with no subscribers is a no-op, not an error.
- `New` returns a ready hub. No `Start`, no background goroutine. **The hub owns no goroutines at all** — publishers call it on their own goroutine, subscribers read on theirs. Fewer goroutines is fewer leaks, and a hub with no lifecycle needs no shutdown.

### Tests

These live in `internal/hub/hub_test.go` as **`package hub`**, not `package hub_test` — so
they call `New()` unqualified and can reach `bufferSize` and the drop counter without those
becoming public API. Task 2 needs both, and a constant exported only so a test can read it
is a test leaking into the design. The tradeoff is real but small: an in-package test cannot
prove the exported surface is sufficient on its own. Task 3's tests, which drive the hub
through the HTTP handler from `package httpapi`, cover that.

```go
func TestSubscribeReceivesAPublishedEvent(t *testing.T) {
	h := New()
	ch, cancel := h.Subscribe("")
	defer cancel()

	want := job.Event{JobID: "j1", State: job.Pending}
	h.Publish(want)

	select {
	case got := <-ch:
		if got != want {
			t.Errorf("received %+v, want %+v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no event arrived")
	}
}

// A subscription for one job must not receive another job's events. Without
// this, /jobs/{id}/events is /events with a misleading URL.
func TestSubscribeFiltersByJobID(t *testing.T) {
	h := New()
	ch, cancel := h.Subscribe("j1")
	defer cancel()

	h.Publish(job.Event{JobID: "j2", State: job.Pending})
	h.Publish(job.Event{JobID: "j1", State: job.Leased})

	select {
	case got := <-ch:
		if got.JobID != "j1" {
			t.Errorf("received an event for %s, want only j1", got.JobID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the matching event never arrived")
	}
}

func TestEveryoneReceivesABroadcast(t *testing.T) {
	h := New()
	const n = 5

	chans := make([]<-chan job.Event, n)
	for i := range chans {
		ch, cancel := h.Subscribe("")
		defer cancel()
		chans[i] = ch
	}

	h.Publish(job.Event{JobID: "j1", State: job.Pending})

	for i, ch := range chans {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Errorf("subscriber %d never received the broadcast", i)
		}
	}
}

// Cancel must remove the subscription, not merely stop reading it. A hub that
// keeps delivering to a cancelled subscriber leaks a buffer per dead client.
func TestCancelStopsDeliveryAndFreesTheSlot(t *testing.T) {
	h := New()
	ch, cancel := h.Subscribe("")

	if h.Len() != 1 {
		t.Fatalf("Len is %d after one Subscribe, want 1", h.Len())
	}

	cancel()

	if h.Len() != 0 {
		t.Errorf("Len is %d after cancel, want 0", h.Len())
	}
	h.Publish(job.Event{JobID: "j1", State: job.Pending})

	// A cancelled subscriber's channel is closed, so this returns immediately
	// with the zero value rather than blocking. Receiving an actual event here
	// would mean the hub still holds a reference to it.
	if got, ok := <-ch; ok {
		t.Errorf("received %+v after cancel, want a closed channel", got)
	}
}

// The handler will defer cancel() and phase 7 will add another caller. Two
// closes of one channel is a panic, so this must be safe.
func TestCancelIsIdempotent(t *testing.T) {
	h := New()
	_, cancel := h.Subscribe("")

	cancel()
	cancel()
	cancel()

	if h.Len() != 0 {
		t.Errorf("Len is %d, want 0", h.Len())
	}
}

func TestPublishWithNoSubscribers(t *testing.T) {
	h := New()
	h.Publish(job.Event{JobID: "j1", State: job.Pending}) // must not panic or block
	if h.Len() != 0 {
		t.Errorf("Len is %d, want 0", h.Len())
	}
}
```

---

## Task 2 — The slow subscriber

**Goal:** the part of the phase that matters. Make it impossible for one subscriber to
affect any other.

### Steps

1. Make the send in `Publish` non-blocking.
2. Count what you drop, per subscriber.
3. Decide and implement a policy for a persistently lagging subscriber.
4. Commit.

### The mechanism

From 0.2, applied to one subscriber inside the broadcast loop:

```go
select {
case sub.ch <- ev:
	// delivered
default:
	// the subscriber is behind; the event is gone
}
```

Three lines. What they buy: `Publish` now runs in time proportional to the number of
subscribers and nothing else. No subscriber's behaviour can extend it. The enqueue handler
that calls `Publish` cannot be slowed by a browser tab on a train.

What they cost: **at-most-once delivery to a slow client.** SSE is a live feed, so this is
the right trade — a client that has fallen behind wants the current state, not a backlog
from four minutes ago. It is also why the durable record stays in the `job_events` table:
the stream is a convenience, Postgres is the truth. Say that in a comment; it is the
justification for the whole design.

### Why the lock must be held across the send

This is the subtle part, and it is worth sitting with.

Ordinary advice says never do a potentially-blocking operation while holding a lock. So the
instinct is to copy the subscriber list under the lock, release it, then send. **Do not.**

If the lock is released before the send, this interleaving exists:

```
publisher                     unsubscriber
---------                     ------------
copy the list under RLock
release RLock
                              take Lock
                              delete from map
                              close(sub.ch)
                              release Lock
send on sub.ch  ← panic: send on closed channel
```

Failure mode 1, exactly. Holding the read lock across the send makes it impossible: the
cancel closure needs the *write* lock to close the channel, and cannot get it while any
publisher holds the read lock.

The reason this is safe — and the reason it is not the anti-pattern it looks like — is 0.2.
The send is non-blocking *by construction*. The critical section's duration is bounded by
the number of subscribers times a few nanoseconds, with no dependence on any external
party. **The `default` clause is what makes holding the lock acceptable, and holding the
lock is what makes closing safe.** Those two decisions only work together; take either one
away and the design breaks in a different place.

Write that reasoning into the code as a comment. It is the least obvious correct thing in
the package, and the next person to read it — including you in phase 9 — will otherwise
"fix" it.

### The lagging policy

Counting drops is not optional: a system that silently discards data is one you cannot
operate. Give each subscriber a counter, incremented in the `default` branch.

Then decide what to do when it grows. Three defensible answers:

**Count and expose.** Simplest. A `Dropped()` method, logged when the subscription ends.
An operator sees "this client missed 4,000 events" after the fact.

**Count and disconnect.** Above a threshold, close the subscription and let the client
reconnect from scratch. Justification: a client 4,000 events behind is not "slightly
delayed", it is broken, and holding its buffer helps nobody. Browsers reconnect
automatically, so this is close to free. Requires care — you are now closing the channel
from inside `Publish`, which is legal only because you hold the lock, and needs the write
lock rather than the read lock. Think about whether that upgrade is worth the complexity.

**Mark the subscriber and let it resync.** Set a `lagged bool` on the `sub` in the same
`default` branch. The buffer being full is not an obstacle, because nothing is sent from
here: the *consumer* — the SSE handler — checks the flag on its next read, emits an
`event: lagged` frame, and clears it. The client's handler for that frame is one
`GET /jobs/{id}`, which returns exact current state.

This is the one to build. The argument against the other two is the same argument:
**a buffer is a guess about how far behind is acceptable; a resync is exact.** A bigger
buffer still loses everything at depth 17, and a drop counter tells an operator after the
fact what the client needed to know at the time. The marker is correct at any lag depth,
costs one bool, and is the same mechanism you would need anyway for a client that
disconnected and reconnected.

Getting the flag *out* to the consumer is the part with a design decision in it. The
handler holds a `<-chan job.Event` and a `func()`; neither can answer "did I lag?". Which
means `Subscribe`'s two return values have run out, and the honest fix is one type instead
of a growing tuple:

```go
type Subscription struct {
	C <-chan job.Event
	// ...
}

func (s *Subscription) Lagged() bool // true once, then clears
func (s *Subscription) Close()       // idempotent; replaces the cancel closure

func (h *Hub) Subscribe(jobID string) *Subscription
```

That folds cancel in rather than adding to it, so the call site stays two lines
(`s := h.Subscribe(id); defer s.Close()`). Cost: Task 1's tests change mechanically — `ch`
becomes `s.C`, `cancel()` becomes `s.Close()`. Do that rename as the first commit of this
task, before adding any behaviour, so the diff that changes semantics is small enough to
review.

`Lagged()` clearing on read is deliberate: the handler emits one `event: lagged` frame per
lag episode, not one per drop. It takes the write lock, since it mutates.

Keep the per-subscriber counter alongside it — the flag says *that* you lagged, the counter
says *how badly*, which is what goes in the log line when the subscription ends. Leave a
`ponytail:` marker naming disconnect-above-threshold as the upgrade; it is better behaviour
under real load and you have no load test yet to pick the threshold.

One property of drop-newest worth knowing, because it is the reason doing nothing is not an
option: the events most likely to be discarded are the *last* ones in a burst, and for a job
stream the last event is `succeeded` or `failed`. Drop that and the client's UI says
"running" forever on a job that finished. The lag marker fixes this; so would a ring buffer
that drops oldest, at the cost of building your own queue with its own readiness signal,
since a slice cannot appear in a `select`.

### Requirements

- Non-blocking send, read lock held across the whole broadcast loop, with the comment explaining why both are required.
- Per-subscriber drop counter and `lagged` flag, both fields on `sub`. Every access is already under `Hub.mu`, so plain `uint64`/`bool` — no `atomic` — but say so in a comment, because an unsynchronized-looking counter written from `Publish` will make the next reader stop and check.
- Hub-wide totals, if you want them, are a sum over the map under `RLock`. Do not keep a second counter on the `Hub`: two places to increment is two places to forget.
- The buffer size constant gets a comment with the actual tradeoff: too small drops events during ordinary scheduling jitter; too large delays the client's discovery that it is behind and costs `size × subscribers × sizeof(Event)` bytes of steady-state memory.

### Tests

```go
// The whole point of the phase. A subscriber that never reads must not slow
// down, block, or otherwise affect Publish.
func TestASubscriberThatNeverReadsCannotBlockPublish(t *testing.T) {
	h := New()
	_, cancel := h.Subscribe("") // deliberately never received from
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10_000; i++ {
			h.Publish(job.Event{JobID: "j1", State: job.Pending})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked: one silent subscriber has stalled the hub")
	}
}

// ...and must not stop anyone else from receiving.
func TestASlowSubscriberDoesNotStarveAFastOne(t *testing.T) {
	h := New()

	_, cancelSlow := h.Subscribe("") // never read
	defer cancelSlow()
	fast, cancelFast := h.Subscribe("")
	defer cancelFast()

	const n = 1000
	go func() {
		for i := 0; i < n; i++ {
			h.Publish(job.Event{JobID: "j1", State: job.Pending})
		}
	}()

	// The fast subscriber may miss events too — it is racing the publisher —
	// but it must keep receiving rather than being starved by the slow one.
	for i := 0; i < 10; i++ {
		select {
		case <-fast:
		case <-time.After(5 * time.Second):
			t.Fatalf("fast subscriber received only %d events before stalling", i)
		}
	}
}

func TestDropsAreCounted(t *testing.T) {
	h := New()
	ch, cancel := h.Subscribe("")
	defer cancel()
	_ = ch // never read, so the buffer fills

	for i := 0; i < bufferSize*4; i++ {
		h.Publish(job.Event{JobID: "j1", State: job.Pending})
	}

	if got := h.dropped(); got == 0 {
		t.Error("the hub dropped events silently; a discarded event nobody counted is a bug nobody can find")
	}
}

// The detector needs contention to have something to observe. Subscribe,
// publish, and cancel all racing is the shape the real server produces.
func TestConcurrentUse(t *testing.T) {
	h := New()
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				h.Publish(job.Event{JobID: "j1", State: job.Pending})
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				ch, cancel := h.Subscribe("")
				select {
				case <-ch:
				default:
				}
				cancel()
			}
		}()
	}

	wg.Wait()
	if h.Len() != 0 {
		t.Errorf("Len is %d after every subscriber cancelled, want 0", h.Len())
	}
}
```

`TestDropsAreCounted` reads the unexported `bufferSize` and `h.dropped()`, which is what
being in `package hub` buys: the test computes "enough to overflow" from the real constant
instead of hardcoding `1000`, which would silently stop testing anything the day the buffer
grows.

`h.dropped()` here is a sum over live subscriptions, not a field. Note what that makes it: a
*gauge*, not a monotonic counter — cancel a subscriber and its drops leave the total. Fine
for a test and for `/healthz`; wrong the day you point Prometheus at it. Write that down
next to the method rather than discovering it from a graph.

Run this task's tests with `-race -count=5`. If any of them is flaky, the design is wrong;
do not "fix" it by widening a timeout.

---

## Task 3 — The SSE endpoints

**Goal:** `GET /jobs/{id}/events` and `GET /events`, correct on the wire and clean on
disconnect.

### Steps

1. Add the handler to `internal/httpapi`.
2. Register both routes **without** the timeout wrapper.
3. Commit.

### The handler's shape

The whole thing is a loop over one channel, with a second case for the client going away:

```go
for {
	select {
	case ev, ok := <-ch:
		if !ok {
			return          // hub closed the subscription
		}
		// write the event, flush
	case <-r.Context().Done():
		return              // client disconnected
	}
}
```

Everything else is detail hung off that skeleton.

### Requirements

**Headers, before anything is written:**

```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Accel-Buffering: no
```

The last one is not standard HTTP — it tells nginx not to buffer the response. Without it,
an nginx in front of you re-introduces the exact bug flushing exists to prevent, and it will
work perfectly in every test and fail only in deployment. Cheap insurance.

Do not set `Content-Length`. You do not know it, and setting it wrong truncates the stream.

**Get the `Flusher` first, and fail loudly if it is missing:**

```go
flusher, ok := w.(http.Flusher)
if !ok {
	// this response writer cannot stream; 500 rather than serve a stream
	// that silently never arrives
}
```

Phase 5 makes this succeed: your `recorder` forwards `Flush`, and `withTimeout` is applied
per route in `routes()` so it is not in the way. This check is what turns a future
regression of either decision into an immediate, obvious failure instead of a
mysteriously-silent stream. `TestFlusherSurvivesTheChainButNotATimeout` already guards half
of it; this guards the other half at runtime.

**Subscribe before doing anything slow, and `defer cancel()` immediately.** The `defer` on
the line after `Subscribe` is the entire defence against failure mode 2. Every `return` in
the function — including the ones you add in three months — is then covered.

**Flush after every event.** And flush once immediately after the headers, before the first
event: that pushes the response header to the client so it knows the stream is open, rather
than leaving it waiting on a connection that has produced no bytes.

**Heartbeats.** A comment line (`: heartbeat\n\n`) on a ticker, every 15–30 seconds. Two
reasons, both real: proxies and load balancers close connections they consider idle, and
a client cannot otherwise distinguish "no events yet" from "the server died". This adds a
third case to the `select`.

Make the interval a field on `API` with a default set in `New`, not a package constant — a
test that has to wait 15 seconds for a heartbeat is a test nobody runs.

**Emit the lag marker.** On each pass of the loop, before writing the event, check
`sub.Lagged()`; if it is set, write a `event: lagged\ndata: {}\n\n` frame first. That frame
is the client's instruction to `GET /jobs/{id}` and rebuild, and it is the only thing
standing between a dropped terminal event and a UI stuck on "running" forever. Task 2 builds
the flag; this is where it becomes visible to anyone.

**Do not marshal inside the loop if you can avoid it.** Each event becomes the same JSON for
every subscriber, but each handler marshals it separately. Fine at this scale; note it as a
possible optimization and do not do it now. It is exactly the kind of thing to earn with a
benchmark in phase 12 rather than assume.

**Consider an initial snapshot.** A client connecting to `/jobs/{id}/events` for a job that
reached `succeeded` a second ago receives nothing, ever. Sending the job's current state as
the first event fixes that — but it introduces a gap: an event published between your read
of the current state and your `Subscribe` is lost. Subscribing *first* and then reading
state can instead produce a duplicate. Duplicates are recoverable by clients, gaps are not,
so subscribe first. **Or** decide the snapshot is out of scope for this phase and write down
that clients should `GET /jobs/{id}` alongside the stream. Both are fine; deciding by
accident is not.

### Routing

```go
mux.HandleFunc("GET /jobs/{id}/events", a.jobEvents)
mux.HandleFunc("GET /events", a.allEvents)
```

`HandleFunc`, not `Handle(timed(...))`. That visual difference from the three lines above it
is the enforcement mechanism for the decision phase 5 made, and it is worth a comment
pointing at it.

Sanity-check that `GET /jobs/{id}` and `GET /jobs/{id}/events` coexist: `ServeMux` scores
patterns by specificity, and the literal `/events` segment is more specific than a bare
wildcard, so the two do not conflict. Verify rather than trust — one test below does.

### Tests

```go
func TestEventStreamSetsStreamingHeaders(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() { defer close(done); api.ServeHTTP(w, r) }()

	waitFor(t, func() bool { return h.Len() == 1 })
	cancel()
	<-done

	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type is %q, want text/event-stream", ct)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control is %q, want no-cache", cc)
	}
}

func TestEventStreamWritesAnEventPerPublish(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() { defer close(done); api.ServeHTTP(w, r) }()

	waitFor(t, func() bool { return h.Len() == 1 })
	h.Publish(job.Event{JobID: "j1", State: job.Pending})
	waitFor(t, func() bool { return strings.Contains(w.Body.String(), "j1") })
	cancel()
	<-done

	body := w.Body.String()
	if !strings.Contains(body, "data: ") {
		t.Errorf("no data: line in the stream: %q", body)
	}
	// The blank line is what dispatches the event. Without it a client holds a
	// complete event in its buffer and never fires a listener.
	if !strings.HasSuffix(body, "\n\n") {
		t.Errorf("event is not terminated by a blank line: %q", body)
	}
}

// Failure mode 2. Every disconnected client must free its slot, or a server
// that has been up a week is holding a buffer for every tab ever opened.
func TestDisconnectUnsubscribes(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/events", nil).WithContext(ctx)

	done := make(chan struct{})
	go func() { defer close(done); api.ServeHTTP(httptest.NewRecorder(), r) }()

	waitFor(t, func() bool { return h.Len() == 1 })
	cancel()
	<-done

	if h.Len() != 0 {
		t.Errorf("Len is %d after the client disconnected, want 0", h.Len())
	}
}

func TestJobEventStreamFiltersByID(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/jobs/j1/events", nil).WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() { defer close(done); api.ServeHTTP(w, r) }()

	waitFor(t, func() bool { return h.Len() == 1 })
	h.Publish(job.Event{JobID: "j2", State: job.Pending})
	h.Publish(job.Event{JobID: "j1", State: job.Leased})
	waitFor(t, func() bool { return strings.Contains(w.Body.String(), "leased") })
	cancel()
	<-done

	if strings.Contains(w.Body.String(), "j2") {
		t.Errorf("another job's event reached the stream: %q", w.Body.String())
	}
}

// /jobs/{id} and /jobs/{id}/events must not shadow each other.
func TestJobRouteAndEventRouteCoexist(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{
		get: func(context.Context, string) (*job.Job, error) {
			return &job.Job{ID: "j1", Type: "resize", State: job.Pending}, nil
		},
	}, h)

	w := do(api, "GET", "/jobs/j1", "")

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("GET /jobs/{id} returned %q; the events route captured it", ct)
	}
}

// A streaming handler that cannot flush produces a response the client never
// sees. Fail at connection time instead.
func TestStreamRequiresAFlusher(t *testing.T) {
	h := hub.New()
	api := newTestAPIWithHub(t, stubStore{}, h)

	w := &unflushableRecorder{httptest.NewRecorder()}
	api.ServeHTTP(w, httptest.NewRequest("GET", "/events", nil))

	if w.Code < 500 {
		t.Errorf("status is %d; a writer that cannot flush must not get a stream", w.Code)
	}
}
```

Two helpers those tests need. `waitFor` is the no-sleep polling primitive — write it once,
in the test file:

```go
// waitFor blocks until cond is true or the test fails. It is the alternative
// to time.Sleep: the common case returns in microseconds, and the deadline
// only fires when something is genuinely broken.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		runtime.Gosched()
	}
}
```

`unflushableRecorder` embeds `*httptest.ResponseRecorder` and shadows `Flush` — you cannot
remove a method by embedding, so give it a `Flush` field or wrap it in a struct that only
promotes the three `ResponseWriter` methods. Work out which; it is a good exercise in the
embedding rules from phase 5.

`newTestAPIWithHub` is the third constructor variant. Three overlapping test constructors is
the point at which to consider a small options struct instead — your call, but notice the
smell.

---

## Task 4 — Publishing real events

**Goal:** connect the hub to something that actually changes state.

### Steps

1. Decide where `Publish` is called from.
2. Wire it.
3. Commit.

### The decision

Nothing publishes yet, and there is exactly one thing in the system that changes a job's
state before phase 7: `Enqueue`. So the question is narrow, but the answer sets a pattern
you will extend four times in later phases.

**Option A — publish in the handler.** After `a.store.Enqueue` succeeds, call
`a.hub.Publish(...)`. Two lines, no new types.
*Against:* every future writer of state must remember. Phase 7's dispatch loop and phase 9's
gRPC completion path are not handlers, so they will each need their own reminder. Forgetting
produces a stream that is silently incomplete.

**Option B — a publishing decorator around `Store`.** A type implementing `httpapi.Store`
that delegates to the real store and publishes on success.
*For:* the API is unchanged, and anything using the decorated store gets events
automatically.
*Against:* the decorator has to know which methods change state and what event each
produces, so it grows a method per store method. It also cannot see transitions the store
makes internally, like `ReapExpired` moving jobs back to pending in bulk.

**Option C — publish inside `internal/store`.** Where the `job_events` row is already
written, in the same transaction's success path.
*For:* one place, structurally impossible to forget, and it matches the durable record
exactly — the stream and the table cannot disagree.
*Against:* `store` gains a dependency on `hub`, and the layering rule says storage should not
know about fan-out. Mitigable by having `store` accept a small `Publisher` interface
(`Publish(job.Event)`) that it declares itself, with `nil` meaning "don't". That is a
consumer-declared interface, the same pattern as `httpapi.Store`.

**Take C with the interface.** The layering objection dissolves once `store` depends on a
one-method interface it declares rather than on `internal/hub`, and it is the only option
where "every state change fans out" — the spec's actual requirement — is true by
construction rather than by discipline. A is tempting today because there is one call site;
it is a trap precisely because there will be five.

Whichever you pick, write the reasoning down. This is the kind of decision that looks
arbitrary in six months.

### Requirements

- Publish **after** the transaction commits, never inside it. An event announcing a state change that then rolls back is a lie the client cannot detect, and it is unfixable after the fact.
- Publishing must not be able to fail the operation it reports. `Publish` returns nothing and never blocks, so this is already true — do not "improve" it into something that can return an error.
- `main` creates the hub and passes it to both the store and the API. One hub, one process.

### Tests

```go
func TestEnqueuePublishesAnEvent(t *testing.T) {
	h := hub.New()
	ch, cancel := h.Subscribe("")
	defer cancel()

	s := freshDBWithHub(t, h)
	j := &job.Job{ID: "j1", Type: "resize", State: job.Pending}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case ev := <-ch:
		if ev.JobID != "j1" || ev.State != job.Pending {
			t.Errorf("published %+v, want j1/pending", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Enqueue committed without publishing an event")
	}
}

// A failed insert must not announce a job that does not exist.
func TestAFailedEnqueuePublishesNothing(t *testing.T) {
	h := hub.New()
	ch, cancel := h.Subscribe("")
	defer cancel()

	s := freshDBWithHub(t, h)
	j := &job.Job{ID: "dup", Type: "resize", State: job.Pending}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	<-ch // the legitimate first event

	if err := s.Enqueue(t.Context(), j); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("second Enqueue returned %v, want ErrDuplicate", err)
	}

	select {
	case ev := <-ch:
		t.Errorf("a rejected Enqueue published %+v", ev)
	case <-time.After(200 * time.Millisecond):
		// nothing arrived, which is the assertion
	}
}
```

The second test is the one case where a short timeout is correct: it is proving a *negative*,
so the wait is the test, and 200ms of certainty is worth the runtime.

---

## Task 5 — End to end

**Goal:** a real browser-equivalent client watching a real job over a real socket.

### Steps

1. Add the end-to-end test to `cmd/conveyord`.
2. `curl -N` it by hand.
3. Commit.

### Test

```go
// The full path: an HTTP client holding a stream open, a second HTTP client
// enqueuing, and the event arriving over the socket. Nothing here uses the
// hub directly — if this passes, the wiring in main is right.
func TestEnqueueAppearsOnTheEventStream(t *testing.T) {
	h := hub.New()
	s := freshDBWithHub(t, h)
	api := httpapi.New(s, h, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	ts := httptest.NewServer(api)
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", ts.URL+"/events", nil)
	if err != nil {
		t.Fatalf("building the stream request: %v", err)
	}
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("opening the stream: %v", err)
	}
	defer stream.Body.Close()
	if ct := stream.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("stream Content-Type is %q", ct)
	}

	// Read on a goroutine: Scan blocks until bytes arrive, which is exactly
	// the synchronization this test wants.
	lines := make(chan string, 32)
	go func() {
		sc := bufio.NewScanner(stream.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()

	body := strings.NewReader(`{"type":"resize","payload":{"w":100}}`)
	post, err := http.NewRequestWithContext(ctx, "POST", ts.URL+"/jobs", body)
	if err != nil {
		t.Fatalf("building the POST: %v", err)
	}
	post.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(post)
	if err != nil {
		t.Fatalf("POST /jobs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /jobs returned %d", resp.StatusCode)
	}

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("the stream closed before an event arrived")
			}
			if strings.HasPrefix(line, "data: ") && strings.Contains(line, "pending") {
				return // the whole phase, proven
			}
		case <-ctx.Done():
			t.Fatal("no event reached the client within the deadline")
		}
	}
}
```

Note the shape: **the stream is opened before the job is enqueued.** Reversed, the test
races the subscription against the publish and flakes. That ordering constraint is real for
clients too, and worth a sentence in the API docs.

### By hand

```bash
go run ./cmd/conveyord &
curl -N localhost:8080/events &            # -N disables curl's buffering
curl -sS -X POST localhost:8080/jobs -d '{"type":"resize","payload":{"w":1}}'
```

The event should appear in the first terminal **immediately**, not when you kill the server.
If it appears only at shutdown, flushing is broken — check the `Flusher` assertion and check
that the route is not wrapped in `withTimeout`.

Leave the stream open for a minute and confirm heartbeat comments arrive.

---

## Phase review checklist

```bash
gofmt -l .
go vet ./...
staticcheck -checks=all ./...
go test -race -count=2 ./...
go test -race -count=10 ./internal/hub/
go list -deps ./internal/job | grep -v '^github.com/mddemin/conveyor' | grep '\.'
go list -deps ./internal/hub  | grep -v '^github.com/mddemin/conveyor' | grep '\.'
```

Both `go list` lines must print **nothing**. `internal/job` keeps its zero-dependency
property after `Event` moves in, and `internal/hub` is stdlib-only.

By eye:

- Every `go` statement in the repo has a comment saying how that goroutine exits.
- `Publish` holds the read lock across the send, and the comment explains why that is required rather than sloppy.
- Every send to a subscriber is inside a `select` with a `default`.
- Nothing outside the hub closes a subscriber channel; the type system says so (`<-chan`).
- Both SSE routes are registered with `HandleFunc`, not through `timed(...)`, with a comment.
- The stream handler's `defer cancel()` is on the line after `Subscribe`.
- No `time.Sleep` anywhere in the test suite. `grep -rn "time.Sleep" --include="*_test.go" .` returns nothing.
- Every event write is followed by a `Flush`.
- Dropped events are counted, and the count is visible somewhere an operator would look.

Then the thing no test proves:

```bash
curl -N localhost:8080/events
```

**Done when** an event appears in that terminal the instant a job is enqueued elsewhere, a
second `curl -N` that you suspend with Ctrl-Z stops affecting the first one, and killing
both leaves `Len()` at zero.

---

## What phase 7 inherits

Written down now so the seams are deliberate:

- The hub owns no goroutines, so it needs no shutdown. Phase 7's `errgroup` tree has one less member.
- Every open stream holds a goroutine parked in `select`. Graceful shutdown has to close them, which is when `goleak` starts earning its place in the test suite.
- `store` publishing every state change means the dispatch loop's transitions appear on the stream with no extra work — that is the payoff for taking option C in Task 4.
- The snapshot-versus-gap question deferred in Task 3 gets sharper once jobs actually move through states quickly. Revisit it then, with the note you wrote.
