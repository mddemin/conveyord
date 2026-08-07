package hub

import (
	"sync"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

func TestSubscribeReceivesAPublishedEvent(t *testing.T) {
	h := New()
	s := h.Subscribe("")
	defer s.Close()

	want := job.Event{JobID: "j1", State: job.Pending}
	h.Publish(want)

	select {
	case got := <-s.C:
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
	s := h.Subscribe("j1")
	defer s.Close()

	h.Publish(job.Event{JobID: "j2", State: job.Pending})
	h.Publish(job.Event{JobID: "j1", State: job.Leased})

	select {
	case got := <-s.C:
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

	subs := make([]*Subscription, n)
	for i := range subs {
		subs[i] = h.Subscribe("")
		defer subs[i].Close()
	}

	h.Publish(job.Event{JobID: "j1", State: job.Pending})

	for i, s := range subs {
		select {
		case <-s.C:
		case <-time.After(2 * time.Second):
			t.Errorf("subscriber %d never received the broadcast", i)
		}
	}
}

// Close must remove the subscription, not merely stop reading it. A hub that
// keeps delivering to a closed subscriber leaks a buffer per dead client.
func TestCloseStopsDeliveryAndFreesTheSlot(t *testing.T) {
	h := New()
	s := h.Subscribe("")

	if h.Len() != 1 {
		t.Fatalf("Len is %d after one Subscribe, want 1", h.Len())
	}

	s.Close()

	if h.Len() != 0 {
		t.Errorf("Len is %d after Close, want 0", h.Len())
	}
	h.Publish(job.Event{JobID: "j1", State: job.Pending})

	// A closed subscriber's channel is closed, so this returns immediately
	// with the zero value rather than blocking. Receiving an actual event here
	// would mean the hub still holds a reference to it.
	if got, ok := <-s.C; ok {
		t.Errorf("received %+v after Close, want a closed channel", got)
	}
}

// The handler will defer Close() and phase 7 will add another caller. Two
// closes of one channel is a panic, so this must be safe.
func TestCloseIsIdempotent(t *testing.T) {
	h := New()
	s := h.Subscribe("")

	s.Close()
	s.Close()
	s.Close()

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

// The whole point of the phase. A subscriber that never reads must not slow
// down, block, or otherwise affect Publish.
func TestASubscriberThatNeverReadsCannotBlockPublish(t *testing.T) {
	h := New()
	s := h.Subscribe("") // deliberately never received from
	defer s.Close()

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

	slow := h.Subscribe("") // never read
	defer slow.Close()
	fast := h.Subscribe("")
	defer fast.Close()

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
		case <-fast.C:
		case <-time.After(5 * time.Second):
			t.Fatalf("fast subscriber received only %d events before stalling", i)
		}
	}
}

// The flag is how the SSE handler learns to tell its client to resync, and it
// is the only record that a drop happened — there is no counter. It must
// survive until read — a drop that clears itself before anyone looks is the
// same as no drop at all — and clear on read, so one lag episode produces one
// "lagged" frame rather than one per discarded event.
func TestLaggedIsSetByADropAndClearsOnRead(t *testing.T) {
	h := New()
	s := h.Subscribe("")
	defer s.Close()

	if s.Lagged() {
		t.Fatal("a fresh subscription reports itself lagged")
	}

	for i := 0; i < bufferSize*4; i++ {
		h.Publish(job.Event{JobID: "j1", State: job.Pending})
	}

	if !s.Lagged() {
		t.Error("events were dropped but the subscription is not marked lagged")
	}
	if s.Lagged() {
		t.Error("Lagged stayed set after being read; the handler would emit a resync frame forever")
	}
}

// Publishing to a subscriber that is keeping up must not mark it lagged. A
// flag that is always set is a flag the client learns to ignore.
func TestAKeptUpSubscriberIsNeverLagged(t *testing.T) {
	h := New()
	s := h.Subscribe("")
	defer s.Close()

	for i := 0; i < bufferSize*4; i++ {
		h.Publish(job.Event{JobID: "j1", State: job.Pending})
		<-s.C
	}

	if s.Lagged() {
		t.Error("a subscriber that received every event is marked lagged")
	}
}

// The detector needs contention to have something to observe. Subscribe,
// publish, close, and reading the lag flag all racing is the shape the real
// server produces.
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
				s := h.Subscribe("")
				select {
				case <-s.C:
				default:
				}
				s.Lagged()
				s.Close()
			}
		}()
	}

	wg.Wait()
	if h.Len() != 0 {
		t.Errorf("Len is %d after every subscriber closed, want 0", h.Len())
	}
}
