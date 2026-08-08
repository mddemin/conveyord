// Package hub fans job events out to live subscribers.
//
// A [Hub] is a broadcaster and nothing more: publishers call [Hub.Publish] on
// their own goroutine, subscribers read from the channel [Hub.Subscribe] hands
// back on theirs, and the hub itself owns no goroutines and has no lifecycle.
// There is nothing to start and nothing to shut down, which is one fewer thing
// to leak. It holds no history — an event published before a subscription
// exists is gone, and a subscriber that connects late sees only what happens
// next.
//
// Publish never blocks. A subscriber whose buffer is full has its event
// dropped and is marked lagged, rather than being waited for; one client on a
// slow connection cannot stall a publisher or starve the other subscribers.
// That is only acceptable because these events are not the record of what
// happened — Postgres is. A dropped event costs a client one GET, which is
// what [Subscription.Lagged] exists to tell it to do.
//
// The type system carries part of the safety argument. Subscribe hands back a
// receive-only channel, so a subscriber cannot close or write to it, and the
// only close is the one [Subscription.Close] performs under the write lock —
// which is what makes a non-blocking send under the read lock safe.
//
// Everything here is safe for concurrent use. The hub is process-local: a
// second conveyord has its own, and a client streaming from one will not see
// events published to the other. That is fine while there is one process and
// is the thing to fix first when there is not.
package hub

import (
	"sync"
	"sync/atomic"

	"github.com/mddemin/conveyor/internal/job"
)

// bufferSize is how far behind a subscriber may fall before Publish starts
// discarding its events. Too small and ordinary scheduling jitter costs a
// client events it could easily have kept up with; too large and the client
// discovers it is behind long after the fact, at a steady-state cost of
// bufferSize × subscribers × sizeof(job.Event) bytes.
const bufferSize = 16

type Hub struct {
	mu     sync.RWMutex
	nextID int
	// Keyed by subscription ID, not job ID: two clients may watch the same job,
	// and /events subscribers have no job at all. Publish scans every subscriber;
	// fine at this scale, index by job if the subscriber count reaches thousands.
	subs map[int]*Subscription
}

func New() *Hub {
	return &Hub{subs: make(map[int]*Subscription)}
}

type Subscription struct {
	C <-chan job.Event

	hub    *Hub           // set by Subscribe; how Close finds the map
	id     int            // this subscription's key in that map
	ch     chan job.Event // the send side of C, for Publish
	jobID  string
	closed bool

	lagged atomic.Bool
}

func (s *Subscription) Lagged() bool {
	return s.lagged.Swap(false)
}

// Close ends the subscription and closes C. Safe to call more than once.
func (s *Subscription) Close() {
	s.hub.mu.Lock()
	defer s.hub.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	delete(s.hub.subs, s.id)
	close(s.ch)
}

func (h *Hub) Subscribe(jobID string) *Subscription {
	ch := make(chan job.Event, bufferSize)
	s := &Subscription{C: ch, ch: ch, hub: h, jobID: jobID}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	s.id = h.nextID
	h.subs[s.id] = s
	return s
}

// Publish delivers ev to every matching subscriber. It never blocks: a
// subscriber that is not keeping up loses events rather than slowing anyone
// down. Postgres holds the truth, so a dropped event costs a client one
// GET /jobs/{id}, not correctness.
func (h *Hub) Publish(ev job.Event) {
	// The read lock is held across the sends, not just the map read. Releasing
	// it first would admit a cancel between the lookup and the send, which is
	// a send on a closed channel — a panic in whichever goroutine happened to
	// be publishing. A read lock suffices because concurrent sends on one
	// channel are safe and nothing here mutates the map; what it excludes is
	// close, which cancel takes the write lock to do.
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, s := range h.subs {
		if s.jobID == "" || s.jobID == ev.JobID {
			// The default is what makes holding the lock affordable: a blocking
			// send here would stall every other publisher behind one slow
			// client. The two decisions only work as a pair — remove either and
			// the design breaks somewhere else.
			select {
			case s.ch <- ev:
			default:
				s.lagged.Store(true)
			}
		}
	}
}

// Len reports the number of live subscriptions. For tests and /healthz.
func (h *Hub) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
