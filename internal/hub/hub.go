package hub

import (
	"sync"

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
	subs map[int]*sub
}

type sub struct {
	jobID string // "" = everything
	ch    chan job.Event
}

func New() *Hub {
	return &Hub{subs: make(map[int]*sub)}
}

// Subscribe returns a channel of events and a function that stops the
// subscription. jobID filters to one job; "" receives every event.
func (h *Hub) Subscribe(jobID string) (<-chan job.Event, func()) {
	ch := make(chan job.Event, bufferSize)

	h.mu.Lock()
	h.nextID++
	id := h.nextID
	h.subs[id] = &sub{jobID: jobID, ch: ch}
	h.mu.Unlock()

	// Cancel is a closure rather than an Unsubscribe(id) the caller has to
	// remember: "defer cancel()" on the next line is the whole contract.
	// It must tolerate repeated calls — the SSE handler defers it and phase 7
	// will add a second caller — so the map lookup guards the close. Closing
	// a channel twice panics; a missing key just means someone got here first.
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if s, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(s.ch)
		}
	}
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
				// TODO(phase 6 task 2): count this and mark the subscriber
				// lagged. A discarded event nobody records is a gap the client
				// cannot see and the operator cannot find.
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
