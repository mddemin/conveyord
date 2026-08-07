package hub

import (
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

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
