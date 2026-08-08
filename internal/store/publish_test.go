package store

import (
	"errors"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

// only returns the single event a write was expected to publish, failing if
// the count is anything but one. Most writes are one state change.
func only(t *testing.T, s *Store) job.Event {
	t.Helper()
	evs := published(t, s)
	if len(evs) != 1 {
		t.Fatalf("published %d events, want exactly 1: %+v", len(evs), evs)
	}
	return evs[0]
}

func TestEnqueuePublishesPending(t *testing.T) {
	s := freshDB(t)

	j := &job.Job{ID: "j1", Type: "resize"}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ev := only(t, s)
	if ev.JobID != "j1" || ev.State != job.Pending {
		t.Errorf("published %+v, want j1/pending", ev)
	}
	// The timestamp comes from the row, not from time.Now() in Go: a zero one
	// means the event was assembled rather than read back.
	if ev.CreatedAt.IsZero() {
		t.Error("published event has no CreatedAt")
	}
}

// The event must describe something that actually happened. A publish inside
// the transaction would announce a job that no longer exists once the conflict
// rolls the insert back.
func TestAFailedEnqueuePublishesNothing(t *testing.T) {
	s := freshDB(t)

	j := &job.Job{ID: "j1", Type: "resize"}
	if err := s.Enqueue(t.Context(), j); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	if err := s.Enqueue(t.Context(), &job.Job{ID: "j1", Type: "resize"}); err == nil {
		t.Fatal("a duplicate id was accepted")
	}

	if evs := published(t, s); len(evs) != 1 {
		t.Errorf("published %d events, want only the one from the successful insert: %+v", len(evs), evs)
	}
}

func TestLeasePublishesOneEventPerJob(t *testing.T) {
	s := freshDB(t)
	for _, id := range []string{"j1", "j2", "j3"} {
		if err := s.Enqueue(t.Context(), &job.Job{ID: id, Type: "resize"}); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}

	leased, err := s.Lease(t.Context(), "agent-1", 3, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(leased) != 3 {
		t.Fatalf("leased %d jobs, want 3", len(leased))
	}

	// A batch lease is three state changes, not one. Subscribers are watching
	// individual jobs and there is no such thing as a batch event.
	evs := published(t, s)[3:] // drop the three enqueues
	if len(evs) != 3 {
		t.Fatalf("a lease of 3 jobs published %d events, want 3: %+v", len(evs), evs)
	}
	for _, ev := range evs {
		if ev.State != job.Leased {
			t.Errorf("published %+v, want state leased", ev)
		}
		if ev.CreatedAt.IsZero() {
			t.Errorf("published %+v with no CreatedAt", ev)
		}
	}
}

func TestCompletePublishesTheTerminalState(t *testing.T) {
	s := freshDB(t)
	if err := s.Enqueue(t.Context(), &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Lease(t.Context(), "agent-1", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	if err := s.Complete(t.Context(), "j1", "agent-1", job.Succeeded, "done"); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// The last event, and the one a watching client most needs: miss it and a
	// UI shows "running" on a job that finished.
	evs := published(t, s)
	ev := evs[len(evs)-1]
	if ev.JobID != "j1" || ev.State != job.Succeeded || ev.Message != "done" {
		t.Errorf("published %+v, want j1/succeeded/done", ev)
	}
	if ev.CreatedAt.IsZero() {
		t.Error("published event has no CreatedAt")
	}
}

// Complete on a job another agent holds changes nothing, so it must announce
// nothing.
func TestARejectedCompletePublishesNothing(t *testing.T) {
	s := freshDB(t)
	if err := s.Enqueue(t.Context(), &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Lease(t.Context(), "agent-1", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	before := len(published(t, s))

	err := s.Complete(t.Context(), "j1", "agent-2", job.Succeeded, "done")
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Complete by the wrong agent returned %v, want job.ErrNotFound", err)
	}

	if got := published(t, s); len(got) != before {
		t.Errorf("a rejected Complete published %+v", got[before:])
	}
}

func TestFailPublishesTheDecidedState(t *testing.T) {
	s := freshDB(t)
	if err := s.Enqueue(t.Context(), &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Lease(t.Context(), "agent-1", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	d, err := s.Fail(t.Context(), "j1", "agent-1", errors.New("boom"), job.Policy{MaxAttempts: 3})
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}

	evs := published(t, s)
	ev := evs[len(evs)-1]
	// The decision's state, not the job's. Publishing j.State would announce
	// "leased" for a job that just failed.
	if ev.State != d.State {
		t.Errorf("published state %s, want the decision's %s", ev.State, d.State)
	}
	if ev.Message != "boom" {
		t.Errorf("published message %q, want the cause", ev.Message)
	}
}

func TestReapPublishesOneEventPerReapedJob(t *testing.T) {
	s := freshDB(t)
	for _, id := range []string{"j1", "j2"} {
		if err := s.Enqueue(t.Context(), &job.Job{ID: id, Type: "resize"}); err != nil {
			t.Fatalf("Enqueue %s: %v", id, err)
		}
	}
	// A lease that has already expired by the time it is taken.
	if _, err := s.Lease(t.Context(), "agent-1", 2, -time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	before := len(published(t, s))

	n, err := s.ReapExpired(t.Context())
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if n != 2 {
		t.Fatalf("reaped %d jobs, want 2", n)
	}

	evs := published(t, s)[before:]
	if len(evs) != 2 {
		t.Fatalf("reaping 2 jobs published %d events, want 2: %+v", len(evs), evs)
	}
	for _, ev := range evs {
		if ev.State != job.Pending {
			t.Errorf("published %+v, want state pending", ev)
		}
		// The message names the agent whose lease expired, which is the only
		// place that survives the reap — the row's leased_by is NULL by then.
		if ev.Message == "" {
			t.Errorf("published %+v with no message", ev)
		}
	}
}

// Renew changes no state and writes no job_events row, so it must publish
// nothing. A stream of "still working on it" is noise.
func TestRenewPublishesNothing(t *testing.T) {
	s := freshDB(t)
	if err := s.Enqueue(t.Context(), &job.Job{ID: "j1", Type: "resize"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := s.Lease(t.Context(), "agent-1", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	before := len(published(t, s))

	if err := s.Renew(t.Context(), "j1", "agent-1", time.Minute); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	if got := published(t, s); len(got) != before {
		t.Errorf("Renew published %+v", got[before:])
	}
}
