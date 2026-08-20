package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/mddemin/conveyor/internal/job"
)

func TestCompleteRecordsAnEvent(t *testing.T) {
	s := freshDB(t)
	const leasedBy = "agent-1"
	const message = "complete"
	leaseN(t, s, 2, leasedBy)
	ctx := t.Context()

	if err := s.Complete(ctx, "j000", leasedBy, job.Succeeded, message); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	events, err := s.Events(ctx, "j000")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("job has %d events, want 3 (enqueue, lease, complete)", len(events))
	}
	last := events[2]
	if last.State != job.Succeeded {
		t.Errorf("last event state is %s, want %s", last.State, job.Succeeded)
	}
	if !strings.Contains(last.Message, message) {
		t.Errorf("last event message is %q, want it to contain %q", last.Message, message)
	}

	// The job nobody completed must not have picked up an event.
	untouched, err := s.Events(ctx, "j001")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(untouched) != 2 {
		t.Errorf("uncompleted job has %d events, want 2", len(untouched))
	}
}

func TestCompleteClearsTheLease(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if err := s.Complete(ctx, "j000", "agent-1", job.Succeeded, "done"); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Succeeded {
		t.Errorf("state is %s after Complete, want %s", got.State, job.Succeeded)
	}

	// A finished job holds no lease; leaving the columns set would make it
	// look like work in flight to the reaper in task 4.
	owner, expires := expiryOf(t, s, "j000")
	if owner != nil || expires != nil {
		t.Errorf("completed job still has leased_by=%v lease_expires_at=%v, want both NULL", owner, expires)
	}
}

func TestCompleteRejectsAnotherAgent(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	err := s.Complete(ctx, "j000", "agent-2", job.Succeeded, "done")
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Complete by the wrong agent returned %v, want job.ErrNotFound", err)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Leased {
		t.Errorf("state is %s after a rejected Complete, want %s", got.State, job.Leased)
	}
	// A rejected Complete must leave no trace in the log either.
	events, err := s.Events(ctx, "j000")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("job has %d events after a rejected Complete, want 2", len(events))
	}
}

func TestCompleteRejectsAnInvalidTransition(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	// leased -> leased is not an edge in job.transitions, and never will be.
	err := s.Complete(ctx, "j000", "agent-1", job.Leased, "sideways")
	if !errors.Is(err, job.ErrInvalidTransition) {
		t.Fatalf("Complete to %s returned %v, want job.ErrInvalidTransition", job.Leased, err)
	}

	// The check runs before any SQL, so nothing should have been written.
	events, err := s.Events(ctx, "j000")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("job has %d events after an invalid Complete, want 2", len(events))
	}
}

func TestCompleteOnAnUnleasedJob(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 1)

	err := s.Complete(t.Context(), "j000", "agent-1", job.Succeeded, "done")
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Complete on a pending job returned %v, want job.ErrNotFound", err)
	}
}

// Complete reserves burial for Fail, so nothing reaches dead without the retry
// policy having had a say and without a cause on the event. job.Dead is
// terminal and reachable from leased, so neither the Transition check nor the
// Terminal check stops it — only naming the one state Complete accepts does.
func TestCompleteRefusesToBuryAJob(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if err := s.Complete(ctx, "j000", "agent-1", job.Dead, "buried"); err == nil {
		t.Error("Complete buried a job, bypassing the retry policy")
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Leased {
		t.Errorf("state is %s after a rejected Complete, want %s", got.State, job.Leased)
	}
}
