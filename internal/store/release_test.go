package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

// Release is the difference between "an agent failed this job" and "no agent
// ever saw it". Fail spends an attempt and applies the backoff, so a queue
// with nothing connected walks itself to dead — measured live: leased attempt
// 0 to dead attempt 5 inside a second of the agent leaving. Work that was
// never delivered has not been attempted, so the retry budget must survive it
// untouched.
func TestReleaseReturnsTheJobWithoutSpendingAnAttempt(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if err := s.Release(ctx, "j000", "agent-1", "no agents available"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Pending || got.Attempt != 0 {
		t.Errorf("job is %s attempt=%d after Release, want pending attempt=0", got.State, got.Attempt)
	}

	owner, expires := expiryOf(t, s, "j000")
	if owner != nil || expires != nil {
		t.Errorf("released job still has leased_by=%v lease_expires_at=%v, want both NULL", owner, expires)
	}
}

// Nothing failed, so there is nothing to back off from: the job goes straight
// back into the queue. If Release wrote a next_attempt_at the job would be
// invisible to Lease for that long, which is the one behaviour separating this
// from Fail with a zero-attempt policy.
func TestAReleasedJobIsLeasableAgain(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if err := s.Release(ctx, "j000", "agent-1", "no agents available"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	jobs, err := s.Lease(ctx, "agent-2", 1, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != "j000" {
		t.Fatalf("Lease returned %v, want the released job back", ids(jobs))
	}
	if jobs[0].Attempt != 0 {
		t.Errorf("re-leased job is attempt=%d, want 0", jobs[0].Attempt)
	}
}

// Same fencing as every other write: the dispatcher that lost the lease must
// not clear the lease of the one that now holds it.
func TestReleaseRejectsAnotherAgent(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	err := s.Release(ctx, "j000", "agent-2", "no agents available")
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Release by the wrong agent returned %v, want job.ErrNotFound", err)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Leased || got.Attempt != 0 {
		t.Errorf("job is %s attempt=%d after a rejected Release, want leased attempt=0", got.State, got.Attempt)
	}
}

// A job that silently bounces between leased and pending is indistinguishable
// from a stuck queue in the timeline, and the dispatcher logs the release at
// debug — so this row is the only durable record of why nothing took the job.
// "no agents available" (deploy an agent) and "agent is slow" (add agents) ask
// the operator for different things, which is why the caller supplies it.
func TestReleaseRecordsWhyTheJobCameBack(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if err := s.Release(ctx, "j000", "agent-1", "assign j000: not delivered: no agents available"); err != nil {
		t.Fatalf("Release: %v", err)
	}

	events, err := s.Events(ctx, "j000")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("job has %d events, want 3 (enqueue, lease, release)", len(events))
	}
	last := events[2]
	if last.State != job.Pending {
		t.Errorf("last event state is %s, want %s", last.State, job.Pending)
	}
	if !strings.Contains(last.Message, "no agents available") {
		t.Errorf("last event message is %q, want it to carry the reason", last.Message)
	}
}
