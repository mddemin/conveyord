package store

import (
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

// leaseExpired hands n jobs to leasedBy with a lease that was already dead
// when it was granted. A negative duration means the reaper can be tested
// without sleeping and without a clock seam in Store.
func leaseExpired(t *testing.T, s *Store, n int, leasedBy string) {
	t.Helper()
	enqueueN(t, s, n)
	if _, err := s.Lease(t.Context(), leasedBy, n, -time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
}

func TestReapFollowsTheStateMachine(t *testing.T) {
	if err := job.Transition(job.Leased, job.Pending); err != nil {
		t.Fatalf("reapExpiredSQL hardcodes leased -> pending, which the domain rejects: %v", err)
	}
}

func TestReapExpiredReturnsJobsToPending(t *testing.T) {
	s := freshDB(t)
	leaseExpired(t, s, 2, "agent-1")
	ctx := t.Context()

	n, err := s.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if n != 2 {
		t.Fatalf("ReapExpired reaped %d jobs, want 2", n)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Pending {
		t.Errorf("state is %s after reaping, want %s", got.State, job.Pending)
	}

	// A reaped job must look untouched to the next Lease, or the reaper's
	// partial index keeps matching it forever.
	owner, expires := expiryOf(t, s, "j000")
	if owner != nil || expires != nil {
		t.Errorf("reaped job still has leased_by=%v lease_expires_at=%v, want both NULL", owner, expires)
	}
}

func TestReapExpiredLeavesLiveLeasesAlone(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 2, "agent-1")
	ctx := t.Context()

	n, err := s.ReapExpired(ctx)
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("ReapExpired reaped %d live leases, want 0", n)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Leased {
		t.Errorf("state is %s, want the lease to survive as %s", got.State, job.Leased)
	}
}

func TestReapExpiredIgnoresJobsThatWereNeverLeased(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 2)

	n, err := s.ReapExpired(t.Context())
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("ReapExpired reaped %d pending jobs, want 0", n)
	}
}

func TestReapExpiredOnAnEmptyQueue(t *testing.T) {
	s := freshDB(t)

	n, err := s.ReapExpired(t.Context())
	if err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}
	if n != 0 {
		t.Errorf("ReapExpired reaped %d jobs from an empty queue, want 0", n)
	}
}

func TestReapExpiredRecordsAnEvent(t *testing.T) {
	s := freshDB(t)
	leaseExpired(t, s, 1, "agent-1")
	ctx := t.Context()

	if _, err := s.ReapExpired(ctx); err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}

	events, err := s.Events(ctx, "j000")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("job has %d events, want 3 (enqueue, lease, reap)", len(events))
	}
	last := events[2]
	if last.State != job.Pending {
		t.Errorf("last event state is %s, want %s", last.State, job.Pending)
	}
	if !strings.Contains(last.Message, "reap") {
		t.Errorf("last event message is %q, want it to say the lease was reaped", last.Message)
	}
}

// A crashed agent is not a failed attempt. Counting it as one would let a
// node that dies on startup bury every job it ever touched.
func TestReapExpiredDoesNotCountAnAttempt(t *testing.T) {
	s := freshDB(t)
	leaseExpired(t, s, 1, "agent-1")
	ctx := t.Context()

	if _, err := s.ReapExpired(ctx); err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Attempt != 0 {
		t.Errorf("attempt is %d after reaping, want 0", got.Attempt)
	}
}

func TestReapedJobCanBeLeasedAgain(t *testing.T) {
	s := freshDB(t)
	leaseExpired(t, s, 1, "agent-1")
	ctx := t.Context()

	if _, err := s.ReapExpired(ctx); err != nil {
		t.Fatalf("ReapExpired: %v", err)
	}

	got, err := s.Lease(ctx, "agent-2", 1, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Lease after reaping returned %d jobs, want 1", len(got))
	}
	owner, _ := expiryOf(t, s, "j000")
	if owner == nil || *owner != "agent-2" {
		t.Errorf("leased_by is %v, want agent-2", owner)
	}
}
