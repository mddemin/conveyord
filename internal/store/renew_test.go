package store

import (
	"errors"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

// expiryOf reads the lease bookkeeping that never reaches job.Job.
// A job with no lease has both columns NULL, hence the pointers.
func expiryOf(t *testing.T, s *Store, id string) (leasedBy *string, expires *time.Time) {
	t.Helper()
	err := s.pool.QueryRow(t.Context(),
		`SELECT leased_by, lease_expires_at FROM jobs WHERE id = $1`,
		id).Scan(&leasedBy, &expires)
	if err != nil {
		t.Fatalf("reading lease columns for %s: %v", id, err)
	}
	return leasedBy, expires
}

func TestRenewExtendsTheLease(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 1)
	ctx := t.Context()

	if _, err := s.Lease(ctx, "agent-1", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	_, before := expiryOf(t, s, "j000")

	if err := s.Renew(ctx, "j000", "agent-1", time.Hour); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	owner, after := expiryOf(t, s, "j000")
	if !after.After(*before) {
		t.Errorf("expiry went from %v to %v, want it to move forward", before, after)
	}
	if owner == nil || *owner != "agent-1" {
		t.Errorf("leased_by is %v after renewal, want agent-1", owner)
	}
}

func TestRenewRejectsAnotherAgent(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 1)
	ctx := t.Context()

	if _, err := s.Lease(ctx, "agent-1", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	_, before := expiryOf(t, s, "j000")

	err := s.Renew(ctx, "j000", "agent-2", time.Hour)
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Renew by the wrong agent returned %v, want job.ErrNotFound", err)
	}

	// The rejection must not have moved the real holder's lease.
	if _, after := expiryOf(t, s, "j000"); !after.Equal(*before) {
		t.Errorf("expiry changed from %v to %v on a rejected renewal", before, after)
	}
}

func TestRenewOnAnUnleasedJob(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 1)

	err := s.Renew(t.Context(), "j000", "agent-1", time.Hour)
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Renew on a pending job returned %v, want job.ErrNotFound", err)
	}
}

func TestRenewOnAnUnknownJob(t *testing.T) {
	s := freshDB(t)

	err := s.Renew(t.Context(), "nope", "agent-1", time.Hour)
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Renew on a missing job returned %v, want job.ErrNotFound", err)
	}
}
