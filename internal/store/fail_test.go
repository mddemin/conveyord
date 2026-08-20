package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

// retryPolicy backs off by a fixed hour with jitter pinned to the top of the
// window, so a scheduled retry is unambiguously in the future.
func retryPolicy(maxAttempts int) job.Policy {
	return job.Policy{
		Base:        time.Hour,
		Max:         time.Hour,
		MaxAttempts: maxAttempts,
		Jitter:      func() float64 { return 1 },
	}
}

// nextAttemptOf reads the retry schedule, which never reaches job.Job.
// A job with no pending retry has the column NULL.
func nextAttemptOf(t *testing.T, s *Store, id string) *time.Time {
	t.Helper()
	var at *time.Time
	err := s.pool.QueryRow(t.Context(),
		`SELECT next_attempt_at FROM jobs WHERE id = $1`, id).Scan(&at)
	if err != nil {
		t.Fatalf("reading next_attempt_at for %s: %v", id, err)
	}
	return at
}

func TestFailFollowsTheStateMachine(t *testing.T) {
	// The two states OnFailure can return must both be reachable from leased,
	// or Fail writes a transition the domain forbids.
	for _, to := range []job.State{job.Pending, job.Dead} {
		if err := job.Transition(job.Leased, to); err != nil {
			t.Errorf("Fail can write leased -> %s, which the domain rejects: %v", to, err)
		}
	}
}

// The decision carries a duration, not an instant, so the schedule is written
// as now() + interval by the database — and next_attempt_at must therefore be
// measured against the database's clock, which is the same clock every Lease
// compares it to. Asserting against time.Now() here would pass on one machine
// and hide a skew that delays or skips every retry in production.
func TestFailSchedulesARetry(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	d, err := s.Fail(ctx, "j000", "agent-1", errors.New("boom"), retryPolicy(3))
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if d.State != job.Pending {
		t.Errorf("decision state is %s, want %s", d.State, job.Pending)
	}
	// retryPolicy pins jitter to the top of a one-hour window.
	if d.Backoff != time.Hour {
		t.Errorf("decision backs off by %v, want %v", d.Backoff, time.Hour)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Pending {
		t.Errorf("state is %s, want %s", got.State, job.Pending)
	}
	if got.Attempt != 1 {
		t.Errorf("attempt is %d after one failure, want 1", got.Attempt)
	}

	var dbNow time.Time
	if err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
		t.Fatalf("reading the database clock: %v", err)
	}
	at := nextAttemptOf(t, s, "j000")
	if at == nil {
		t.Fatal("next_attempt_at is NULL on a job scheduled for retry")
	}
	// A second of slack: the assertion is about the hour, not the milliseconds.
	if delay := at.Sub(dbNow); delay < d.Backoff-time.Second || delay > d.Backoff+time.Second {
		t.Errorf("next_attempt_at is %v after the database's now(), want the decided %v", delay, d.Backoff)
	}
}

func TestFailBuriesAfterMaxAttempts(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	d, err := s.Fail(ctx, "j000", "agent-1", errors.New("boom"), retryPolicy(1))
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if d.State != job.Dead {
		t.Fatalf("decision state is %s on the last allowed attempt, want %s", d.State, job.Dead)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Dead {
		t.Errorf("state is %s, want %s", got.State, job.Dead)
	}
	if at := nextAttemptOf(t, s, "j000"); at != nil {
		t.Errorf("buried job has next_attempt_at = %v, want NULL", at)
	}
}

func TestFailBuriesPermanentErrors(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	// A permanent cause skips the retry budget entirely.
	d, err := s.Fail(ctx, "j000", "agent-1", job.Permanent(errors.New("bad payload")), retryPolicy(10))
	if err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if d.State != job.Dead {
		t.Errorf("decision state is %s for a permanent error, want %s", d.State, job.Dead)
	}
}

func TestFailClearsTheLease(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if _, err := s.Fail(ctx, "j000", "agent-1", errors.New("boom"), retryPolicy(3)); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	// A job waiting to be retried is not held by anyone.
	owner, expires := expiryOf(t, s, "j000")
	if owner != nil || expires != nil {
		t.Errorf("failed job still has leased_by=%v lease_expires_at=%v, want both NULL", owner, expires)
	}
}

func TestFailRecordsAnEvent(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if _, err := s.Fail(ctx, "j000", "agent-1", errors.New("disk on fire"), retryPolicy(3)); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	events, err := s.Events(ctx, "j000")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("job has %d events, want 3 (enqueue, lease, fail)", len(events))
	}
	last := events[2]
	if last.State != job.Pending {
		t.Errorf("last event state is %s, want the state the job moved to (%s)", last.State, job.Pending)
	}
	if !strings.Contains(last.Message, "disk on fire") {
		t.Errorf("last event message is %q, want it to carry the cause", last.Message)
	}
}

// A failure with no cause is a caller bug: there is nothing to record and
// nothing for the policy to classify. Fail must say so before it touches the
// database.
func TestFailRejectsANilCause(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	_, err := s.Fail(ctx, "j000", "agent-1", nil, retryPolicy(3))
	if !errors.Is(err, job.ErrEmptyCause) {
		t.Fatalf("Fail with a nil cause returned %v, want ErrEmptyCause", err)
	}

	// Rejected before the transaction opens, so the lease is intact.
	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Leased || got.Attempt != 0 {
		t.Errorf("job is %s attempt=%d after a rejected Fail, want leased attempt=0", got.State, got.Attempt)
	}
}

func TestFailRejectsAnotherAgent(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	_, err := s.Fail(ctx, "j000", "agent-2", errors.New("boom"), retryPolicy(3))
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Fail by the wrong agent returned %v, want job.ErrNotFound", err)
	}

	got, err := s.Get(ctx, "j000")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != job.Leased || got.Attempt != 0 {
		t.Errorf("job is %s attempt=%d after a rejected Fail, want leased attempt=0", got.State, got.Attempt)
	}
}

func TestFailOnAnUnleasedJob(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 1)

	_, err := s.Fail(t.Context(), "j000", "agent-1", errors.New("boom"), retryPolicy(3))
	if !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Fail on a pending job returned %v, want job.ErrNotFound", err)
	}
}

// The whole point of next_attempt_at: a job in backoff must be invisible to
// Lease until its delay has elapsed.
func TestFailHidesTheJobUntilItsNextAttempt(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	if _, err := s.Fail(ctx, "j000", "agent-1", errors.New("boom"), retryPolicy(3)); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	got, err := s.Lease(ctx, "agent-2", 10, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Lease handed out %v while it was still backing off", ids(got))
	}
}

func TestFailWithAnElapsedBackoffIsLeasableAgain(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	// A negative window puts the retry in the past, the same trick the reaper
	// tests use to avoid sleeping.
	p := retryPolicy(3)
	p.Base, p.Max = -time.Hour, -time.Hour
	if _, err := s.Fail(ctx, "j000", "agent-1", errors.New("boom"), p); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	got, err := s.Lease(ctx, "agent-2", 10, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Lease returned %d jobs after the backoff elapsed, want 1", len(got))
	}
	if got[0].Attempt != 1 {
		t.Errorf("re-leased job has attempt=%d, want the failure to have been counted (1)", got[0].Attempt)
	}
}

func TestFailAccumulatesAttempts(t *testing.T) {
	s := freshDB(t)
	leaseN(t, s, 1, "agent-1")
	ctx := t.Context()

	p := retryPolicy(5)
	p.Base, p.Max = -time.Hour, -time.Hour

	for want := 1; want <= 3; want++ {
		if _, err := s.Fail(ctx, "j000", "agent-1", errors.New("boom"), p); err != nil {
			t.Fatalf("Fail %d: %v", want, err)
		}
		got, err := s.Get(ctx, "j000")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Attempt != want {
			t.Fatalf("attempt is %d after %d failures, want %d", got.Attempt, want, want)
		}
		if _, err := s.Lease(ctx, "agent-1", 1, time.Minute); err != nil {
			t.Fatalf("re-Lease: %v", err)
		}
	}
}
