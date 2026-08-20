package store

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

func ids(jobs []job.Job) []string {
	var jobIds []string
	for _, j := range jobs {
		jobIds = append(jobIds, j.ID)
	}
	return jobIds
}

func intersect(f1 []string, f2 []string) []string {
	var inter []string
	for _, v1 := range f1 {
		if slices.Contains(f2, v1) {
			inter = append(inter, v1)
		}
	}
	return inter
}

func enqueueN(t *testing.T, s *Store, n int) {
	t.Helper()
	for i := range n {
		j := &job.Job{ID: fmt.Sprintf("j%03d", i), Type: "resize"}
		if err := s.Enqueue(t.Context(), j); err != nil {
			t.Fatalf("Enqueue %s: %v", j.ID, err)
		}
	}
}

func leaseN(t *testing.T, s *Store, n int, leasedBy string) []job.Job {
	t.Helper()
	enqueueN(t, s, n)

	jobs, err := s.Lease(t.Context(), leasedBy, n, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	return jobs
}

func TestLeaseFollowsTheStateMachine(t *testing.T) {
	if err := job.Transition(job.Pending, job.Leased); err != nil {
		t.Fatalf("leaseJobSQL hardcodes pending -> leased, which the domain rejects: %v", err)
	}
}

func TestLeaseClaimsPendingJobs(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 5)

	got, err := s.Lease(t.Context(), "agent-1", 3, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("Lease returned %d jobs, want 3", len(got))
	}
	for _, j := range got {
		if j.State != job.Leased {
			t.Errorf("job %s has state %s, want %s", j.ID, j.State, job.Leased)
		}
	}
}

func TestLeaseTakesOldestFirst(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 5)

	got, err := s.Lease(t.Context(), "agent-1", 2, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	// RETURNING makes no promise about row order — the inner ORDER BY decides
	// which rows are picked, not the order they come back in. Compare as a set.
	gotIDs := ids(got)
	slices.Sort(gotIDs)
	if !slices.Equal(gotIDs, []string{"j000", "j001"}) {
		t.Errorf("Lease returned %v, want the two oldest jobs", gotIDs)
	}
}

func TestLeaseSkipsAlreadyLeasedJobs(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 3)
	ctx := t.Context()

	first, err := s.Lease(ctx, "agent-1", 2, time.Minute)
	if err != nil {
		t.Fatalf("first Lease: %v", err)
	}
	second, err := s.Lease(ctx, "agent-2", 2, time.Minute)
	if err != nil {
		t.Fatalf("second Lease: %v", err)
	}

	if len(first) != 2 || len(second) != 1 {
		t.Fatalf("leases returned %d and %d jobs, want 2 and 1", len(first), len(second))
	}
	if overlap := intersect(ids(first), ids(second)); len(overlap) != 0 {
		t.Errorf("both leases returned %v", overlap)
	}
}

func TestLeaseOnEmptyQueueReturnsNothing(t *testing.T) {
	s := freshDB(t)

	got, err := s.Lease(t.Context(), "agent-1", 5, time.Minute)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Lease on an empty queue returned %d jobs", len(got))
	}
}

// TestLeaseRecordsOwnerAndExpiry checks the two columns that never reach
// job.Job, so no other test can see them. Renew and Complete key off
// leased_by, and the expiry is what proves pgx encodes a time.Duration as an
// interval rather than the query merely not erroring.
func TestLeaseRecordsOwnerAndExpiry(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 1)
	ctx := t.Context()

	const lease = 30 * time.Minute
	got, err := s.Lease(ctx, "agent-1", 1, lease)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Lease returned %d jobs, want 1", len(got))
	}

	// now() comes from the database so the container's clock is the only one
	// involved; the test host's clock never enters the comparison.
	var (
		leasedBy string
		expires  time.Time
		now      time.Time
	)
	err = s.pool.QueryRow(ctx,
		`SELECT leased_by, lease_expires_at, now() FROM jobs WHERE id = $1`,
		got[0].ID).Scan(&leasedBy, &expires, &now)
	if err != nil {
		t.Fatalf("reading lease columns: %v", err)
	}

	if leasedBy != "agent-1" {
		t.Errorf("leased_by is %q, want %q", leasedBy, "agent-1")
	}
	if d := expires.Sub(now); d < lease-time.Minute || d > lease {
		t.Errorf("lease expires in %v, want just under %v", d, lease)
	}
}

// TestLeaseIsExclusiveUnderConcurrency is the test this phase exists for.
// Ten leasers race for fifty jobs; every job must be handed out exactly once.
func TestLeaseIsExclusiveUnderConcurrency(t *testing.T) {
	const (
		jobs    = 50
		leasers = 10
		each    = 5
	)

	s := freshDB(t)
	enqueueN(t, s, jobs)
	ctx := t.Context()

	var (
		mu      sync.Mutex
		claimed []string
		errs    []error
		wg      sync.WaitGroup
	)

	wg.Add(leasers)
	for i := range leasers {
		go func() {
			defer wg.Done()
			got, err := s.Lease(ctx, fmt.Sprintf("agent-%d", i), each, time.Minute)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			claimed = append(claimed, ids(got)...)
		}()
	}
	wg.Wait()

	for _, err := range errs {
		t.Errorf("Lease: %v", err)
	}

	seen := make(map[string]int, len(claimed))
	for _, id := range claimed {
		seen[id]++
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("job %s was leased %d times", id, n)
		}
	}
	if len(claimed) != jobs {
		t.Errorf("leasers claimed %d jobs in total, want %d", len(claimed), jobs)
	}
}

func TestLeaseRecordsAnEvent(t *testing.T) {
	s := freshDB(t)
	enqueueN(t, s, 2)
	ctx := t.Context()

	if _, err := s.Lease(ctx, "agent-7", 1, time.Minute); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	events, err := s.Events(ctx, "j000")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("job has %d events, want 2 (enqueue and lease)", len(events))
	}
	last := events[1]
	if last.State != job.Leased {
		t.Errorf("last event state is %s, want %s", last.State, job.Leased)
	}
	if !strings.Contains(last.Message, "agent-7") {
		t.Errorf("last event message is %q, want it to name the leaseholder", last.Message)
	}

	// The job nobody leased must not have picked up an event.
	untouched, err := s.Events(ctx, "j001")
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(untouched) != 1 {
		t.Errorf("unleased job has %d events, want 1", len(untouched))
	}
}

// Lease(n) where n came from arithmetic must mean "this many and no more".
// Phase 7's dispatcher passes its free capacity: if a zero there becomes
// DefaultLimit, a dispatcher with no free slots leases a hundred jobs that
// nothing will run and holds them until every one of those leases expires.
func TestLeaseWithNoCapacityTakesNothing(t *testing.T) {
	for _, n := range []int{0, -1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := freshDB(t)
			enqueueN(t, s, 3)
			ctx := t.Context()

			got, err := s.Lease(ctx, "agent-1", n, time.Minute)
			if err != nil {
				t.Fatalf("Lease: %v", err)
			}
			if len(got) != 0 {
				t.Errorf("Lease(n=%d) handed out %v, want nothing", n, ids(got))
			}

			j, err := s.Get(ctx, "j000")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if j.State != job.Pending {
				t.Errorf("job is %s after a zero-capacity Lease, want %s", j.State, job.Pending)
			}
		})
	}
}
