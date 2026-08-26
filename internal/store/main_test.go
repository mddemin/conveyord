package store

import (
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"

	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/pgtest"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	code := pgtest.Run(m)
	if err := goleak.Find(
		goleak.IgnoreTopFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

// capturePublisher records what a Store published, so a test can assert on
// the events a write produced without standing up a real hub. Locked because
// Lease is exercised from several goroutines in the concurrency test.
type capturePublisher struct {
	mu     sync.Mutex
	events []job.Event
}

func (c *capturePublisher) Publish(ev job.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, ev)
}

func (c *capturePublisher) published() []job.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.events)
}

// published returns everything s has published so far. Every Store built by
// freshDB has a capturePublisher, so the assertion reads the same in any test.
func published(t *testing.T, s *Store) []job.Event {
	t.Helper()
	c, ok := s.events.(*capturePublisher)
	if !ok {
		t.Fatalf("store was built with a %T, not a capturePublisher", s.events)
	}
	return c.published()
}

// freshDB truncates every table and returns a Store ready for one test.
// Truncating is faster than recreating the schema and keeps tests independent
// of the order they run in.
func freshDB(t *testing.T) *Store {
	t.Helper()
	pool := pgtest.Pool(t)

	ctx := t.Context()
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_events RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return New(pool, &capturePublisher{})
}
