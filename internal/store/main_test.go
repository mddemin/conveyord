package store

import (
	"os"
	"testing"

	"github.com/mddemin/conveyor/internal/pgtest"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

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
	return New(pool)
}
