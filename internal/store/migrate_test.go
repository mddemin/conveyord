package store

import (
	"testing"

	"github.com/mddemin/conveyor/internal/pgtest"
)

func TestMigrateCreatesSchema(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := t.Context()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	for _, table := range []string{"jobs", "job_events", "schema_migrations"} {
		var exists bool
		err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`,
			table).Scan(&exists)
		if err != nil {
			t.Fatalf("checking for %s: %v", table, err)
		}
		if !exists {
			t.Errorf("table %s was not created", table)
		}
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	pool := pgtest.Pool(t)
	ctx := t.Context()

	countApplied := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil {
			t.Fatalf("counting migrations: %v", err)
		}
		return n
	}

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	want := countApplied()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	if got := countApplied(); got != want {
		t.Errorf("schema_migrations has %d rows after a second run, want %d", got, want)
	}
}
