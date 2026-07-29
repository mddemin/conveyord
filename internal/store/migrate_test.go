package store

import "testing"

func TestMigrateCreatesSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := t.Context()

	if err := Migrate(ctx, testPool); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	for _, table := range []string{"jobs", "job_events", "schema_migrations"} {
		var exists bool
		err := testPool.QueryRow(ctx,
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
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := t.Context()

	if err := Migrate(ctx, testPool); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := Migrate(ctx, testPool); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}

	var applied int
	if err := testPool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("counting migrations: %v", err)
	}
	if applied != 1 {
		t.Errorf("schema_migrations has %d rows after two runs, want 1", applied)
	}
}
