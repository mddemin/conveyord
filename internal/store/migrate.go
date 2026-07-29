// Package store persists jobs in Postgres
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"path"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationLockID = 8734129

// Migrate applies every embedded migration that has not been applied yet.
// It is safe to call on every startup and safe to call concurrently.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// all-or-nothing leaves the schema in a known state;
	// partial application leaves it in a state nobody has a name for
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx,
			"CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")
		if err != nil {
			return err
		}
		applied := make(map[string]struct{})
		rows, err := tx.Query(ctx, "SELECT version FROM schema_migrations")
		if err != nil {
			return err
		}
		strRows, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, row := range strRows {
			applied[row] = struct{}{}
		}

		entries, err := fs.ReadDir(migrationsFS, "migrations")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			version := entry.Name()
			if _, ok := applied[version]; ok {
				continue
			}
			migration, err := fs.ReadFile(migrationsFS, path.Join("migrations", version))
			if err != nil {
				return fmt.Errorf("migration %s: %w", version, err)
			}

			_, err = tx.Exec(ctx, string(migration))
			if err != nil {
				return fmt.Errorf("applying %s: %w", version, err)
			}
			_, err = tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version)
			if err != nil {
				return fmt.Errorf("recording %s: %w", version, err)
			}
		}
		return nil
	})
	return err
}
