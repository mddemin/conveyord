// Package pgtest provides a throwaway Postgres for integration tests.
//
// It is a normal package rather than a _test.go file because test files are
// not importable across packages, and more than one package needs a database.
// It imports testing and must only ever be imported by test files.
package pgtest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DSNEnv names an already-running Postgres to use instead of starting a
// container. Point it at a local docker compose and the whole suite skips
// container startup; leave it unset in CI and every package gets a clean,
// isolated database.
//
// Each test process runs in its own schema (pgtest_<pid>) inside the shared
// database: packages run as concurrent processes and their tests TRUNCATE
// tables, so without namespacing one package's freshDB deletes another's
// fixtures mid-test. The schema is dropped when the process finishes.
const DSNEnv = "CONVEYOR_TEST_DSN"

// pool is the connection pool every integration test in the importing package
// uses. It is nil under -short, where Run never starts a container.
var pool *pgxpool.Pool

// dsn is the connection string behind pool, exposed through [DSN] for code
// under test that opens its own connections instead of taking Pool.
var dsn string

// Run boots Postgres, runs the package's tests, and tears it down. Call it
// from TestMain and pass the result to os.Exit:
//
//	func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }
//
// Go runs each package's tests in its own process, so every package that needs
// a database calls this from its own TestMain.
func Run(m *testing.M) int {
	// m.Run parses the test flags, so -short is not readable until we do it
	// ourselves. testing.Short() before this line panics.
	flag.Parse()

	if testing.Short() {
		return m.Run()
	}

	code, err := runWithPostgres(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pgtest:", err)
		return 1
	}
	return code
}

// Pool returns the pool for one test. Under -short there is no database, so
// the test skips rather than dereferencing a nil pool.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	return pool
}

// DSN returns the test database's connection string, for code under test
// that opens its own connections instead of taking Pool.
func DSN(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	return dsn
}

// withSearchPath appends a search_path runtime parameter to a DSN.
func withSearchPath(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "options=-c%20search_path%3D" + schema
}

// runWithPostgres exists so that every defer runs before the caller's os.Exit.
// os.Exit in TestMain skips deferred calls, which would leak the container.
func runWithPostgres(m *testing.M) (int, error) {
	ctx := context.Background()

	if d := os.Getenv(DSNEnv); d != "" {
		schema := fmt.Sprintf("pgtest_%d", os.Getpid())
		dsn = withSearchPath(d, schema)
		// Create the schema on a plain connection: the pool below routes
		// everything into it via search_path.
		conn, err := pgx.Connect(ctx, d)
		if err != nil {
			return 0, fmt.Errorf("opening connection for %s: %w", DSNEnv, err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, schema)); err != nil {
			conn.Close(ctx)
			return 0, fmt.Errorf("creating schema %s: %w", schema, err)
		}
		conn.Close(ctx)

		pool, err = pgxpool.New(ctx, withSearchPath(d, schema))
		if err != nil {
			return 0, fmt.Errorf("opening pool for %s: %w", DSNEnv, err)
		}
		// Fail here rather than in every test: an unreachable DSN in the
		// environment is a setup mistake, not a test failure.
		if err := pool.Ping(ctx); err != nil {
			return 0, fmt.Errorf("pinging %s: %w", DSNEnv, err)
		}

		code := m.Run()

		// Drop our schema before closing the pool, so a shared dev database
		// does not accumulate an abandoned pgtest_<pid> per run.
		if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, schema)); err != nil {
			fmt.Fprintln(os.Stderr, "pgtest: dropping schema:", err)
		}
		pool.Close()
		return code, nil
	}

	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("conveyor"),
		tcpostgres.WithUsername("conveyor"),
		tcpostgres.WithPassword("conveyor"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		return 0, fmt.Errorf("starting postgres: %w", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			fmt.Fprintln(os.Stderr, "terminating container:", err)
		}
	}()

	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, fmt.Errorf("connection string: %w", err)
	}

	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("opening pool: %w", err)
	}
	defer pool.Close()

	return m.Run(), nil
}
