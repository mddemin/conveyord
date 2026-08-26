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
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DSNEnv names an already-running Postgres to use instead of starting a
// container. Point it at a local docker compose and the whole suite skips
// container startup; leave it unset in CI and every package gets a clean,
// isolated database.
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

// runWithPostgres exists so that every defer runs before the caller's os.Exit.
// os.Exit in TestMain skips deferred calls, which would leak the container.
func runWithPostgres(m *testing.M) (int, error) {
	ctx := context.Background()

	if d := os.Getenv(DSNEnv); d != "" {
		dsn = d
		var err error
		pool, err = pgxpool.New(ctx, dsn)
		if err != nil {
			return 0, fmt.Errorf("opening pool for %s: %w", DSNEnv, err)
		}
		defer pool.Close()
		// Fail here rather than in every test: an unreachable DSN in the
		// environment is a setup mistake, not a test failure.
		if err := pool.Ping(ctx); err != nil {
			return 0, fmt.Errorf("pinging %s: %w", DSNEnv, err)
		}
		return m.Run(), nil
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
