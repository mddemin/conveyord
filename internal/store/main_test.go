package store

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

// testPool is the connection pool every integration test in this package uses.
// It is nil under -short, where TestMain never starts a container.
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	// m.Run parses the test flags, so -short is not readable until we do it
	// ourselves. testing.Short() before this line panics.
	flag.Parse()

	if testing.Short() {
		os.Exit(m.Run())
	}

	code, err := runWithPostgres(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "store integration setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// runWithPostgres exists so that every defer runs before os.Exit.
// os.Exit in TestMain skips deferred calls, which would leak the container.
func runWithPostgres(m *testing.M) (int, error) {
	ctx := context.Background()

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

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, fmt.Errorf("connection string: %w", err)
	}

	testPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("opening pool: %w", err)
	}
	defer testPool.Close()

	return m.Run(), nil
}

func TestPoolIsAlive(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	if err := testPool.Ping(t.Context()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
