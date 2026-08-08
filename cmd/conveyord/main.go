package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mddemin/conveyor/internal/httpapi"
	"github.com/mddemin/conveyor/internal/hub"
	"github.com/mddemin/conveyor/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		slog.Error("startup failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	addr := flag.String("addr", ":8080", "address to listen on")
	dsn := flag.String("dsn", os.Getenv("CONVEYOR_DSN"), "Postgres connection string")
	flag.Parse()

	if *dsn == "" {
		return errors.New("no database: set -dsn or CONVEYOR_DSN")
	}

	ctx := context.Background() // pool lifetime: the process

	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pool.Close()

	// Startup work gets its own deadline. A daemon that hangs forever waiting
	// for a database is worse than one that exits with a message.
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := pool.Ping(startupCtx); err != nil {
		return fmt.Errorf("pinging postgres: %w", err)
	}
	if err := store.Migrate(startupCtx, pool); err != nil {
		return fmt.Errorf("migrating: %w", err)
	}

	hub := hub.New()

	api := httpapi.New(store.New(pool, hub), hub, log)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
	}

	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", srv.Addr, err)
	}

	log.Info("listening", "addr", ln.Addr().String()) // bind already succeeded

	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serving: %w", err)
	}
	return nil
}
