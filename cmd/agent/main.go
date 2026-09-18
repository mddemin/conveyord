// Command agent connects to conveyord, runs the jobs it is assigned, and
// reports every one of them. The stream discipline lives in internal/agent and
// the job types in executors.go; this file is flags, the dial, and the signal.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mddemin/conveyor/internal/agent"
	"github.com/mddemin/conveyor/internal/obs"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func parseArgs(args []string, execs map[string]agent.Executor) (server, pprof string, cfg agent.Config, err error) {
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	types := fs.String("types", "", "comma-separated job types to accept (default: every registered executor)")
	fs.StringVar(&server, "server", "localhost:50051", "conveyord gRPC address")
	fs.StringVar(&pprof, "pprof", "", "pprof listen address; empty disables it")
	fs.StringVar(&cfg.Name, "name", "", "agent identity, must be unique: a second stream with this name displaces the first")
	fs.IntVar(&cfg.Concurrency, "concurrency", 4, "jobs this agent runs at once")
	fs.DurationVar(&cfg.Heartbeat, "heartbeat", 10*time.Second, "how often to prove the stream is alive")
	fs.DurationVar(&cfg.Timeout, "timeout", 5*time.Minute, "how long one job may run before it is failed permanently")
	if err := fs.Parse(args); err != nil {
		return server, pprof, cfg, err
	}

	if cfg.Name == "" {
		host, _ := os.Hostname()
		cfg.Name = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	// Announcing a type this process cannot run means being handed work that
	// can only come back as a permanent failure, so the default is the truth:
	// exactly what is in the executor map.
	if *types == "" {
		for t := range execs {
			cfg.Types = append(cfg.Types, t)
		}
	} else {
		cfg.Types = strings.Split(*types, ",")
	}

	switch {
	case cfg.Concurrency <= 0:
		return server, pprof, cfg, errors.New("-concurrency must be positive")
	case cfg.Heartbeat <= 0 || cfg.Timeout <= 0:
		return server, pprof, cfg, errors.New("durations must be positive")
	}
	return server, pprof, cfg, nil
}

func main() {
	log := slog.New(obs.Handler{Handler: slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})})
	slog.SetDefault(log)

	execs := defaultExecutors(log)

	server, pprof, cfg, err := parseArgs(os.Args[1:], execs)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		log.Error("exiting", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := obs.Setup(ctx, "agent")
	if err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
	defer shutdown(context.WithoutCancel(ctx))

	conn, err := grpc.NewClient(server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("exiting", "err", fmt.Errorf("dialling %s: %w", server, err))
		os.Exit(1)
	}
	defer conn.Close()

	log.Info("starting", "agent", cfg.Name, "server", server, "types", cfg.Types)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return obs.ServePprof(ctx, pprof) })
	g.Go(func() error {
		err := agent.New(cfg, execs, log).Run(ctx, conn)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}
