package main

import (
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"
)

// Announcing a type this process cannot run means being handed work whose only
// possible outcome is a permanent failure. The default capability set is
// therefore the executor map, not "anything".
func TestCapabilitiesDefaultToTheExecutorMap(t *testing.T) {
	execs := defaultExecutors(slog.New(slog.NewTextHandler(io.Discard, nil)))

	_, cfg, err := parseArgs([]string{"-name", "a-1"}, execs)
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	slices.Sort(cfg.Types)
	if !slices.Equal(cfg.Types, []string{"checksum", "sleep"}) {
		t.Errorf("announced %v, want every registered executor", cfg.Types)
	}

	// And -types narrows it, for an agent dedicated to one kind of work.
	_, cfg, err = parseArgs([]string{"-name", "a-1", "-types", "sleep"}, execs)
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if !slices.Equal(cfg.Types, []string{"sleep"}) {
		t.Errorf("announced %v, want only sleep", cfg.Types)
	}
}

// A zero concurrency or a zero timeout is a process that accepts jobs and
// never finishes one. Refuse at startup, where an operator sees it, rather
// than at the first assignment.
func TestNonsenseFlagsAreRejected(t *testing.T) {
	execs := defaultExecutors(slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, args := range [][]string{
		{"-concurrency", "0"},
		{"-timeout", "0"},
		{"-heartbeat", "-1s"},
	} {
		if _, _, err := parseArgs(args, execs); err == nil {
			t.Errorf("parseArgs%v was accepted", args)
		}
	}
}

// The defaults have to be usable unconfigured: an agent started with no flags
// at all still identifies itself and still bounds its work.
func TestDefaultsAreUsable(t *testing.T) {
	server, cfg, err := parseArgs(nil, defaultExecutors(slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err != nil {
		t.Fatalf("parseArgs: %v", err)
	}
	if server == "" || cfg.Name == "" {
		t.Errorf("server=%q name=%q, want both filled in", server, cfg.Name)
	}
	if cfg.Concurrency <= 0 || cfg.Timeout <= 0 || cfg.Heartbeat <= 0 {
		t.Errorf("concurrency=%d timeout=%s heartbeat=%s, want all positive",
			cfg.Concurrency, cfg.Timeout, cfg.Heartbeat)
	}
	if cfg.Timeout < time.Second {
		t.Errorf("default timeout is %s: a real job would be buried for being slow", cfg.Timeout)
	}
}
