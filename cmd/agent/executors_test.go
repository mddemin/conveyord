package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/mddemin/conveyor/internal/job"
)

// Every executor in the catalogue must be reachable by name: a type in the map
// that nothing announces is dead code, and a name announced with no executor
// behind it is a job that can only fail permanently.
func TestEveryCatalogueEntryHasAnExecutor(t *testing.T) {
	execs := defaultExecutors(slog.New(slog.NewTextHandler(io.Discard, nil)))

	names := make([]string, 0, len(execs))
	for name, exec := range execs {
		if exec == nil {
			t.Errorf("type %q is registered with a nil executor", name)
		}
		names = append(names, name)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"checksum", "sleep"}) {
		t.Errorf("catalogue is %v, want checksum and sleep", names)
	}
}

// A payload the executor cannot parse will not parse on the retry either, so
// it is permanent — this is the path that turns a bad enqueue into a dead job
// after one attempt instead of five.
func TestSleepRejectsAnUnparseablePayloadPermanently(t *testing.T) {
	for _, payload := range []string{`not json`, `{"duration":"banana"}`} {
		err := sleepJob(t.Context(), []byte(payload))
		if err == nil {
			t.Fatalf("payload %s was accepted", payload)
		}
		if !job.IsPermanent(err) {
			t.Errorf("payload %s failed retryably (%v); it fails identically every attempt", payload, err)
		}
	}
}

// The bound the agent owns: a sleep job stops when its context does, so a
// CancelJob or the agent's own timeout actually ends the work rather than
// leaving it to run unobserved.
func TestSleepStopsWhenItsContextDoes(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- sleepJob(ctx, []byte(`{"duration":"1h"}`)) }()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("sleep returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sleep ignored its context: the job would hold its lease for an hour")
	}
}

// An empty payload is a caller mistake that repeats, so checksum buries it
// rather than hashing nothing and reporting success.
func TestChecksumRejectsAnEmptyPayloadPermanently(t *testing.T) {
	exec := checksumJob(slog.New(slog.NewTextHandler(io.Discard, nil)))

	if err := exec(t.Context(), nil); !job.IsPermanent(err) {
		t.Errorf("empty payload returned %v, want a permanent failure", err)
	}
	if err := exec(t.Context(), []byte(`{"file":"x"}`)); err != nil {
		t.Errorf("checksum of a real payload failed: %v", err)
	}
}
