package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mddemin/conveyor/internal/agent"
	"github.com/mddemin/conveyor/internal/job"
)

// This file is the catalogue of work this binary can do. Adding a job type is
// adding a function here and a line in defaultExecutors; anything more
// elaborate is a plugin system nobody asked for.
//
// Two rules an executor must follow, both of them load-bearing:
//
//   - Wrap a failure that will fail identically next time in job.Permanent.
//     That is the only way to say "do not retry" — everything else burns
//     MaxAttempts first and dies anyway, slower and noisier.
//   - Return when ctx does. It carries both the agent's timeout and a
//     CancelJob from the server; an executor that ignores it holds its lease
//     until the deadline the operator cannot see.
func defaultExecutors(log *slog.Logger) map[string]agent.Executor {
	return map[string]agent.Executor{
		"sleep":    sleepJob,
		"checksum": checksumJob(log),
	}
}

// sleepJob does nothing for as long as it is told. It exists so the queue can
// be demonstrated with a job long enough to watch: leases renewing, progress
// arriving, a signal interrupting it.
func sleepJob(ctx context.Context, payload []byte) error {
	var p struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return job.Permanent(fmt.Errorf(`payload is not {"duration":"2s"}: %w`, err))
	}
	d, err := time.ParseDuration(p.Duration)
	if err != nil {
		return job.Permanent(fmt.Errorf("duration %q: %w", p.Duration, err))
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// checksumJob hashes the payload and logs the digest. Result carries no body
// on the wire, so an executor's output is a side effect by construction —
// writing somewhere, or telling someone.
func checksumJob(log *slog.Logger) agent.Executor {
	return func(_ context.Context, payload []byte) error {
		if len(payload) == 0 {
			return job.Permanent(errors.New("nothing to checksum: payload is empty"))
		}
		sum := sha256.Sum256(payload)
		log.Info("checksum", "sha256", hex.EncodeToString(sum[:]), "bytes", len(payload))
		return nil
	}
}
