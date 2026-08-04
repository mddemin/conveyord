package httpapi

import (
	"context"
	"log/slog"

	"github.com/mddemin/conveyor/internal/job"
	"github.com/mddemin/conveyor/internal/store"
)

// Store is the slice of storage this package needs. Declared here, not in
// internal/store, so the API depends on a behaviour rather than on a struct.
type Store interface {
	Enqueue(ctx context.Context, j *job.Job) error
	Get(ctx context.Context, id string) (*job.Job, error)
	List(ctx context.Context, f store.Filter) ([]job.Job, error)
}

// API serves the job API. It is an http.Handler; binding it to a port is
// the caller's job.
type API struct {
	store Store
	log   *slog.Logger
}

// New returns an API backed by store. A nil log defaults to slog.Default(),
// so a wiring mistake degrades to the standard logger instead of panicking
// on the first 500.
func New(store Store, log *slog.Logger) *API {
	if log == nil {
		log = slog.Default()
	}
	return &API{store: store, log: log}
}
