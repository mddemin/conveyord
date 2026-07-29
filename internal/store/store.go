package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mddemin/conveyor/internal/job"
)

const uniqueViolation = "23505"

// Store persists jobs in Postgres. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool. The caller owns the pool and must close it.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Enqueue inserts j as a new pending job and fills in its
// State, Attempt, CreatedAt and UpdatedAt from the stored row.
func (s *Store) Enqueue(ctx context.Context, j *job.Job) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		payload := j.Payload
		if payload == nil {
			payload = []byte{}
		}
		err := tx.QueryRow(ctx,
			"INSERT INTO jobs (id, type, payload, state) VALUES ($1, $2, $3, $4) RETURNING created_at, updated_at",
			j.ID, j.Type, payload, job.Pending).Scan(&j.CreatedAt, &j.UpdatedAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				if pgErr.Code == uniqueViolation {
					return fmt.Errorf("job id %s: %w", j.ID, ErrDuplicate)
				}
			}
			return err
		}
		j.Attempt = 0
		j.State = job.Pending
		return nil
	})
	return err
}
