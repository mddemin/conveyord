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

const getJobSQL = `
      SELECT id, type, payload, state, attempt, created_at, updated_at
      FROM jobs
      WHERE id = $1`

const listJobSQL = `
      SELECT id, type, payload, state, attempt, created_at, updated_at
      FROM jobs
      WHERE ($1 = '' OR state = $1)
      ORDER BY created_at DESC, id DESC
      LIMIT $2`

// Get returns the job with the given id.
// It returns an error wrapping job.ErrNotFound if no such job exists.
func (s *Store) Get(ctx context.Context, id string) (*job.Job, error) {
	var j job.Job

	err := s.pool.QueryRow(ctx, getJobSQL, id).Scan(
		&j.ID, &j.Type, &j.Payload, &j.State, &j.Attempt, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", job.ErrNotFound, id)
		}
		return nil, err
	}

	return &j, nil
}

// Filter narrows a List query. The zero Filter matches every job.
type Filter struct {
	State job.State // empty matches any state
	Limit int       // non-positive means DefaultLimit
}

// DefaultLimit caps a List that did not ask for a limit.
const DefaultLimit = 100

// List returns jobs matching f, newest first.
func (s *Store) List(ctx context.Context, f Filter) ([]job.Job, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}

	rows, err := s.pool.Query(ctx, listJobSQL, f.State, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []job.Job
	for rows.Next() {
		var j job.Job
		err = rows.Scan(&j.ID, &j.Type, &j.Payload, &j.State, &j.Attempt, &j.CreatedAt, &j.UpdatedAt)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}
