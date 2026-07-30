package store

import (
	"context"
	"errors"
	"fmt"
	"time"

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

const insertJobSQL = `
      INSERT INTO jobs (id, type, payload, state)
      VALUES ($1, $2, $3, $4)
      RETURNING created_at, updated_at`

const insertJobEventSQL = `
      INSERT INTO job_events (job_id, state)
      VALUES ($1, $2)`

// Enqueue inserts j as a new pending job and fills in its
// State, Attempt, CreatedAt and UpdatedAt from the stored row.
func (s *Store) Enqueue(ctx context.Context, j *job.Job) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		payload := j.Payload
		if payload == nil {
			payload = []byte{}
		}
		err := tx.QueryRow(ctx,
			insertJobSQL,
			j.ID, j.Type, payload, job.Pending).Scan(&j.CreatedAt, &j.UpdatedAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				if pgErr.Code == uniqueViolation {
					return fmt.Errorf("job id %s: %w", j.ID, ErrDuplicate)
				}
			}
			return fmt.Errorf("inserting job %s: %w", j.ID, err)
		}
		j.Attempt = 0
		j.State = job.Pending

		_, err = tx.Exec(ctx, insertJobEventSQL, j.ID, j.State)
		if err != nil {
			return fmt.Errorf("recording enqueue event for job %s: %w", j.ID, err)
		}

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
		return nil, fmt.Errorf("getting job %s: %w", id, err)
	}

	return &j, nil
}

// Filter narrows a List query. The zero Filter matches every job.
type Filter struct {
	State job.State // empty matches any state
	Limit int       // non-positive means DefaultLimit
}

// DefaultLimit caps a List that did not ask for a limit and caps for Events.
const DefaultLimit = 100

// List returns jobs matching f, newest first.
func (s *Store) List(ctx context.Context, f Filter) ([]job.Job, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}

	rows, err := s.pool.Query(ctx, listJobSQL, f.State, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs (state=%q limit=%d): %w", f.State, limit, err)
	}
	defer rows.Close()

	var jobs []job.Job
	for rows.Next() {
		var j job.Job
		err = rows.Scan(&j.ID, &j.Type, &j.Payload, &j.State, &j.Attempt, &j.CreatedAt, &j.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("scanning jobs (state=%q limit=%d): %w", f.State, limit, err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating jobs (state=%q limit=%d): %w", f.State, limit, err)
	}
	return jobs, nil
}

// Event is one recorded step in a job's lifecycle.
// Lives here while storage is the only consumer; revisit if transports need it.
type Event struct {
	JobID     string
	State     job.State
	Message   string
	CreatedAt time.Time
}

const listJobEventsSQL = `
      SELECT job_id, state, message, created_at
      FROM job_events
      WHERE job_id = $1
      ORDER BY id ASC
      LIMIT $2`

// Events returns up to DefaultLimit events for a job, oldest first.
func (s *Store) Events(ctx context.Context, jobID string) ([]Event, error) {
	rows, err := s.pool.Query(ctx, listJobEventsSQL, jobID, DefaultLimit)
	if err != nil {
		return nil, fmt.Errorf("events for job %s: %w", jobID, err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByName[Event])
	if err != nil {
		return nil, fmt.Errorf("events for job %s: %w", jobID, err)
	}
	return events, nil
}

// The pending -> leased transition is spelled out here as well as in
// job.transitions: a set-based UPDATE cannot call job.Transition per row
// without giving up SKIP LOCKED. Changing the state machine means changing
// this query too.
const leaseJobSQL = `
      WITH leased AS (
          UPDATE jobs
          SET state            = 'leased',
              lease_expires_at = now() + $3,
              leased_by        = $2,
              updated_at       = now()
          WHERE id IN (
              SELECT id FROM jobs
              WHERE state = 'pending'
              ORDER BY created_at, id
              LIMIT $1
              FOR UPDATE SKIP LOCKED
          )
          RETURNING id, type, payload, state, attempt, created_at, updated_at
      ), logged AS (
          INSERT INTO job_events (job_id, state, message)
          SELECT id, state, 'leased by ' || $2 FROM leased
      )
      SELECT * FROM leased`

// Lease claims up to n jobs that are ready to run, marks them leased by
// leasedBy for the given duration, and returns them.
// It returns an empty slice when no work is available.
//
// Unlike the single-row paths, Lease does not call job.Transition: the
// transition is enforced by the query's WHERE state = 'pending' clause,
// which the database checks per row while holding the lock. Validating in Go
// first would be a check on stale data, since any row can change between the
// check and the UPDATE.
func (s *Store) Lease(ctx context.Context, leasedBy string, n int, d time.Duration) ([]job.Job, error) {
	if n <= 0 {
		n = DefaultLimit
	}
	rows, err := s.pool.Query(ctx, leaseJobSQL, n, leasedBy, d)
	if err != nil {
		return nil, fmt.Errorf("query jobs leased by %s: %w", leasedBy, err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[job.Job])
	if err != nil {
		return nil, fmt.Errorf("collect jobs leased by %s: %w", leasedBy, err)
	}
	return jobs, nil
}

const renewJobSQL = `
      UPDATE jobs
      SET lease_expires_at = now() + $3,
          updated_at       = now()
      WHERE id = $1 AND leased_by = $2 AND state = 'leased'`

// Renew extends the lease on a job held by leasedBy.
// It returns an error wrapping job.ErrNotFound if the job is not leased by
// leasedBy — because the lease expired and someone else took it, or because it
// never held it.
func (s *Store) Renew(ctx context.Context, id, leasedBy string, d time.Duration) error {
	ct, err := s.pool.Exec(ctx, renewJobSQL, id, leasedBy, d)
	if err != nil {
		return fmt.Errorf("renew job %s leased by %s: %w", id, leasedBy, err)
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", job.ErrNotFound, id)
	}
	return nil
}

const completeJobSQL = `
      WITH leased AS (
          UPDATE jobs
          SET state            = $4,
              lease_expires_at = NULL,
              leased_by        = NULL,
              updated_at       = now()
          WHERE id = $1 AND leased_by = $2 AND state = 'leased'
          RETURNING id, state
      ), logged AS (
          INSERT INTO job_events (job_id, state, message)
          SELECT id, state, $3 FROM leased
      )
      SELECT * FROM leased`

// Complete moves a leased job to a terminal state and records an event.
// state must be terminal and reachable from job.Leased, which today means
// job.Succeeded. Burial goes through Fail, which reaches job.Dead by way of
// job.Failed so the retry policy gets a say.
func (s *Store) Complete(ctx context.Context, id, leasedBy string, state job.State, message string) error {
	err := job.Transition(job.Leased, state)
	if err != nil {
		return fmt.Errorf("transiting job %s: %w", id, err)
	}
	if !state.Terminal() {
		return fmt.Errorf("completing job %s: %s is not terminal", id, state)
	}
	ct, err := s.pool.Exec(ctx, completeJobSQL, id, leasedBy, message, state)
	if err != nil {
		return fmt.Errorf("completing job %s leased by %s: %w", id, leasedBy, err)
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", job.ErrNotFound, id)
	}
	return nil
}

// logged reads from expired, not reaped: by the time the UPDATE's RETURNING
// runs, leased_by is already NULL, and the dead agent's name survives only in
// the rows the first CTE locked.
const reapExpiredSQL = `
      WITH expired AS (
          SELECT id, leased_by FROM jobs
          WHERE state = 'leased' AND lease_expires_at < now()
          ORDER BY lease_expires_at
          LIMIT $1
          FOR UPDATE SKIP LOCKED
      ), reaped AS (
          UPDATE jobs
          SET state            = 'pending',
              lease_expires_at = NULL,
              leased_by        = NULL,
              updated_at       = now()
          FROM expired
          WHERE jobs.id = expired.id
          RETURNING jobs.id
      ), logged AS (
          INSERT INTO job_events (job_id, state, message)
          SELECT id, 'pending', 'reaped, was leased by ' || leased_by FROM expired
      )
      SELECT count(*) FROM reaped`

// ReapExpired returns jobs whose lease has expired to the pending state and
// records an event for each. It returns the number of jobs reaped, at most
// DefaultLimit per call — a backlog drains over several ticks rather than in
// one long transaction.
func (s *Store) ReapExpired(ctx context.Context) (int, error) {
	var reaped int
	err := s.pool.QueryRow(ctx, reapExpiredSQL, DefaultLimit).Scan(&reaped)
	if err != nil {
		return 0, fmt.Errorf("reaping jobs: %w", err)
	}
	return reaped, nil
}
