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
	pool   *pgxpool.Pool
	events Publisher
}

// New returns a Store backed by pool, publishing every state change it
// commits to events. The caller owns the pool and must close it. A nil events
// discards them, so a wiring mistake costs the live stream rather than every
// write.
func New(pool *pgxpool.Pool, events Publisher) *Store {
	if events == nil {
		events = nopPublisher{}
	}
	return &Store{pool: pool, events: events}
}

// Publisher receives a job.Event for every state change this package commits.
// Declared here rather than imported so storage depends on the behaviour it
// needs and not on whatever provides it: internal/hub satisfies this today,
// and a Postgres LISTEN/NOTIFY publisher could satisfy it tomorrow without
// this package changing.
//
// Implementations must not block. Every call happens on the goroutine that
// performed the write, after its transaction has committed, and a slow
// Publish would slow down enqueueing and leasing.
type Publisher interface {
	Publish(ev job.Event)
}

// nopPublisher discards events. It is what a Store gets when the caller
// passes no publisher, so a wiring mistake means no stream rather than a
// panic on the first write.
type nopPublisher struct{}

func (nopPublisher) Publish(job.Event) {}

const insertJobSQL = `
      INSERT INTO jobs (id, type, payload, state)
      VALUES ($1, $2, $3, $4)
      RETURNING created_at, updated_at`

const insertJobEventSQL = `
      INSERT INTO job_events (job_id, state, message)
      VALUES ($1, $2, $3)
      RETURNING created_at`

// Enqueue inserts j as a new pending job and fills in its
// State, Attempt, CreatedAt and UpdatedAt from the stored row.
func (s *Store) Enqueue(ctx context.Context, j *job.Job) error {
	var ev job.Event
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		payload := j.Payload
		if payload == nil {
			payload = []byte{}
		}
		err := tx.QueryRow(ctx,
			insertJobSQL,
			j.ID, j.Type, payload, job.Pending).Scan(&j.CreatedAt, &j.UpdatedAt)
		if err != nil {
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
				if pgErr.Code == uniqueViolation {
					return fmt.Errorf("job id %s: %w", j.ID, job.ErrDuplicate)
				}
			}
			return fmt.Errorf("inserting job %s: %w", j.ID, err)
		}
		j.Attempt = 0
		j.State = job.Pending

		// The event is read back from the row rather than assembled here, so
		// its CreatedAt is the database's timestamp and matches the history
		// GET /jobs/{id}/events will later serve.
		ev = job.Event{JobID: j.ID, State: j.State}
		err = tx.QueryRow(ctx, insertJobEventSQL, j.ID, j.State, ev.Message).Scan(&ev.CreatedAt)
		if err != nil {
			return fmt.Errorf("recording enqueue event for job %s: %w", j.ID, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Published after the transaction, never inside it: a subscriber told
	// about a write that then rolls back cannot be untold.
	s.events.Publish(ev)
	return nil
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

const listJobEventsSQL = `
      SELECT job_id, state, message, created_at
      FROM job_events
      WHERE job_id = $1
      ORDER BY id ASC
      LIMIT $2`

// Events returns up to DefaultLimit events for a job, oldest first.
func (s *Store) Events(ctx context.Context, jobID string) ([]job.Event, error) {
	rows, err := s.pool.Query(ctx, listJobEventsSQL, jobID, DefaultLimit)
	if err != nil {
		return nil, fmt.Errorf("events for job %s: %w", jobID, err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByName[job.Event])
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
              WHERE state = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= now())
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
	if n < 0 {
		n = 0
	}
	rows, err := s.pool.Query(ctx, leaseJobSQL, n, leasedBy, d)
	if err != nil {
		return nil, fmt.Errorf("query jobs leased by %s: %w", leasedBy, err)
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[job.Job])
	if err != nil {
		return nil, fmt.Errorf("collect jobs leased by %s: %w", leasedBy, err)
	}
	// One event per job: a batch lease is n state changes, and subscribers are
	// watching individual jobs. Publishing n times in a row is safe because a
	// Publisher must not block; see the interface's contract.
	//
	// CreatedAt comes from the job's UpdatedAt rather than from the job_events
	// row. They are the same instant, not an approximation: now() is the
	// transaction timestamp, and one statement wrote both.
	for i := range jobs {
		s.events.Publish(job.Event{
			JobID:     jobs[i].ID,
			State:     jobs[i].State, // 'leased', set by the UPDATE
			Message:   "leased by " + leasedBy,
			CreatedAt: jobs[i].UpdatedAt,
		})
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
          RETURNING id, state, updated_at
      ), logged AS (
          INSERT INTO job_events (job_id, state, message)
          SELECT id, state, $3 FROM leased
      )
      SELECT * FROM leased`

// Complete moves a leased job to a terminal state and records an event.
// state must be terminal and reachable from job.Leased, which today means
// job.Succeeded. Burial goes through Fail instead, so the retry policy gets a
// say before a job reaches job.Dead.
func (s *Store) Complete(ctx context.Context, id, leasedBy string, state job.State, message string) error {
	err := job.Transition(job.Leased, state)
	if err != nil {
		return fmt.Errorf("transiting job %s: %w", id, err)
	}
	if !state.Terminal() {
		return fmt.Errorf("completing job %s: %s is not terminal", id, state)
	}
	if state != job.Succeeded {
		return fmt.Errorf("completing job %s: %s is not Succeeded", id, state)
	}
	ev := job.Event{Message: message}
	err = s.pool.QueryRow(ctx, completeJobSQL, id, leasedBy, message, state).
		Scan(&ev.JobID, &ev.State, &ev.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s", job.ErrNotFound, id)
	}
	if err != nil {
		return fmt.Errorf("completing job %s leased by %s: %w", id, leasedBy, err)
	}
	s.events.Publish(ev)
	return nil
}

// Nothing selects from reaped, which is not a mistake: Postgres runs a
// data-modifying CTE exactly once whether or not the primary query reads its
// output, so the UPDATE happens either way. The query returns the job_events
// rows instead, because those are what gets published.
//
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
      ), logged AS (
          INSERT INTO job_events (job_id, state, message)
          SELECT id, 'pending', 'reaped, was leased by ' || leased_by FROM expired
          RETURNING job_id, state, message, created_at
      )
      SELECT job_id, state, message, created_at FROM logged`

// ReapExpired returns jobs whose lease has expired to the pending state and
// records an event for each. It returns the number of jobs reaped, at most
// DefaultLimit per call — a backlog drains over several ticks rather than in
// one long transaction.
func (s *Store) ReapExpired(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, reapExpiredSQL, DefaultLimit)
	if err != nil {
		return 0, fmt.Errorf("reaping jobs: %w", err)
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByName[job.Event])
	if err != nil {
		return 0, fmt.Errorf("collecting reaped jobs: %w", err)
	}
	for _, ev := range events {
		s.events.Publish(ev)
	}
	return len(events), nil
}

const getFailedJobSQL = `
      SELECT id, type, payload, state, attempt, created_at, updated_at
      FROM jobs
      WHERE id = $1 AND leased_by = $2 AND state = 'leased'
      FOR UPDATE`

const updateFailedJobSQL = `
      UPDATE jobs
      SET state            = $1,
          attempt          = $2,
          next_attempt_at  = now() + $3,
          leased_by        = NULL,
          lease_expires_at = NULL,
          updated_at       = now()
      WHERE id = $4`

// Fail applies p to a failed attempt on a leased job: it increments the
// attempt count, then either schedules a retry or dead-letters the job.
// It returns the decision that was applied.
// cause must be non-nil; Fail returns ErrEmptyCause otherwise.
func (s *Store) Fail(ctx context.Context, id, leasedBy string, cause error, p job.Policy) (job.Decision, error) {
	var d job.Decision
	if cause == nil {
		return d, fmt.Errorf("job %s: %w", id, job.ErrEmptyCause)
	}
	var ev job.Event

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var j job.Job
		err := tx.QueryRow(ctx,
			getFailedJobSQL, id, leasedBy).Scan(&j.ID, &j.Type, &j.Payload, &j.State, &j.Attempt, &j.CreatedAt, &j.UpdatedAt)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("%w: %s", job.ErrNotFound, id)
			}
			return fmt.Errorf("getting failed job %s leased by %s: %w", id, leasedBy, err)
		}
		j.Attempt++
		d = p.OnFailure(&j, cause)

		backoff := &d.Backoff
		if *backoff == 0 {
			backoff = nil
		}

		_, err = tx.Exec(ctx, updateFailedJobSQL, d.State, j.Attempt, backoff, id)
		if err != nil {
			return fmt.Errorf("updating failed job %s leased by %s: %w", id, leasedBy, err)
		}

		message := cause.Error()
		ev = job.Event{JobID: j.ID, State: d.State, Message: message}

		err = tx.QueryRow(ctx, insertJobEventSQL, j.ID, d.State, message).Scan(&ev.CreatedAt)
		if err != nil {
			return fmt.Errorf("recording failed event for job %s: %w", j.ID, err)
		}

		return nil
	})
	if err != nil {
		return d, err
	}

	s.events.Publish(ev)
	return d, nil
}

// next_attempt_at is written from a caller-supplied delay, and the reason is
// measured rather than assumed. Leaving the column alone looked free — the job
// would only be re-leased on the next poll — but runOne signals slotFreed as
// it returns, so the loop wakes the instant a release lands and re-leases
// immediately. Live, with one job and no agent connected: 67 lease+release
// pairs in 8 seconds against a 300 ms poll. The delay is what turns that back
// into one attempt per interval.
//
// attempt is still untouched. That is the whole point of Release: the retry
// budget belongs to work that was actually attempted.
const releaseJobSQL = `
      WITH released AS (
          UPDATE jobs
          SET state = 'pending', leased_by = NULL, lease_expires_at = NULL,
              next_attempt_at = now() + $4, updated_at = now()
          WHERE id = $1 AND leased_by = $2 AND state = 'leased'
          RETURNING id, state, updated_at
      ), logged AS (
          INSERT INTO job_events (job_id, state, message) SELECT id, state, $3 FROM released
      )
      SELECT * FROM released`

// Release returns a leased job to pending without spending an attempt: the
// work was never delivered to an executor, so nothing was tried and there is
// nothing to back off from. attempt is left exactly as it was, which is the
// whole difference from Fail.
//
// message says why, because the store cannot know: "no agents available" asks
// the operator to deploy an agent, "agent is slow" to add one. It becomes the
// job's event, the only durable copy — the dispatcher logs releases at debug.
//
// retryAfter hides the job until then. Zero means "immediately", which is
// honest for a one-off release and ruinous for a queue with no agent: see the
// measurement above.
//
// Same fencing as every other write: a dispatcher that has lost the lease gets
// job.ErrNotFound rather than clearing the lease of whoever now holds it.
func (s *Store) Release(ctx context.Context, id, leasedBy, message string, retryAfter time.Duration) error {
	ev := job.Event{Message: message}
	err := s.pool.QueryRow(ctx, releaseJobSQL, id, leasedBy, message, retryAfter).
		Scan(&ev.JobID, &ev.State, &ev.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s", job.ErrNotFound, id)
	}
	if err != nil {
		return fmt.Errorf("releasing job %s leased by %s: %w", id, leasedBy, err)
	}
	s.events.Publish(ev)
	return nil
}
