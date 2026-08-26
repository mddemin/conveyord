package dispatch

import "errors"

// ErrLeaseLost is the cause carried by a job context cancelled because the
// dispatcher no longer holds the job's lease. The runner can read it with
// context.Cause to distinguish "shutting down" from "someone else owns this
// job now" — and phase 10's agent will need the same distinction.
var ErrLeaseLost = errors.New("lease lost")

// errPanicked marks failures caused by a runner panicking instead of
// returning an error. A panic is a bug, not business logic, so it stays
// retryable: IsPermanent on a wrapped panic is false by construction.
var errPanicked = errors.New("job panicked")
