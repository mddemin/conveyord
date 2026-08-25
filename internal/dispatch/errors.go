package dispatch

import "errors"

// errPanicked marks failures caused by a runner panicking rather than
// returning an error. Sentinel lives in this package next to ErrLeaseLost.
var errPanicked = errors.New("job panicked")
