package job

import "errors"

// ErrInvalidTransition reports that a state change the lifecycle forbids
var ErrInvalidTransition = errors.New("invalid state transition")

// ErrNotFound reports that no job exists with the requested identifier
var ErrNotFound = errors.New("job not found")

// ErrDuplicate reports that a job with the same id already exists.
var ErrDuplicate = errors.New("duplicate")

// ErrEmptyCause reports that cause is empty
var ErrEmptyCause = errors.New("cause is nil")

var ErrUndelivered = errors.New("job was not delivered to any executor")
