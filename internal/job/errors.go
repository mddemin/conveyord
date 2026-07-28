package job

import "errors"

// ErrInvalidTransition reports that a state change the lifecycle forbids
var ErrInvalidTransition = errors.New("invalid state transition")

// ErrNotFound reports that no job exists with the requested identifier
var ErrNotFound = errors.New("job not found")
