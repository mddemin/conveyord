package store

import "errors"

// ErrDuplicate reports that a job with the same id already exists.
var ErrDuplicate = errors.New("duplicate")

// ErrEmptyCause reports that cause is empty
var ErrEmptyCause = errors.New("cause is nil")
