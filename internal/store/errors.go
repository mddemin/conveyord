package store

import "errors"

// ErrDuplicate reports that a job with the same id already exists.
var ErrDuplicate = errors.New("duplicate")
