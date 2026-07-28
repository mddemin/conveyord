package job

import "errors"

// permanentError is not exported because
// Permanent and IsPermanent are the only supported ways in and out
type permanentError struct {
	err error
}

func (e *permanentError) Error() string {
	return e.err.Error()
}

func (e *permanentError) Unwrap() error {
	return e.err
}

// Permanent returns wrapped error if it's present to show that it is permanent
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether error is permanent
func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}
