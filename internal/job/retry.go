package job

import (
	"math"
	"math/rand/v2"
	"time"
)

// Policy is a rule that decides what happens to a failed job
type Policy struct {
	Base        time.Duration  // delay window after the first failure
	Max         time.Duration  // cap on the window, however many attempts
	MaxAttempts int            // attempts after which a job is buried, 0 means no limit
	Jitter      func() float64 // returns [0,1); nil means rand.Float64
}

// Backoff returns backoff duration [0, window)
func (p Policy) Backoff(attempt int) time.Duration {
	limit := float64(math.MaxInt64)
	if p.Max > 0 {
		limit = float64(p.Max)
	}
	window := math.Min(math.Ldexp(float64(p.Base), max(attempt-1, 0)), limit)
	if delay := window * p.jitter(); delay < float64(math.MaxInt64) {
		return time.Duration(delay)
	}
	return math.MaxInt64
}

func (p Policy) jitter() float64 {
	if p.Jitter == nil {
		return rand.Float64()
	}
	return p.Jitter()
}

// Decision shows OnFailure value the caller applies
type Decision struct {
	State   State         // Pending when the job will be retried, Dead when it will not
	Backoff time.Duration // zero when State is Dead
}

// OnFailure returns a decision of what to do with failed job, caller should increase job's attempts.
// Attempt must already include the failure being reported, otherwise every job gets one extra retry.
func (p Policy) OnFailure(j *Job, cause error) Decision {
	if IsPermanent(cause) || (j.Attempt >= p.MaxAttempts && p.MaxAttempts > 0) {
		return Decision{State: Dead}
	}
	return Decision{State: Pending, Backoff: p.Backoff(j.Attempt)}
}
