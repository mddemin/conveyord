package job

import (
	"math/rand/v2"
	"time"
)

// Clock exists to test Policy; it reports the current time
type Clock interface {
	Now() time.Time
}

// Policy is a rule that decides what happens to a failed job
type Policy struct {
	Base        time.Duration  // delay window after the first failure
	Max         time.Duration  // cap on the window, however many attempts
	MaxAttempts int            // attempts after which a job is buried
	Clock       Clock          // nil means the wall clock
	Jitter      func() float64 // returns [0,1); nil means rand.Float64
}

// Backoff returns backoff duration [0, window)
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		return time.Duration(float64(p.Base) * p.jitter())
	}

	window := float64(p.Base)
	for i := 1; i < attempt; i++ {
		window *= 2
		if window > float64(p.Max) {
			window = float64(p.Max)
			break
		}
	}

	return time.Duration(window * p.jitter())
}

func (p Policy) jitter() float64 {
	if p.Jitter == nil {
		return rand.Float64()
	}
	return p.Jitter()
}

// Decision shows OnFailure value the caller applies
type Decision struct {
	State         State     // Pending when the job will be retried, Dead when it will not
	NextAttemptAt time.Time // zero when State is Dead
}

// OnFailure returns a decision of what to do with failed job, caller should increase job's attempts.
// Attempt must already include the failure being reported, otherwise every job gets one extra retry.
func (p Policy) OnFailure(j *Job, cause error) Decision {
	if IsPermanent(cause) || j.Attempt >= p.MaxAttempts {
		return Decision{State: Dead}
	}
	return Decision{State: Pending, NextAttemptAt: p.now().Add(p.Backoff(j.Attempt))}
}

func (p Policy) now() time.Time {
	if p.Clock == nil {
		return time.Now()
	}
	return p.Clock.Now()
}
