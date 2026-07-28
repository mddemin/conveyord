package job

import "time"

// Job is a unit of work
type Job struct {
	ID        string    // unique identifier
	Type      string    // selects the executor that runs the job
	Payload   []byte    // the executor's opaque input
	State     State     // current lifecycle position
	Attempt   int       // completed attempts; zero before the first run
	CreatedAt time.Time // time when job was created, this package never sets it
	UpdatedAt time.Time // time when job was updated, this package never sets it
}

// To moves the job to the next state.
// If the transition is not permitted it returns an error wrapping
// ErrInvalidTransition and leaves the job unchanged.
func (j *Job) To(next State) error {
	if err := Transition(j.State, next); err != nil {
		return err
	}

	j.State = next
	return nil
}
