// Package job defines Conveyor's domain model: what a job is, how it moves
// through its lifecycle, and how failures are retried.
// It imports nothing outside the standard library and knows nothing about storage or transport
package job

import (
	"fmt"
	"slices"
)

// State is a job's position in its lifecycle.
type State string

// The states a job can occupy. A job is created Pending, is Leased to an
// agent, and ends Succeeded or Dead. A failed attempt is not a state: the
// retry policy sends the job straight back to Pending or on to Dead.
const (
	Pending   State = "pending"
	Leased    State = "leased"
	Succeeded State = "succeeded"

	Dead State = "dead"
)

// String returns the state's wire representation.
func (s State) String() string {
	return string(s)
}

// Terminal reports whether the state has no outgoing transitions.
func (s State) Terminal() bool {
	return len(transitions[s]) == 0
}

// Valid reports whether s is a state a job can occupy.
func (s State) Valid() bool {
	switch s {
	case Pending, Leased, Succeeded, Dead:
		return true
	}
	return false
}

// transitions is the only description of the lifecycle in Go, but not the
// only one that exists: set-based SQL in the store encodes some edges
// directly, because a statement that updates many rows at once cannot call
// Transition per row. Changing this table means grepping the SQL as well.
var transitions = map[State][]State{
	Pending: {Leased},
	Leased:  {Succeeded, Pending, Dead},
}

// Transition returns ErrInvalidTransition if transition to next state is not possible.
func Transition(from, to State) error {
	if !slices.Contains(transitions[from], to) {
		return fmt.Errorf("%w: from %s to %s", ErrInvalidTransition, from, to)
	}
	return nil
}
