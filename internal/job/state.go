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
// agent, and ends Succeeded or Dead. Failed is transient: the retry policy
// moves it back to Pending or on to Dead.
const (
	Pending   State = "pending"
	Leased    State = "leased"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Dead      State = "dead"
)

// String returns the state's wire representation.
func (s State) String() string {
	return string(s)
}

// Terminal reports whether the state has no outgoing transitions.
func (s State) Terminal() bool {
	return s == Succeeded || s == Dead
}

var transitions = map[State][]State{
	Pending: {Leased},
	Leased:  {Succeeded, Failed, Pending},
	Failed:  {Pending, Dead},
}

// Transition returns error if transition to next state is not possible.
func Transition(from, to State) error {
	if !slices.Contains(transitions[from], to) {
		return fmt.Errorf("%w: from %s to %s", ErrInvalidTransition, from, to)
	}
	return nil
}
