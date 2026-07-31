package job

import (
	"errors"
	"testing"
)

func TestStateString(t *testing.T) {
	if got := Pending.String(); got != "pending" {
		t.Errorf("Pending.String() = %q, want %q", got, "pending")
	}
}

func TestStateTerminal(t *testing.T) {
	tests := []struct {
		state State
		want  bool
	}{
		{Pending, false},
		{Leased, false},
		{Succeeded, true},
		{Dead, true},
	}

	for _, tt := range tests {
		t.Run(tt.state.String(), func(t *testing.T) {
			if got := tt.state.Terminal(); got != tt.want {
				t.Errorf("%s.Terminal() = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}

func TestTransitionAllowed(t *testing.T) {
	allowed := []struct{ from, to State }{
		{Pending, Leased},
		{Leased, Succeeded},
		{Leased, Pending},
	}

	for _, tt := range allowed {
		t.Run(tt.from.String()+"_to_"+tt.to.String(), func(t *testing.T) {
			if err := Transition(tt.from, tt.to); err != nil {
				t.Errorf("Transition(%s, %s) = %v, want nil", tt.from, tt.to, err)
			}
		})
	}
}

func TestTransitionRejectsEverythingElse(t *testing.T) {
	all := []State{Pending, Leased, Succeeded, Dead}
	legal := map[State]map[State]bool{
		Pending: {Leased: true},
		Leased:  {Succeeded: true, Pending: true, Dead: true},
	}

	for _, from := range all {
		for _, to := range all {
			if legal[from][to] {
				continue
			}
			t.Run(from.String()+"_to_"+to.String(), func(t *testing.T) {
				err := Transition(from, to)
				if !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("Transition(%s, %s) = %v, want ErrInvalidTransition", from, to, err)
				}
			})
		}
	}
}
